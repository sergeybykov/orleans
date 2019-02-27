using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Networking.Shared;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;
using Orleans.Runtime.Messaging;
using Orleans.Serialization;

namespace Orleans.Messaging
{
    // <summary>
    // This class is used on the client only.
    // It provides the client counterpart to the Gateway and GatewayAcceptor classes on the silo side.
    // 
    // There is one ClientMessageCenter instance per OutsideRuntimeClient. There can be multiple ClientMessageCenter instances
    // in a single process, but because RuntimeClient keeps a static pointer to a single OutsideRuntimeClient instance, this is not
    // generally done in practice.
    // 
    // Each ClientMessageCenter keeps a collection of GatewayConnection instances. Each of these represents a bidirectional connection
    // to a single gateway endpoint. Requests are assigned to a specific connection based on the target grain ID, so that requests to
    // the same grain will go to the same gateway, in sending order. To do this efficiently and scalably, we bucket grains together
    // based on their hash code mod a reasonably large number (currently 8192).
    // 
    // When the first message is sent to a bucket, we assign a gateway to that bucket, selecting in round-robin fashion from the known
    // gateways. If this is the first message to be sent to the gateway, we will create a new connection for it and assign the bucket to
    // the new connection. Either way, all messages to grains in that bucket will be sent to the assigned connection as long as the
    // connection is live.
    // 
    // Connections stay live as long as possible. If a socket error or other communications error occurs, then the client will try to 
    // reconnect twice before giving up on the gateway. If the connection cannot be re-established, then the gateway is deemed (temporarily)
    // dead, and any buckets assigned to the connection are unassigned (so that the next message sent will cause a new gateway to be selected).
    // There is no assumption that this death is permanent; the system will try to reuse the gateway every 5 minutes.
    // 
    // The list of known gateways is managed by the GatewayManager class. See comments there for details.
    // </summary>
    internal class ClientMessageCenter : IMessageCenter, IDisposable
    {
        internal readonly SerializationManager SerializationManager;

        internal static readonly TimeSpan MINIMUM_INTERCONNECT_DELAY = TimeSpan.FromMilliseconds(100);   // wait one tenth of a second between connect attempts
        internal const int CONNECT_RETRY_COUNT = 2;                                                      // Retry twice before giving up on a gateway server

        internal GrainId ClientId { get; private set; }
        public IRuntimeClient RuntimeClient { get; }
        internal bool Running { get; private set; }

        private readonly GatewayManager gatewayManager;
        internal readonly Channel<Message> PendingInboundMessages;
        private readonly Action<Message>[] messageHandlers;
        private int numMessages;
        // The grainBuckets array is used to select the connection to use when sending an ordered message to a grain.
        // Requests are bucketed by GrainID, so that all requests to a grain get routed through the same bucket.
        // Each bucket holds a (possibly null) weak reference to a GatewayConnection object. That connection instance is used
        // if the WeakReference is non-null, is alive, and points to a live gateway connection. If any of these conditions is
        // false, then a new gateway is selected using the gateway manager, and a new connection established if necessary.
        private readonly WeakReference<Connection>[] grainBuckets;
        private readonly ILogger logger;
        public SiloAddress MyAddress { get; private set; }
        private readonly QueueTrackingStatistic queueTracking;
        private int numberOfConnectedGateways = 0;
        private readonly MessageFactory messageFactory;
        private readonly IClusterConnectionStatusListener connectionStatusListener;
        private readonly ConnectionManager connectionManager;
        private StatisticsLevel statisticsLevel;

        public ClientMessageCenter(
            IOptions<ClientMessagingOptions> clientMessagingOptions,
            IPAddress localAddress,
            int gen,
            GrainId clientId,
            SerializationManager serializationManager,
            IRuntimeClient runtimeClient,
            MessageFactory messageFactory,
            IClusterConnectionStatusListener connectionStatusListener,
            ILoggerFactory loggerFactory,
            IOptions<StatisticsOptions> statisticsOptions,
            ConnectionManager connectionManager,
            GatewayManager gatewayManager)
        {
            this.messageHandlers = new Action<Message>[Enum.GetValues(typeof(Message.Categories)).Length];
            this.connectionManager = connectionManager;
            this.SerializationManager = serializationManager;
            MyAddress = SiloAddress.New(new IPEndPoint(localAddress, 0), gen);
            ClientId = clientId;
            this.RuntimeClient = runtimeClient;
            this.messageFactory = messageFactory;
            this.connectionStatusListener = connectionStatusListener;
            Running = false;
            this.gatewayManager = gatewayManager;
            PendingInboundMessages = Channel.CreateUnbounded<Message>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false,
                AllowSynchronousContinuations = false
            });
            numMessages = 0;
            this.grainBuckets = new WeakReference<Connection>[clientMessagingOptions.Value.ClientSenderBuckets];
            logger = loggerFactory.CreateLogger<ClientMessageCenter>();
            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Proxy grain client constructed");
            IntValueStatistic.FindOrCreate(
                StatisticNames.CLIENT_CONNECTED_GATEWAY_COUNT,
                () =>
                {
                    return connectionManager.ConnectionCount;
                });
            statisticsLevel = statisticsOptions.Value.CollectionLevel;
            if (statisticsLevel.CollectQueueStats())
            {
                queueTracking = new QueueTrackingStatistic("ClientReceiver", statisticsOptions);
            }
        }

        public void Start()
        {
            Running = true;
            if (this.statisticsLevel.CollectQueueStats())
            {
                queueTracking.OnStartExecution();
            }
            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug("Proxy grain client started");
        }

        public void Stop()
        {
            Running = false;

            Utils.SafeExecute(() =>
            {
                PendingInboundMessages.Writer.TryComplete();
            });

            if (this.statisticsLevel.CollectQueueStats())
            {
                queueTracking.OnStopExecution();
            }
            gatewayManager.Stop();
        }

        public ChannelReader<Message> GetReader(Message.Categories type) => PendingInboundMessages.Reader;

        public void OnReceivedMessage(Message message)
        {
            var handler = this.messageHandlers[(int)message.Category];
            if (handler != null)
            {
                handler(message);
            }
            else
            {
                if (!PendingInboundMessages.Writer.TryWrite(message))
                {
                    this.logger.LogWarning($"{nameof(ClientMessageCenter)} dropping message {message} because inbound queue is closed.");
                }
            }
        }

        public void SendMessage(Message msg)
        {
            if (!Running)
            {
                this.logger.Error(ErrorCode.ProxyClient_MsgCtrNotRunning, $"Ignoring {msg} because the Client message center is not running");
                return;
            }

            void Send(ValueTask<Connection> connection, Message message)
            {
                try
                {
                    var gatewaySender = connection.Result;
                    gatewaySender.Send(message);
                    if (this.logger.IsEnabled(LogLevel.Trace)) this.logger.Trace(ErrorCode.ProxyClient_QueueRequest, "Sending message {0} via gateway {1}", message, gatewaySender);
                }
                catch (Exception exception)
                {
                    // This exception can be thrown if the gateway connection we selected was closed since we checked (i.e., we lost the race)
                    // If this happens, we reject if the message is targeted to a specific silo, or try again if not
                    
                    if (msg.TargetSilo != null)
                    {
                        RejectMessage(msg, string.Format("Target silo {0} is unavailable", msg.TargetSilo), exception);
                    }
                    else
                    {
                        _ = Task.Run(async () =>
                        {
                            await Task.Delay(TimeSpan.FromSeconds(2));
                            this.SendMessage(msg);
                        });
                    }
                }
            }

            var connectionTask = this.GetGatewayConnection(msg);
            if (connectionTask.IsCompletedSuccessfully)
            {
                Send(connectionTask, msg);
            }
            else
            {
                // The send function handles any failures.
                _ = connectionTask.AsTask().ContinueWith(t => { Send(connectionTask, msg); });
            }
        }

        private ValueTask<Connection> GetGatewayConnection(Message msg)
        {
            // If there's a specific gateway specified, use it
            if (msg.TargetSilo != null && gatewayManager.GetLiveGateways().Contains(msg.TargetSilo.ToGatewayUri()))
            {
                return this.connectionManager.GetConnection(msg.TargetSilo);
            }

            // For untargeted messages to system targets, and for unordered messages, pick a next connection in round robin fashion.
            if (msg.TargetGrain.IsSystemTarget || msg.IsUnordered)
            {
                // Get the cached list of live gateways.
                // Pick a next gateway name in a round robin fashion.
                // See if we have a live connection to it.
                // If Yes, use it.
                // If not, create a new GatewayConnection and start it.
                // If start fails, we will mark this connection as dead and remove it from the GetCachedLiveGatewayNames.
                int msgNumber = Interlocked.Increment(ref numMessages);
                var gatewayNames = gatewayManager.GetLiveGateways();
                int numGateways = gatewayNames.Count;
                if (numGateways == 0)
                {
                    RejectMessage(msg, "No gateways available");
                    logger.Warn(ErrorCode.ProxyClient_CannotSend, "Unable to send message {0}; gateway manager state is {1}", msg, gatewayManager);
                    return new ValueTask<Connection>(default(Connection));
                }

                var gatewayAddress = gatewayNames[msgNumber % numGateways].ToSiloAddress();
                return this.connectionManager.GetConnection(gatewayAddress);
            }

            // Otherwise, use the buckets to ensure ordering.
            var index = msg.TargetGrain.GetHashCode_Modulo((uint)grainBuckets.Length);
            WeakReference<Connection> weakRef;

            // Repeated from above, at the declaration of the grainBuckets array:
            // Requests are bucketed by GrainID, so that all requests to a grain get routed through the same bucket.
            // Each bucket holds a (possibly null) weak reference to a GatewayConnection object. That connection instance is used
            // if the WeakReference is non-null, is alive, and points to a live gateway connection. If any of these conditions is
            // false, then a new gateway is selected using the gateway manager, and a new connection established if necessary.
            weakRef = grainBuckets[index];

            if (weakRef != null && weakRef.TryGetTarget(out var existingConnection) && existingConnection.IsValid)
            {
                return new ValueTask<Connection>(existingConnection);
            }

            var addr = gatewayManager.GetLiveGateway();
            if (addr == null)
            {
                RejectMessage(msg, "No gateways available");
                logger.Warn(ErrorCode.ProxyClient_CannotSend_NoGateway, "Unable to send message {0}; gateway manager state is {1}", msg, gatewayManager);
                return new ValueTask<Connection>(default(Connection));
            }
            if (logger.IsEnabled(LogLevel.Trace)) logger.Trace(ErrorCode.ProxyClient_NewBucketIndex, "Starting new bucket index {0} for ordered messages to grain {1}", index, msg.TargetGrain);

            if (logger.IsEnabled(LogLevel.Debug)) logger.Debug(
                ErrorCode.ProxyClient_CreatedGatewayToGrain,
                "Creating gateway to {0} for message to grain {1}, bucket {2}, grain id hash code {3}X",
                addr,
                msg.TargetGrain,
                index,
                msg.TargetGrain.GetHashCode().ToString("x"));

            var gatewayConnection = this.connectionManager.GetConnection(addr.ToSiloAddress());
            if (gatewayConnection.IsCompletedSuccessfully)
            {
                var result = this.TryUpdateConnection(index, gatewayConnection.Result, weakRef);
                if (result == null) return this.GetGatewayConnection(msg);
                return new ValueTask<Connection>(result);
            }

            return AddToBucketAsync(index, msg, gatewayConnection, weakRef, addr);

            async ValueTask<Connection> AddToBucketAsync(
                uint i,
                Message message,
                ValueTask<Connection> c,
                WeakReference<Connection> existing,
                Uri gatewayAddress)
            {
                try
                {
                    var connection = await c;

                    var result = this.TryUpdateConnection(i, connection, existing);
                    if (result == null) return await this.GetGatewayConnection(message);
                    return result;
                }
                catch (Exception)
                {
                    this.gatewayManager.MarkAsDead(gatewayAddress);
                    return await this.GetGatewayConnection(message);
                }
            }
        }

        private Connection TryUpdateConnection(uint index, Connection connection, WeakReference<Connection> existing)
        {
            WeakReference<Connection> result;
            var replacement = new WeakReference<Connection>(connection);
            var newWeakRef = Interlocked.CompareExchange(ref grainBuckets[index], replacement, existing);
            if (newWeakRef == existing)
            {
                result = replacement;
            }
            else
            {
                result = newWeakRef;
            }

            if (result != null && result.TryGetTarget(out var existingConnection))
            {
                return existingConnection;
            }
            else
            {
                // If the reference has expired, retry with this non-expired reference.
                return this.TryUpdateConnection(index, connection, result);
            }
        }

        public Task<IGrainTypeResolver> GetGrainTypeResolver(IInternalGrainFactory grainFactory)
        {
            var silo = GetLiveGatewaySiloAddress();
            return GetTypeManager(silo, grainFactory).GetClusterGrainTypeResolver();
        }

        public Task<Streams.ImplicitStreamSubscriberTable> GetImplicitStreamSubscriberTable(IInternalGrainFactory grainFactory)
        {
            var silo = GetLiveGatewaySiloAddress();
            return GetTypeManager(silo, grainFactory).GetImplicitStreamSubscriberTable(silo);
        }

        public void RegisterLocalMessageHandler(Message.Categories category, Action<Message> handler)
        {
            this.messageHandlers[(int)category] = handler;
        }

        public void RejectMessage(Message msg, string reason, Exception exc = null)
        {
            if (!Running) return;
            
            if (msg.Direction != Message.Directions.Request)
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.Debug(ErrorCode.ProxyClient_DroppingMsg, "Dropping message: {0}. Reason = {1}", msg, reason);
            }
            else
            {
                if (logger.IsEnabled(LogLevel.Debug)) logger.Debug(ErrorCode.ProxyClient_RejectingMsg, "Rejecting message: {0}. Reason = {1}", msg, reason);
                MessagingStatisticsGroup.OnRejectedMessage(msg);
                var error = this.messageFactory.CreateRejectionResponse(msg, Message.RejectionTypes.Unrecoverable, reason, exc);
                OnReceivedMessage(error);
            }
        }

        /// <summary>
        /// For testing use only.
        /// </summary>
        public void Reconnect()
        {
            throw new NotImplementedException("Reconnect");
        }

        public int SendQueueLength
        {
            get { return 0; }
        }

        public int ReceiveQueueLength
        {
            get { return 0; }
        }

        private IClusterTypeManager GetTypeManager(SiloAddress destination, IInternalGrainFactory grainFactory)
        {
            return grainFactory.GetSystemTarget<IClusterTypeManager>(Constants.TypeManagerId, destination);
        }

        private SiloAddress GetLiveGatewaySiloAddress()
        {
            var gateway = gatewayManager.GetLiveGateway();

            if (gateway == null)
            {
                throw new OrleansException("Not connected to a gateway");
            }

            return gateway.ToSiloAddress();
        }

        internal void UpdateClientId(GrainId clientId)
        {
            if (ClientId.Category != UniqueKey.Category.Client)
                throw new InvalidOperationException("Only handshake client ID can be updated with a cluster ID.");

            if (clientId.Category != UniqueKey.Category.GeoClient)
                throw new ArgumentException("Handshake client ID can only be updated  with a geo client.", nameof(clientId));

            ClientId = clientId;
        }

        internal void OnGatewayConnectionOpen()
        {
            int newCount = Interlocked.Increment(ref numberOfConnectedGateways);
            this.connectionStatusListener.NotifyGatewayCountChanged(newCount, newCount - 1);
        }

        internal void OnGatewayConnectionClosed()
        {
            var gatewayCount = Interlocked.Decrement(ref numberOfConnectedGateways);
            if (gatewayCount == 0)
            {
                this.connectionStatusListener.NotifyClusterConnectionLost();
            }

            this.connectionStatusListener.NotifyGatewayCountChanged(gatewayCount, gatewayCount + 1);
        }

        public void Dispose()
        {
            PendingInboundMessages.Writer.TryComplete();
            gatewayManager.Dispose();
        }
    }
}
