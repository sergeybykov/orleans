using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.LogConsistency;
using Orleans.MultiCluster;
using Orleans.SystemTargetInterfaces;
using Orleans.GrainDirectory;
using Orleans.Serialization;

namespace Orleans.Runtime.LogConsistency
{
    /// <summary>
    /// Functionality for use by log view adaptors that run distributed protocols. 
    /// This class allows access to these services to providers that cannot see runtime-internals.
    /// It also stores grain-specific information like the grain reference, and caches 
    /// </summary>
    internal class ProtocolServices : ILogConsistencyProtocolServices
    {

        private readonly ILogger log;
        private readonly Grain grain;   // links to the grain that owns this service object

        public ProtocolServices(
            Grain gr,
            ILoggerFactory loggerFactory,
            SerializationManager serializationManager)
        {
            this.grain = gr;
            this.log = loggerFactory.CreateLogger<ProtocolServices>();
            this.SerializationManager = serializationManager;
        }
        
        public GrainReference GrainReference => grain.GrainReference;

        /// <inheritdoc />
        public SerializationManager SerializationManager { get; }

        public void ProtocolError(string msg, bool throwexception)
        {

            log?.Error((int)(throwexception ? ErrorCode.LogConsistency_ProtocolFatalError : ErrorCode.LogConsistency_ProtocolError),
                string.Format("{0} Protocol Error: {1}",
                    grain.GrainReference,
                    msg));

            if (!throwexception)
                return;

            throw new OrleansException($"{msg} (grain={grain.GrainReference})");
        }

        public void CaughtException(string where, Exception e)
        {
            log?.Error((int)ErrorCode.LogConsistency_CaughtException,
               string.Format("{0} Exception Caught at {1}",
                   grain.GrainReference,
                   where),e);
        }

        public void CaughtUserCodeException(string callback, string where, Exception e)
        {
            log?.Warn((int)ErrorCode.LogConsistency_UserCodeException,
                string.Format("{0} Exception caught in user code for {1}, called from {2}",
                   grain.GrainReference,
                   callback,
                   where), e);
        }

        public void Log(LogLevel level, string format, params object[] args)
        {
            if (log != null && log.IsEnabled(level))
            {
                var msg = string.Format("{0} {1}",
                        grain.GrainReference,
                        string.Format(format, args));
                log.Log(level, 0, msg, null, (m, exc) => $"{m}");
            }
        }
    }

}
