using System;
using System.Collections.Generic;
using System.Xml;

namespace Orleans.Runtime.Configuration
{
    [Serializable]
    public class GrainServiceConfigurations
    {
        public IDictionary<string, IGrainServiceConfiguration> GrainServices { get; set; }

        public GrainServiceConfigurations()
        {
            GrainServices = new Dictionary<string, IGrainServiceConfiguration>();
        }

        internal static GrainServiceConfigurations Load(XmlElement child)
        {
            var container = new GrainServiceConfigurations();

            var nsManager = new XmlNamespaceManager(new NameTable());
            nsManager.AddNamespace("orleans", "urn:orleans");

            GrainServiceConfiguration.LoadProviderConfigurations(
                child, nsManager, container.GrainServices,
                c => container.GrainServices.Add(c.ServiceType, c));

            return container;
        }
    }

    internal static class GrainServiceConfigurationsUtility
    {
        internal static void RegisterGrainService(GrainServiceConfigurations grainServicesConfig, string serviceType)
        {
            if (grainServicesConfig.GrainServices.ContainsKey(serviceType))
                throw new InvalidOperationException(
                    string.Format("Grain service of type '{0}' has been already registered", serviceType));

            var config = new GrainServiceConfiguration(serviceType);

            grainServicesConfig.GrainServices.Add(serviceType, config);
        }
    }

    public interface IGrainServiceConfiguration
    {
        string ServiceType { get; set; }
    }

    [Serializable]
    public class GrainServiceConfiguration : IGrainServiceConfiguration
    {
        public string ServiceType { get; set; }

        public GrainServiceConfiguration() {}

        public GrainServiceConfiguration(string serviceType)
        {
            ServiceType = serviceType;
        }

        internal void Load(XmlElement child, IDictionary<string, IGrainServiceConfiguration> alreadyLoaded, XmlNamespaceManager nsManager)
        {
            if (nsManager == null)
            {
                nsManager = new XmlNamespaceManager(new NameTable());
                nsManager.AddNamespace("orleans", "urn:orleans");
            }
            
            if (alreadyLoaded != null && alreadyLoaded.ContainsKey(ServiceType))
            {
                return;
            }
            
            if (child.HasAttribute("ServiceType"))
            {
                ServiceType = child.GetAttribute("ServiceType");
            }
            else
            {
                throw new FormatException("Missing 'ServiceType' attribute on 'GrainService' element");
            }
        }
        
        internal static void LoadProviderConfigurations(XmlElement root, XmlNamespaceManager nsManager,
            IDictionary<string, IGrainServiceConfiguration> alreadyLoaded, Action<IGrainServiceConfiguration> add)
        {
            var nodes = root.SelectNodes("orleans:GrainService", nsManager);
            foreach (var node in nodes)
            {
                var subElement = node as XmlElement;
                if (subElement == null) continue;

                var config = new GrainServiceConfiguration();
                config.Load(subElement, alreadyLoaded, nsManager);
                add(config);
            }
        }
    }
}