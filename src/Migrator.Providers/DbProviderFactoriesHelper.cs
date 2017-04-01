using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Migrator.Providers
{
    public static class DbProviderFactoriesHelper
    {
        public static DbProviderFactory GetFactory(string providerName, string assemblyName, string factoryProviderType)
        {
            try
            {
                return DbProviderFactories.GetFactory(providerName);
            }
            catch(Exception)
            { }

#if NETSTANDARD
            return null;
#else
            return (DbProviderFactory)AppDomain.CurrentDomain.CreateInstanceAndUnwrap(assemblyName, factoryProviderType);
#endif
        }
    }

#if NETSTANDARD
    public abstract class DbProviderFactories
    {

        internal static readonly Dictionary<string, Func<DbProviderFactory>> _configs = new Dictionary<string, Func<DbProviderFactory>>();

        public static DbProviderFactory GetFactory(string providerInvariantName)
        {
            if (_configs.ContainsKey(providerInvariantName))
            {
                return _configs[providerInvariantName]();
            }

            throw new Exception("ConfigProviderNotFound");
        }

        public static void RegisterFactory(string providerInvariantName, Func<DbProviderFactory> factory)
        {
            _configs[providerInvariantName] = factory;
        }

        public static IEnumerable<string> GetFactoryProviderNames()
        {
            return _configs.Keys.ToArray();
        }
    }
#endif
}
