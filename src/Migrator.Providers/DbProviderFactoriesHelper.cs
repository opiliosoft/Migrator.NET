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
#if NETSTANDARD
            return null;
#else
            try
            {
                return DbProviderFactories.GetFactory(providerName);
            }
            catch(Exception)
            { }

            return (DbProviderFactory)AppDomain.CurrentDomain.CreateInstanceAndUnwrap(assemblyName, factoryProviderType);
#endif
        }
    }
}
