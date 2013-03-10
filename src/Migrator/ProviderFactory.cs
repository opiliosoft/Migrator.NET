#region License

//The contents of this file are subject to the Mozilla Public License
//Version 1.1 (the "License"); you may not use this file except in
//compliance with the License. You may obtain a copy of the License at
//http://www.mozilla.org/MPL/
//Software distributed under the License is distributed on an "AS IS"
//basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
//License for the specific language governing rights and limitations
//under the License.

#endregion

using System;
using System.Collections.Generic;
using System.Reflection;
using Migrator.Framework;
using Migrator.Providers;
using Migrator.Providers.Mysql;
using Migrator.Providers.Oracle;
using Migrator.Providers.PostgreSQL;
using Migrator.Providers.SQLite;
using Migrator.Providers.SqlServer;

namespace Migrator
{
	/// <summary>
	/// Handles loading Provider implementations
	/// </summary>
	public class ProviderFactory
	{
		static ProviderFactory()
		{ }       

        public static ITransformationProvider Create(ProviderTypes providerType, string connectionString, string defaultSchema, string scope = "default")
        {
            Dialect dialectInstance = DialectForProvider(providerType);

            return dialectInstance.NewProviderForDialect(connectionString, defaultSchema, scope);            
        }
      
        public static Dialect DialectForProvider(ProviderTypes providerType)
        {
            switch (providerType)
            {
                case ProviderTypes.SQLite:
                    return (Dialect)Activator.CreateInstance(typeof(SQLiteDialect));
                case ProviderTypes.MonoSQLite:
                    return (Dialect)Activator.CreateInstance(typeof(SQLiteMonoDialect));
                case ProviderTypes.Mysql:
                    return (Dialect)Activator.CreateInstance(typeof(MysqlDialect));
                case ProviderTypes.Oracle:
                    return (Dialect)Activator.CreateInstance(typeof(OracleDialect));
                case ProviderTypes.PostgreSQL:
                    return (Dialect)Activator.CreateInstance(typeof(PostgreSQLDialect));
                case ProviderTypes.PostgreSQL82:
                    return (Dialect)Activator.CreateInstance(typeof(PostgreSQL82Dialect));
                case ProviderTypes.SqlServer:
                    return (Dialect)Activator.CreateInstance(typeof(SqlServerDialect));
                case ProviderTypes.SqlServer2005:
                    return (Dialect)Activator.CreateInstance(typeof(SqlServer2005Dialect));
                case ProviderTypes.SqlServerCe:
                    return (Dialect)Activator.CreateInstance(typeof(SqlServerCeDialect));
            }

            return null;
        }              
	}
}