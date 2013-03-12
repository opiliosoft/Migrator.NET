using System;
using System.Data;
using Migrator.Framework;
using Migrator.Providers.Impl.Oracle;

namespace Migrator.Providers.Oracle
{
	public class MsOracleDialect : OracleDialect
	{		
        public override ITransformationProvider GetTransformationProvider(Dialect dialect, string connectionString, string defaultSchema, string scope, string providerName)
		{
			return new MsOracleTransformationProvider(dialect, connectionString, defaultSchema, scope, providerName);
		}				
	}
}