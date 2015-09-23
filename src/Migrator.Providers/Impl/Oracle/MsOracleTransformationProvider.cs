using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using Migrator.Framework;

namespace Migrator.Providers.Oracle
{
	public class MsOracleTransformationProvider : OracleTransformationProvider
	{
		public MsOracleTransformationProvider(Dialect dialect, string connectionString, string defaultSchema, string scope, string providerName)
			: base(dialect, connectionString, defaultSchema, scope, providerName)
		{
           
		}

        public MsOracleTransformationProvider(Dialect dialect, IDbConnection connection, string defaultSchema, string scope, string providerName)
           : base(dialect, connection, defaultSchema, scope, providerName)
        {                            
        }

        protected override void CreateConnection(string providerName)
        {
            if (string.IsNullOrEmpty(providerName)) providerName = "System.Data.OracleClient";
            var fac = DbProviderFactories.GetFactory(providerName);
            _connection = fac.CreateConnection(); // new OracleConnection();
            _connection.ConnectionString = _connectionString;
            _connection.Open();
        }		
	}
}