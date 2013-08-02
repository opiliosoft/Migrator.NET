using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

using Migrator.Framework;

namespace Migrator.Providers.Impl.Firebird
{
    /// <summary>
    /// Firebird transformation provider
    /// </summary>
    public class FirebirdTransformationProvider : TransformationProvider
    {
        public FirebirdTransformationProvider(Dialect dialect, string connectionString, string scope, string providerName)
            : base(dialect, connectionString, null, scope)
        {
            if (string.IsNullOrEmpty(providerName)) providerName = "FirebirdSql.Data.FirebirdClient";
            var fac = DbProviderFactories.GetFactory(providerName);
            _connection = fac.CreateConnection(); // new FbConnection(this._connectionString);
            _connection.ConnectionString = _connectionString;
            this._connection.Open();
        }

        public override List<string> GetDatabases()
        {
            throw new NotImplementedException();
        }

        public override bool ConstraintExists(string table, string name)
        {
            throw new NotImplementedException();
        }

        public override bool IndexExists(string table, string name)
        {
            throw new NotImplementedException();
        }
    }
}