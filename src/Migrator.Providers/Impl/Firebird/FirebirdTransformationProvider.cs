using System;
using System.Collections.Generic;
using System.Data;

using FirebirdSql.Data.FirebirdClient;

using Migrator.Framework;

namespace Migrator.Providers.Impl.Firebird
{
    /// <summary>
    /// Firebird transformation provider
    /// </summary>
    public class FirebirdTransformationProvider : TransformationProvider
    {
        public FirebirdTransformationProvider(Dialect dialect, string connectionString, string scope = "default")
            : base(dialect, connectionString, null, scope)
        {
            this._connection = new FbConnection(this._connectionString);
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