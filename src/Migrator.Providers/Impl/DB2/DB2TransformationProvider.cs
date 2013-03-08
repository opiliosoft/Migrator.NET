using System;
using System.Collections.Generic;

using IBM.Data.DB2;

namespace Migrator.Providers.Impl.DB2
{
    /// <summary>
    /// DB2 transformation provider
    /// </summary>
    public class DB2TransformationProvider : TransformationProvider
    {
        public DB2TransformationProvider(Dialect dialect, string connectionString, string subSchemaName = "default")
            : base(dialect, connectionString, null, subSchemaName)
        {
            this._connection = new DB2Connection(this._connectionString);
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