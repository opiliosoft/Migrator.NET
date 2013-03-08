using System;
using System.Collections.Generic;

using IBM.Data.Informix;

namespace Migrator.Providers.Impl.Informix
{
    /// <summary>
    /// DB2 transformation provider
    /// </summary>
    public class InformixTransformationProvider : TransformationProvider
    {
        public InformixTransformationProvider(Dialect dialect, string connectionString, string subSchemaName = "default")
            : base(dialect, connectionString, null, subSchemaName)
        {
            this._connection = new IfxConnection(this._connectionString);
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