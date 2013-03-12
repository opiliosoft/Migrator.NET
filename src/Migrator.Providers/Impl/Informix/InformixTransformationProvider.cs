using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Migrator.Providers.Impl.Informix
{
    /// <summary>
    /// DB2 transformation provider
    /// </summary>
    public class InformixTransformationProvider : TransformationProvider
    {
        public InformixTransformationProvider(Dialect dialect, string connectionString, string scope, string providerName)
            : base(dialect, connectionString, null, scope)
        {
            if (string.IsNullOrEmpty(providerName)) providerName = "IBM.Data.Informix.Client";
            var fac = DbProviderFactories.GetFactory(providerName);
            _connection = fac.CreateConnection(); // new IfxConnection(this._connectionString);
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