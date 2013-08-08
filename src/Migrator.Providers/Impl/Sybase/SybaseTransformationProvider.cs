using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Migrator.Providers.Impl.Sybase
{   
    public class SybaseTransformationProvider : TransformationProvider
    {
        public SybaseTransformationProvider(Dialect dialect, string connectionString, string scope, string providerName)
            : base(dialect, connectionString, null, scope)
        {
            if (string.IsNullOrEmpty(providerName)) providerName = "Sybase.Data.AseClient";
            var fac = DbProviderFactories.GetFactory(providerName);
            _connection = fac.CreateConnection(); 
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