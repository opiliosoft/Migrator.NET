using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

using Migrator.Framework;

namespace Migrator.Providers.SQLite
{
	/// <summary>
	/// Summary description for SQLiteTransformationProvider.
	/// </summary>
    public class SQLiteMonoTransformationProvider : SQLiteTransformationProvider
	{
        public SQLiteMonoTransformationProvider(Dialect dialect, string connectionString, string scope, string providerName)
            : base(dialect, connectionString, scope, providerName)
		{
          
		}		
	}
}
