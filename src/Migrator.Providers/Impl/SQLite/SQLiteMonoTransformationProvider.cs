using System;
using System.Collections.Generic;
using System.Data;
using Migrator.Framework;

using Mono.Data.Sqlite;

namespace Migrator.Providers.SQLite
{
	/// <summary>
	/// Summary description for SQLiteTransformationProvider.
	/// </summary>
    public class SQLiteMonoTransformationProvider : SQLiteTransformationProvider
	{
        public SQLiteMonoTransformationProvider(Dialect dialect, string connectionString, string scope = "default")
            : base(dialect, connectionString, scope)
		{
			_connection = new SqliteConnection(_connectionString);
			_connection.ConnectionString = _connectionString;
			_connection.Open();
		}		
	}
}
