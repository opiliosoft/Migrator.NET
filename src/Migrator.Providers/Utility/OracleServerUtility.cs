using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Oracle.DataAccess.Client;

namespace Migrator.Providers.Utility
{
	public static class OracleServerUtility
	{
		static readonly string[] _specialTableNames = new[]
		                                              	{
		                                              		"DEF$_AQCALL",
		                                              		"DEF$_AQERROR",
		                                              		"SQLPLUS_PRODUCT_PROFILE",
		                                              		"HELP",
		                                              		"MVIEW$_ADV_INDEX",
		                                              		"MVIEW$_ADV_PARTITION"
		                                              	};

		public static void RemoveAllTablesFromDefaultDatabase(string connectionString)
		{
			using (var connection = new OracleConnection(connectionString))
			{
				connection.Open();

				string[] allTablesToDrop = GetTablesToDrop(connection).ToArray();

				foreach (string table in allTablesToDrop)
				{
					string statement = string.Format("drop table \"{0}\" cascade constraints", table);

					ExecuteDropCommand(connection, statement);
				}
			}
		}

		static void ExecuteDropCommand(OracleConnection connection, string statement)
		{
			using (var dropCmd = new OracleCommand(statement, connection))
			{
				dropCmd.ExecuteNonQuery();
			}
		}

		public static int GetTableCount(string connectionString)
		{
			using (var connection = new OracleConnection(connectionString))
			{
				connection.Open();

				return GetTablesToDrop(connection).Count();
			}
		}

		public static IEnumerable<string> GetTablesToDrop(OracleConnection connection)
		{
			const string query = @"select * from user_tables where TABLESPACE_NAME = 'SYSTEM'";

			using (var getDropAllTablesCommand = new OracleCommand(query, connection))
			{
				getDropAllTablesCommand.CommandType = CommandType.Text;

				using (OracleDataReader reader = getDropAllTablesCommand.ExecuteReader())
				{
					while (reader.Read() && (reader[0] != null && !Convert.IsDBNull(reader[0])))
					{
						string tableName = reader[0].ToString();

						if (!_specialTableNames.Contains(tableName))
						{
							yield return tableName;
						}
					}
				}
			}
		}
	}
}