using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

using Migrator.Framework;

namespace Migrator.Providers.Mysql
{
    /// <summary>
    /// MySql transformation provider
    /// </summary>    
    public class MariaDBTransformationProvider : MySqlTransformationProvider
    {
        public MariaDBTransformationProvider(Dialect dialect, string connectionString, string scope, string providerName) 
            : base(dialect, connectionString, scope, providerName)
        {
            if (string.IsNullOrEmpty(providerName)) providerName = "MySql.Data.MySqlClient";
            var fac = DbProviderFactories.GetFactory(providerName);
            _connection = fac.CreateConnection(); 
            _connection.ConnectionString = _connectionString;
            _connection.Open();
        }

        //public override void RenameColumn(string tableName, string oldColumnName, string newColumnName)
        //{
        //    if (ColumnExists(tableName, newColumnName))
        //    {
        //        throw new MigrationException(String.Format("Table '{0}' has column named '{1}' already", tableName, newColumnName));
        //    }

        //    if (!ColumnExists(tableName, oldColumnName))
        //    {
        //        throw new MigrationException(string.Format("The table '{0}' does not have a column named '{1}'", tableName, oldColumnName));
        //    }

        //    string definition = null;
        //    using (IDataReader reader = ExecuteQuery(String.Format("SHOW COLUMNS FROM {0} WHERE Field='{1}'", tableName, oldColumnName)))
        //    {
        //        if (reader.Read())
        //        {
        //            // TODO: Could use something similar to construct the columns in GetColumns
        //            definition = reader["Type"].ToString();
        //            if ("NO" == reader["Null"].ToString())
        //            {
        //                definition += " " + "NOT NULL";
        //            }

        //            if (!reader.IsDBNull(reader.GetOrdinal("Key")))
        //            {
        //                string key = reader["Key"].ToString();
        //                if ("PRI" == key)
        //                {
        //                    definition += " " + "PRIMARY KEY";
        //                }
        //                else if ("UNI" == key)
        //                {
        //                    definition += " " + "UNIQUE";
        //                }
        //            }

        //            if (!reader.IsDBNull(reader.GetOrdinal("Extra")))
        //            {
        //                definition += " " + reader["Extra"];
        //            }
        //        }
        //    }

        //    if (!String.IsNullOrEmpty(definition))
        //    {
        //        ExecuteNonQuery(String.Format("ALTER TABLE {0} CHANGE {1} {2} {3}", tableName, oldColumnName, newColumnName, definition));
        //    }
        //}
    }
}