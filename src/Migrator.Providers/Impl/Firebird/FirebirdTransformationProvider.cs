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

        public override void AddColumn(string table, string sqlColumn)
        {
            table = QuoteTableNameIfRequired(table);
            ExecuteNonQuery(String.Format("ALTER TABLE {0} ADD {1}", table, sqlColumn));
        }

        /// <summary>
        /// Execute an SQL query returning results.
        /// </summary>
        /// <param name="sql">The SQL command.</param>
        /// <returns>A data iterator, <see cref="System.Data.IDataReader">IDataReader</see>.</returns>
        public override IDataReader ExecuteQuery(string sql)
        {
            Logger.Trace(sql);
            IDbCommand cmd = BuildCommand(sql);
            {
                try
                {
                    return cmd.ExecuteReader();
                }
                catch (Exception ex)
                {
                    Logger.Warn("query failed: {0}", cmd.CommandText);
                    throw new Exception("Failed to execute sql statement: " + sql, ex);
                }
            }
        }
        
        public override Column[] GetColumns(string table)
        {
            var columns = new List<Column>();
            using (
                IDataReader reader =
                    ExecuteQuery(
                        String.Format("select RDB$FIELD_NAME, RDB$NULL_FLAG from RDB$RELATION_FIELDS where RDB$RELATION_NAME = '{0}'", table.ToUpper())))
            {
                while (reader.Read())
                {
                    var column = new Column(reader.GetString(0).Trim(), DbType.String);
                    string nullableStr = reader.GetString(1);
                    bool isNullable = nullableStr == "1";
                    column.ColumnProperty |= isNullable ? ColumnProperty.Null : ColumnProperty.NotNull;

                    columns.Add(column);
                }
            }

            return columns.ToArray();
        }

        public override List<string> GetDatabases()
        {
            throw new NotImplementedException();
        }

        public override bool ConstraintExists(string table, string name)
        {
            //todo, implement this!!!

            //http://edn.embarcadero.com/article/25259 field infos in FB
            //http://www.felix-colibri.com/papers/db/interbase/using_interbase_system_tables/using_interbase_system_tables.html
      
            return false;
        }

        public override bool IndexExists(string table, string name)
        {
            return false;
        }        
    }
}