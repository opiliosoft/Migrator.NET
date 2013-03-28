using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
//using System.Data.SQLite;
using System.Linq;

using Migrator.Framework;

using ForeignKeyConstraint = Migrator.Framework.ForeignKeyConstraint;

namespace Migrator.Providers.SQLite
{
	/// <summary>
	/// Summary description for SQLiteTransformationProvider.
	/// </summary>
    public class SQLiteTransformationProvider : TransformationProvider
	{
        public SQLiteTransformationProvider(Dialect dialect, string connectionString, string scope, string providerName)
			: base(dialect, connectionString, null, scope)
        {
           this.CreateConnection(providerName);
		}

        protected virtual void CreateConnection(string providerName)
        {
            if (string.IsNullOrEmpty(providerName)) providerName = "System.Data.SQLite";
            var fac = DbProviderFactories.GetFactory(providerName);
            _connection = fac.CreateConnection(); // new SQLiteConnection(_connectionString);
            _connection.ConnectionString = _connectionString;
            _connection.Open();
        }

		public override void AddForeignKey(string name, string primaryTable, string[] primaryColumns, string refTable,
		                                   string[] refColumns, ForeignKeyConstraintType constraint)
		{
			// NOOP Because SQLite doesn't support foreign keys
		}

		public override void RemoveForeignKey(string name, string table)
		{
			// NOOP Because SQLite doesn't support foreign keys
		}

		public override void RemoveColumn(string table, string column)
		{
			if (! (TableExists(table) && ColumnExists(table, column)))
				return;


		    var newColumns = GetColumns(table).Where(x => x.Name != column).ToArray();
            
            AddTable(table + "_temp", null, newColumns);
		    var colNamesSql = string.Join(", ", newColumns.Select(x => x.Name));
            ExecuteQuery(String.Format("INSERT INTO {0}_temp SELECT {1} FROM {0}", table, colNamesSql));
			RemoveTable(table);
			ExecuteQuery(String.Format("ALTER TABLE {0}_temp RENAME TO {0}", table));
		}

		public override void RenameColumn(string tableName, string oldColumnName, string newColumnName)
		{
			if (ColumnExists(tableName, newColumnName))
				throw new MigrationException(String.Format("Table '{0}' has column named '{1}' already", tableName, newColumnName));

			if (ColumnExists(tableName, oldColumnName))
			{
			    var columnDef = GetColumns(tableName).First(x => x.Name == oldColumnName);

			    //if (columnDef.IsPrimaryKey)
			    {
			        columnDef.Name = newColumnName;
			        this.changeColumnInternal(tableName, new[] { oldColumnName }, new[] { columnDef });
			    }
			    /*else
			    {
			        columnDef.Name = newColumnName;
			        AddColumn(tableName, columnDef);
			        ExecuteQuery(String.Format("UPDATE {0} SET {1}={2}", tableName, newColumnName, oldColumnName));
			        RemoveColumn(tableName, oldColumnName);
			    }*/
			}
		}

        public override void RemoveColumnDefaultValue(string table, string column)
        {
            var columnDef = GetColumns(table).First(x => x.Name == column);
            columnDef.DefaultValue = null;
            changeColumnInternal(table, new[] { column }, new[] { columnDef });
        }

	    public override void AddPrimaryKey(string name, string table, params string[] columns)
	    {
	        List<Column> newCol = new List<Column>();
	        foreach (var column in columns)
	        {
                var columnDef = GetColumns(table).First(x => x.Name == column);
	            columnDef.ColumnProperty |= ColumnProperty.PrimaryKey;
                newCol.Add(columnDef);
	        }
	        this.changeColumnInternal(table, columns, newCol.ToArray());
	    }

	    private void changeColumnInternal(string table, string[] old, Column[] columns)
	    {
	        var newColumns = GetColumns(table).Where(x => !old.Any(y => x.Name.ToLower() == y.ToLower())).ToList();
            var oldColumnNames = newColumns.Select(x => x.Name).ToList();
            newColumns.AddRange(columns);
            oldColumnNames.AddRange(old);

            AddTable(table + "_temp", null, newColumns.ToArray());
            var colNamesNewSql = string.Join(", ", newColumns.Select(x => x.Name));
            var colNamesSql = string.Join(", ", oldColumnNames);
            ExecuteQuery(String.Format("INSERT INTO {1}_temp ({0}) SELECT {2} FROM {1}", colNamesNewSql, table, colNamesSql));
            RemoveTable(table);
            ExecuteQuery(String.Format("ALTER TABLE {0}_temp RENAME TO {0}", table));
        }


		public override void ChangeColumn(string table, Column column)
		{
			if (! ColumnExists(table, column.Name))
			{
				Logger.Warn("Column {0}.{1} does not exist", table, column.Name);
				return;
			}

			string tempColumn = "temp_" + column.Name;
			RenameColumn(table, column.Name, tempColumn);
			AddColumn(table, column);
			ExecuteQuery(String.Format("UPDATE {0} SET {1}={2}", table, column.Name, tempColumn));
			RemoveColumn(table, tempColumn);
		}

	    public override int TruncateTable(string table)
	    {
            return ExecuteNonQuery(String.Format("DELETE FROM {0} ", table));
	    }

	    public override bool TableExists(string table)
		{
			using (IDataReader reader =
				ExecuteQuery(String.Format("SELECT name FROM sqlite_master WHERE type='table' and name='{0}'", table)))
			{
				return reader.Read();
			}
		}

	    public override List<string> GetDatabases()
	    {
	        throw new NotImplementedException();
	    }

	    public override bool ConstraintExists(string table, string name)
		{
			return false;
		}

		public override string[] GetTables()
		{
			var tables = new List<string>();

			using (IDataReader reader = ExecuteQuery("SELECT name FROM sqlite_master WHERE type='table' AND name <> 'sqlite_sequence' ORDER BY name"))
			{
				while (reader.Read())
				{
					tables.Add((string) reader[0]);
				}
			}

			return tables.ToArray();
		}

		public override Column[] GetColumns(string table)
		{
		    var columns = new List<Column>();
            using (IDataReader reader = ExecuteQuery(String.Format("PRAGMA table_info('{0}')", table)))
			{
				while (reader.Read())
				{
				    var column = new Column((string)reader[1]);

				    column.Type = _dialect.GetDbTypeFromString((string)reader[2]);
                    
                    if (Convert.ToBoolean(reader[3]))
                    {
                        column.ColumnProperty |= ColumnProperty.NotNull;
                    }
                    else
                    {
                        column.ColumnProperty |= ColumnProperty.Null;
                    }

				    column.DefaultValue = reader[4] == DBNull.Value ? null : reader[4];

                    if (Convert.ToBoolean(reader[5]))
                    {
                        column.ColumnProperty |= ColumnProperty.PrimaryKey;
                    }

                    columns.Add(column);
                   
				}
			}

           

			return columns.ToArray();
		}

		public bool IsNullable(string columnDef)
		{
			return ! columnDef.Contains("NOT NULL");
		}

		public bool ColumnMatch(string column, string columnDef)
		{
			return columnDef.StartsWith(column + " ") || columnDef.StartsWith(_dialect.Quote(column));
		}

        public override bool IndexExists(string table, string name)
		{
			using (IDataReader reader =
				ExecuteQuery(String.Format("SELECT name FROM sqlite_master WHERE type='index' and name='{0}'", name)))
			{
				return reader.Read();
			}
		}

        public override void AddTable(string name, string engine, params IDbField[] fields)
        {
            if (TableExists(name))
            {
                Logger.Warn("Table {0} already exists", name);
                return;
            }

            var columns = fields.Where(x => x is Column).Cast<Column>().ToArray();

            List<string> pks = GetPrimaryKeys(columns);
            bool compoundPrimaryKey = pks.Count > 1;

            var columnProviders = new List<ColumnPropertiesMapper>(columns.Length);
            foreach (Column column in columns)
            {
                // Remove the primary key notation if compound primary key because we'll add it back later
                if (compoundPrimaryKey && column.IsPrimaryKey)
                {
                    column.ColumnProperty = column.ColumnProperty ^ ColumnProperty.PrimaryKey;
                    column.ColumnProperty = column.ColumnProperty | ColumnProperty.NotNull; // PK is always not-null
                }

                ColumnPropertiesMapper mapper = _dialect.GetAndMapColumnProperties(column);
                columnProviders.Add(mapper);
            }

            string columnsAndIndexes = JoinColumnsAndIndexes(columnProviders);
            
            var table = _dialect.TableNameNeedsQuote ? _dialect.Quote(name) : name;
            string sqlCreate;

            sqlCreate = String.Format("CREATE TABLE {0} ({1}", table, columnsAndIndexes);
            
            if (compoundPrimaryKey)
            {
                sqlCreate += String.Format(", PRIMARY KEY ({0}) ", String.Join(",", pks.ToArray()));
            }

            var foreignKeys = fields.Where(x => x is ForeignKeyConstraint).Cast<ForeignKeyConstraint>().ToArray();
            foreach (var fk in foreignKeys)
            {
                sqlCreate += String.Format(", FOREIGN KEY ({0}) REFERENCES {1}({2})", String.Join(",", fk.Columns), fk.PkTable, String.Join(",", fk.PkColumns));
            }

            sqlCreate += ")";

            ExecuteNonQuery(sqlCreate);

            var indexes = fields.Where(x => x is Index).Cast<Index>().ToArray();
            foreach (var index in indexes)
            {
                AddIndex(name, index);
            }
        }
	}
}
