using System;
using System.Collections.Generic;
using System.Data;
using Migrator.Framework;
using Migrator.Framework.SchemaBuilder;

using ForeignKeyConstraint = Migrator.Framework.ForeignKeyConstraint;

namespace Migrator.Providers
{
	/// <summary>
	/// No Op (Null Object Pattern) implementation of the ITransformationProvider
	/// </summary>
	public class NoOpTransformationProvider : ITransformationProvider
	{
		public static readonly NoOpTransformationProvider Instance = new NoOpTransformationProvider();

		NoOpTransformationProvider()
		{
		}

		public Dialect Dialect
		{
			get { return null; }
		}

        public string ConnectionString
        {
            get { return String.Empty; }
	    }

	    public virtual ILogger Logger
		{
			get { return null; }
			set { }
		}

		public string[] GetTables()
		{
			return null;
		}

	    public ForeignKeyConstraint[] GetForeignKeyConstraints(string table)
	    {
	        return null;
	    }

	    public int Insert(string table, string[] columns, object[] values)
		{
			return 0;
		}

	    public List<string> ExecuteStringQuery(string sql, params object[] args)
	    {
	        return new List<string>();
	    }

	    public Index[] GetIndexes(string table)
	    {
	        return null;
	    }

	    public Column[] GetColumns(string table)
		{
			return null;
		}

		public Column GetColumnByName(string table, string column)
		{
			return null;
		}

		public void RemoveForeignKey(string table, string name)
		{
			// No Op
		}

		public void RemoveConstraint(string table, string name)
		{
			// No Op
		}

	    public void RemoveAllConstraints(string table)
	    {
	        
	    }

	    public void AddView(string name, string tableName, params IViewField[] fields)
	    {
	        
	    }

	    public void AddTable(string name, params IDbField[] columns)
		{
			// No Op
		}

		public void AddTable(string name, string engine, params IDbField[] columns)
		{
			// No Op
		}

		public void RemoveTable(string name)
		{
			// No Op
		}

		public void RenameTable(string oldName, string newName)
		{
			// No Op
		}

		public void RenameColumn(string tableName, string oldColumnName, string newColumnName)
		{
			// No Op
		}

		public void RemoveColumn(string table, string column)
		{
			// No Op
		}

	    public void RemoveColumnDefaultValue(string table, string column)
	    {
	        // No Op
	    }

	    public bool ColumnExists(string table, string column)
		{
			return false;
		}

		public bool TableExists(string table)
		{
			return false;
		}

		public void AddColumn(string table, string column, DbType type, int size, ColumnProperty property, object defaultValue)
		{
			// No Op
		}

		public void AddColumn(string table, string column, DbType type)
		{
			// No Op
		}

		public void AddColumn(string table, string column, DbType type, object defaultValue)
		{
			// No Op
		}

		public void AddColumn(string table, string column, DbType type, int size)
		{
			// No Op
		}

		public void AddColumn(string table, string column, DbType type, ColumnProperty property)
		{
			// No Op
		}

		public void AddColumn(string table, string column, DbType type, int size, ColumnProperty property)
		{
			// No Op
		}

		public void AddPrimaryKey(string name, string table, params string[] columns)
		{
			// No Op
		}

		public void GenerateForeignKey(string primaryTable, string primaryColumn, string refTable, string refColumn)
		{
			// No Op
		}

		public void GenerateForeignKey(string primaryTable, string[] primaryColumns, string refTable, string[] refColumns)
		{
			// No Op
		}

		public void GenerateForeignKey(string primaryTable, string primaryColumn, string refTable, string refColumn, ForeignKeyConstraintType constraint)
		{
			// No Op
		}

		public void GenerateForeignKey(string primaryTable, string[] primaryColumns, string refTable,
		                               string[] refColumns, ForeignKeyConstraintType constraint)
		{
			// No Op
		}

		public void AddForeignKey(string name, string primaryTable, string primaryColumn, string refTable,
		                          string refColumn)
		{
			// No Op
		}

		public void AddForeignKey(string name, string primaryTable, string[] primaryColumns, string refTable, string[] refColumns)
		{
			// No Op
		}

		public void AddForeignKey(string name, string primaryTable, string primaryColumn, string refTable, string refColumn, ForeignKeyConstraintType constraint)
		{
			// No Op
		}

		public void AddForeignKey(string name, string primaryTable, string[] primaryColumns, string refTable,
		                          string[] refColumns, ForeignKeyConstraintType constraint)
		{
			// No Op
		}

		public void AddUniqueConstraint(string name, string table, params string[] columns)
		{
			// No Op
		}

		public void AddCheckConstraint(string name, string table, string checkSql)
		{
			// No Op
		}

		public bool ConstraintExists(string table, string name)
		{
			return false;
		}

		public void ChangeColumn(string table, Column column)
		{
			// No Op
		}

		public bool PrimaryKeyExists(string table, string name)
		{
			return false;
		}

		public int ExecuteNonQuery(string sql)
		{
			return 0;
		}
        public int ExecuteNonQuery(string sql, int timeout)
        {
            return 0;
        }
        public int ExecuteNonQuery(string sql, int timeout, object[] parameters)
        {
            return 0;
        }

        public IDataReader ExecuteQuery(string sql)
		{
			return null;
		}

		public object ExecuteScalar(string sql)
		{
			return null;
		}

		public IDataReader Select(string what, string from)
		{
			return null;
		}

		public IDataReader Select(string what, string from, string where)
		{
			return null;
		}

		public object SelectScalar(string what, string from)
		{
			return null;
		}

		public object SelectScalar(string what, string from, string where)
		{
			return null;
		}

		public int Update(string table, string[] columns, object[] values)
		{
			return 0;
		}

		public int Update(string table, string[] columns, object[] values, string where)
		{
			return 0;
		}

		public int Delete(string table, string[] columns, string[] columnValues)
		{
			return 0;
		}

		public int Delete(string table, string column, string value)
		{
			return 0;
		}

	    public int TruncateTable(string table)
	    {
	        return 0;
	    }

	    public void BeginTransaction()
		{
			// No Op
		}

		public void Rollback()
		{
			// No Op
		}

		public void Commit()
		{
			// No Op
		}

		public ITransformationProvider this[string provider]
		{
			get { return this; }
		}

	    public string SchemaInfoTable { get; set; }

	    public void MigrationApplied(long version, string scope)
		{
			//no op
		}

		public void MigrationUnApplied(long version, string scope)
		{
			//no op
		}

		public List<long> AppliedMigrations
		{
			get { return new List<long>(); }
		}

		public void AddColumn(string table, Column column)
		{
			// No Op
		}

		public void GenerateForeignKey(string primaryTable, string refTable)
		{
			// No Op
		}

		public void GenerateForeignKey(string primaryTable, string refTable, ForeignKeyConstraintType constraint)
		{
			// No Op
		}

		public IDbCommand GetCommand()
		{
			return null;
		}

		public void ExecuteSchemaBuilder(SchemaBuilder schemaBuilder)
		{
			// No Op
		}

		public string[] QuoteColumnNamesIfRequired(params string[] columnNames)
		{
			throw new NotImplementedException();
		}

		public string QuoteColumnNameIfRequired(string name)
		{
			throw new NotImplementedException();
		}

		public string QuoteTableNameIfRequired(string name)
		{
			throw new NotImplementedException();
		}

		public string Encode(Guid guid)
		{
			return guid.ToString();
		}

	    public void SwitchDatabase(string databaseName)
	    {
	        
	    }

	    public List<string> GetDatabases()
	    {
	        return new List<string>();
	    }

	    public bool DatabaseExists(string name)
	    {
	        return true;
	    }

	    public void CreateDatabases(string databaseName)
	    {
	        
	    }

	    public void DropDatabases(string databaseName)
	    {
	        
	    }

	    public void AddIndex(string table, Index index)
	    {
	        
	    }

	    public void Dispose()
		{
			//No Op
		}

		public void AddColumn(string table, string sqlColumn)
		{
			// No Op
		}

		public int Insert(string table, string[] columns, string[] columnValues)
		{
			return 0;
		}

		protected void CreateSchemaInfoTable()
		{
		}

        public void RemoveIndex(string table, string name)
		{
			// No Op
		}

        public void AddIndex(string name, string table, params string[] columns)
		{
			// No Op
		}

        public bool IndexExists(string table, string name)
		{
			return false;
		}
	}
}