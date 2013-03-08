using System;
using System.Data;
using Migrator.Framework;

namespace Migrator.Providers.SqlServer
{
	public class SqlServerDialect : Dialect
	{
		public const string DboSchemaName = "dbo";
		
	    public SqlServerDialect()
	    {
	        RegisterColumnType(DbType.AnsiStringFixedLength, "CHAR(255)");
            RegisterColumnType(DbType.AnsiStringFixedLength, 8000, "CHAR($l)");
            RegisterColumnType(DbType.AnsiString, "VARCHAR(255)");
            RegisterColumnType(DbType.AnsiString, 8000, "VARCHAR($l)");
            RegisterColumnType(DbType.AnsiString, 2147483647, "TEXT");
            RegisterColumnType(DbType.Binary, "VARBINARY(8000)");
            RegisterColumnType(DbType.Binary, 8000, "VARBINARY($l)");
            RegisterColumnType(DbType.Binary, 2147483647, "IMAGE");
            RegisterColumnType(DbType.Boolean, "BIT");
            RegisterColumnType(DbType.Byte, "TINYINT");
            RegisterColumnType(DbType.Currency, "MONEY");
            RegisterColumnType(DbType.Date, "DATETIME");
            RegisterColumnType(DbType.DateTime, "DATETIME");
            RegisterColumnType(DbType.Decimal, "DECIMAL(19,5)");
            RegisterColumnType(DbType.Decimal, 19, "DECIMAL(19, $l)");
            RegisterColumnType(DbType.Double, "DOUBLE PRECISION"); //synonym for FLOAT(53)
            RegisterColumnType(DbType.Guid, "UNIQUEIDENTIFIER");
            RegisterColumnType(DbType.Int16, "SMALLINT");
            RegisterColumnType(DbType.Int32, "INT");
            RegisterColumnType(DbType.Int64, "BIGINT");
            RegisterColumnType(DbType.Single, "REAL"); //synonym for FLOAT(24) 
            RegisterColumnType(DbType.StringFixedLength, "NCHAR(255)");
            RegisterColumnType(DbType.StringFixedLength, 4000, "NCHAR($l)");
            RegisterColumnType(DbType.String, "NVARCHAR(255)");
            RegisterColumnType(DbType.String, 4000, "NVARCHAR($l)");
            RegisterColumnType(DbType.String, 1073741823, "NTEXT");
            RegisterColumnType(DbType.Time, "DATETIME");
             
	        RegisterProperty(ColumnProperty.Identity, "IDENTITY");

            AddReservedWords("ADD", "EXCEPT", "PERCENT", "ALL", "EXEC", "PLAN", "ALTER", "EXECUTE", "PRECISION", "AND", "EXISTS", "PRIMARY", "ANY", "EXIT", "PRINT", "AS", "FETCH", "PROC", "ASC", "FILE", "PROCEDURE", "AUTHORIZATION", "FILLFACTOR", "PUBLIC", "BACKUP", "FOR", "RAISERROR", "BEGIN", "FOREIGN", "READ", "BETWEEN", "FREETEXT", "READTEXT", "BREAK", "FREETEXTTABLE", "RECONFIGURE", "BROWSE", "FROM", "REFERENCES", "BULK", "FULL", "REPLICATION", "BY", "FUNCTION", "RESTORE", "CASCADE", "GOTO", "RESTRICT", "CASE", "GRANT", "RETURN", "CHECK", "GROUP", "REVOKE", "CHECKPOINT", "HAVING", "RIGHT", "CLOSE", "HOLDLOCK", "ROLLBACK", "CLUSTERED", "IDENTITY", "ROWCOUNT", "COALESCE", "IDENTITY_INSERT", "ROWGUIDCOL", "COLLATE", "IDENTITYCOL", "RULE", "COLUMN", "IF", "SAVE", "COMMIT", "IN", "SCHEMA", "COMPUTE", "INDEX", "SELECT", "CONSTRAINT", "INNER", "SESSION_USER", "CONTAINS", "INSERT", "SET", "CONTAINSTABLE", "INTERSECT", "SETUSER", "CONTINUE", "INTO", "SHUTDOWN", "CONVERT", "IS", "SOME", "CREATE", "JOIN", "STATISTICS", "CROSS", "KEY", "SYSTEM_USER", "CURRENT", "KILL", "TABLE", "CURRENT_DATE", "LEFT", "TEXTSIZE", "CURRENT_TIME", "LIKE", "THEN", "CURRENT_TIMESTAMP", "LINENO", "TO", "CURRENT_USER", "LOAD", "TOP", "CURSOR", "NATIONAL", "TRAN", "DATABASE", "NOCHECK", "TRANSACTION", "DBCC", "NONCLUSTERED", "TRIGGER", "DEALLOCATE", "NOT", "TRUNCATE", "DECLARE", "NULL", "TSEQUAL", "DEFAULT", "NULLIF", "UNION", "DELETE", "OF", "UNIQUE", "DENY", "OFF", "UPDATE", "DESC", "OFFSETS", "UPDATETEXT", "DISK", "ON", "USE", "DISTINCT", "OPEN", "USER", "DISTRIBUTED", "OPENDATASOURCE", "VALUES", "DOUBLE", "OPENQUERY", "VARYING", "DROP", "OPENROWSET", "VIEW", "DUMMY", "OPENXML", "WAITFOR", "DUMP", "OPTION", "WHEN", "ELSE", "OR", "WHERE", "END", "ORDER", "WHILE", "ERRLVL", "OUTER", "WITH", "ESCAPE", "OVER", "WRITETEXT");
		}

        public override bool SupportsIndex
        {
            get { return false; }
        }

        public override bool ColumnNameNeedsQuote
        {
			get { return true; }
        }

		public override bool TableNameNeedsQuote
		{
			get { return true; }
		}

        public override string QuoteTemplate
        {
            get { return "[{0}]"; }
        }

		public override ITransformationProvider GetTransformationProvider(Dialect dialect, string connectionString, string defaultSchema)
		{
			return new SqlServerTransformationProvider(dialect, connectionString, defaultSchema ?? DboSchemaName);
		}

		public override string Quote(string value)
		{
			int firstDotIndex = value.IndexOf('.');
			if (firstDotIndex >= 0)
			{
				string owner = value.Substring(0, firstDotIndex);
				string table = value.Substring(firstDotIndex + 1);
				return (string.Format(QuoteTemplate, owner) + "." + string.Format(QuoteTemplate, table));
			}
			return string.Format(QuoteTemplate, value);
		}

        public override string Default(object defaultValue)
        {
            if (defaultValue.GetType().Equals(typeof (bool)))
            {
                defaultValue = ((bool) defaultValue) ? 1 : 0;
            }
            else if (defaultValue.GetType().Equals(typeof(Guid)))
            {
                defaultValue = "'" + ((Guid) defaultValue).ToString("D") + "'";
            }
            else if (defaultValue.GetType().Equals(typeof(string)) && !string.IsNullOrEmpty((string)defaultValue))
            {
                defaultValue = "'" + defaultValue + "'";
            }
            return String.Format("DEFAULT {0}", defaultValue);
        }
    }
}
