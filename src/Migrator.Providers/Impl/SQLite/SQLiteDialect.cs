using System;
using System.Data;
using Migrator.Framework;

namespace Migrator.Providers.SQLite
{
    public class SQLiteDialect : Dialect
    {
        public SQLiteDialect()
        {
            RegisterColumnType(DbType.Binary, "BINARY");
            RegisterColumnType(DbType.Byte, "TINYINT");
            RegisterColumnType(DbType.Int16, "SMALLINT");
            RegisterColumnType(DbType.Int32, "INTEGER");
            RegisterColumnType(DbType.Int64, "INTEGER");
            RegisterColumnType(DbType.SByte, "INTEGER");
            RegisterColumnType(DbType.UInt16, "INTEGER");
            RegisterColumnType(DbType.UInt32, "INTEGER");
            RegisterColumnType(DbType.UInt64, "INTEGER");

            RegisterColumnType(DbType.Currency, "CURRENCY");
            RegisterColumnType(DbType.Decimal, "DECIMAL");
            RegisterColumnType(DbType.Double, "DOUBLE");
            RegisterColumnType(DbType.Single, "REAL");
            RegisterColumnType(DbType.VarNumeric, "NUMERIC");

            RegisterColumnType(DbType.String, "TEXT");
            RegisterColumnType(DbType.StringFixedLength, "TEXT");
            RegisterColumnType(DbType.AnsiString, "TEXT");
            RegisterColumnType(DbType.AnsiStringFixedLength, "TEXT");

            RegisterColumnType(DbType.Date, "DATE");
            RegisterColumnType(DbType.DateTime, "DATETIME");
            RegisterColumnType(DbType.DateTimeOffset, "TEXT");
            RegisterColumnType(DbType.Time, "TIME");
            RegisterColumnType(DbType.Boolean, "BOOLEAN"); // Important for Dapper to know it should map to a bool
            RegisterColumnType(DbType.Guid, "UNIQUEIDENTIFIER");

            RegisterProperty(ColumnProperty.Identity, "AUTOINCREMENT");
            RegisterProperty(ColumnProperty.CaseSensitive, "COLLATE NOCASE");
        }

		public override bool ColumnNameNeedsQuote => true;

		public override bool TableNameNeedsQuote => true;

		public override string QuoteTemplate => "\"{0}\"";

        public override string Default(object defaultValue)
        {
            if (defaultValue is bool)
            {
                return String.Format("DEFAULT {0}", (bool)defaultValue ? "1" : "0");
            }            

            return base.Default(defaultValue);
        }

        public override bool NeedsNotNullForIdentity
        {
            get { return false; }
        }

        public override ITransformationProvider GetTransformationProvider(Dialect dialect, string connectionString, string defaultSchema, string scope, string providerName)
        {
            return new SQLiteTransformationProvider(dialect, connectionString, scope, providerName);
        }

        public override ITransformationProvider GetTransformationProvider(Dialect dialect, IDbConnection connection, string defaultSchema,
            string scope, string providerName)
        {
            return new SQLiteTransformationProvider(dialect, connection, scope, providerName);
        }
    }
}