using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Migrator.Framework
{
    public class ForeignKeyConstraint : IDbField
    {
        public ForeignKeyConstraint()
        { }

        public ForeignKeyConstraint(string name, string table, string[] columns, string pkTable, string[] pkColumns)
        {
            this.Name = name;
            this.Table = table;
            this.Columns = columns;
            this.PkTable = pkTable;
            this.PkColumns = pkColumns;
        }

        public string Name { get; set; }
        public string Table { get; set; }
        public string[] Columns { get; set; }
        public string PkTable { get; set; }
        public string[] PkColumns { get; set; }        
    }
}
