using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Migrator.Framework
{
    public class ForeignKeyConstraint
    {
        public string Name { get; set; }
        public string Table { get; set; }
        public string Column { get; set; }
        public string PkTable { get; set; }
        public string PkColumn { get; set; }        
    }
}
