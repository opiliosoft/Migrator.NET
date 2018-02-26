using System.Linq.Expressions;

namespace Migrator.Framework
{
	public class ViewColumn : IViewAttributes
	{
		public string Prefix { get; }
		public string ColumnName { get; }

		public ViewColumn(string prefix, string columnName)
		{
			Prefix = prefix;
			ColumnName = columnName;
		}
	}
}
