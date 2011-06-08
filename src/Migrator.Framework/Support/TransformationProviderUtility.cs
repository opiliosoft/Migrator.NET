using log4net;

namespace Migrator.Framework.Support
{
	public static class TransformationProviderUtility
	{
		public const int MaxLengthForForeignKeyInOracle = 30;
		static readonly ILog log = LogManager.GetLogger(typeof (TransformationProviderUtility));

		public static string CreateForeignKeyName(string tableName, string foreignKeyTableName)
		{
			string fkName = string.Format("FK_{0}_{1}", tableName, foreignKeyTableName);

			return AdjustNameToSize(fkName, MaxLengthForForeignKeyInOracle);
		}

		public static string AdjustNameToSize(string name, int totalCharacters)
		{
			string adjustedName = name;

			if (adjustedName.Length > totalCharacters)
			{
				if (adjustedName.Contains("Test"))
				{
					adjustedName = adjustedName.Replace("Test", "");
				}
			}

			if (adjustedName.Length > totalCharacters) adjustedName = adjustedName.Substring(0, totalCharacters);

			if (name != adjustedName)
			{
				log.WarnFormat("Name has been truncated from: {0} to: {1}", name, adjustedName);
			}

			return adjustedName;
		}

		public static string FormatTableName(string schema, string tableName)
		{
			return string.IsNullOrEmpty(schema) ? tableName : string.Format("{0}.{1}", schema, tableName);
		}
	}
}