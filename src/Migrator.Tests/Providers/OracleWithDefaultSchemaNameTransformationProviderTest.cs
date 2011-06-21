using System;
using System.Configuration;
using Migrator.Providers.Oracle;
using NUnit.Framework;

namespace Migrator.Tests.Providers
{
	[TestFixture]
	[Category("Oracle")]
	public class OracleWithDefaultSchemaNameTransformationProviderTest : TransformationProviderConstraintBase
	{
		#region Setup/Teardown

		[SetUp]
		public void SetUp()
		{
			string constr = ConfigurationManager.AppSettings["OracleWithDefaultSchemaNameConnectionString"];
			if (constr == null)
				throw new ArgumentNullException("OracleConnectionString", "No config file");
			_provider = new OracleTransformationProvider(new OracleDialect(), constr, null);
			_provider.BeginTransaction();

			AddDefaultTable();
		}

		#endregion
	}
}