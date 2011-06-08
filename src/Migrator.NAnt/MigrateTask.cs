#region License

//The contents of this file are subject to the Mozilla Public License
//Version 1.1 (the "License"); you may not use this file except in
//compliance with the License. You may obtain a copy of the License at
//http://www.mozilla.org/MPL/
//Software distributed under the License is distributed on an "AS IS"
//basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
//License for the specific language governing rights and limitations
//under the License.

#endregion

using System;
using System.IO;
using System.Reflection;
using Migrator.Compile;
using Migrator.Framework.Loggers;
using Migrator.NAnt.Loggers;
using NAnt.Core;
using NAnt.Core.Attributes;

namespace Migrator.NAnt
{
	/// <summary>
	/// Runs migrations on a database
	/// </summary>
	/// <example>
	/// <loadtasks assembly="…/Migrator.NAnt.dll" />
	/// <target name="migrate" description="Migrate the database" depends="build">
	///  <property name="version" value="-1" overwrite="false" />
	///  <migrate
	///    provider="MySql"
	///    connectionstring="Database=MyDB;Data Source=localhost;User Id=;Password=;"
	///    migrations="bin/MyProject.dll"
	///    to="${version}" />
	/// </target>
	/// </example>
	[TaskName("migrate")]
	public class MigrateTask : Task
	{
		string _scriptFile;
		long _to = -1; // To last revision

		[TaskAttribute("provider", Required = true)]
		public string Provider { set; get; }

		[TaskAttribute("connectionstring", Required = true)]
		public string ConnectionString { set; get; }

		[TaskAttribute("migrations")]
		public FileInfo MigrationsAssembly { set; get; }

		/// <summary>
		/// The paths to the directory that contains your migrations. 
		/// This will generally just be a single item.
		/// </summary>
		[TaskAttribute("directory")]
		public string Directory { set; get; }

		[TaskAttribute("language")]
		public string Language { set; get; }

		[TaskAttribute("to")]
		public long To
		{
			set { _to = value; }
			get { return _to; }
		}

		[TaskAttribute("trace")]
		public bool Trace { set; get; }

		[TaskAttribute("dryrun")]
		public bool DryRun { set; get; }

		/// <summary>
		/// Gets value indicating whether to script the changes made to the database 
		/// to the file indicated by <see cref="ScriptFile"/>.
		/// </summary>
		/// <value><c>true</c> if the changes should be scripted to a file; otherwise, <c>false</c>.</value>
		public bool ScriptChanges
		{
			get { return !String.IsNullOrEmpty(_scriptFile); }
		}

		/// <summary>
		/// Gets or sets the script file that will contain the Sql statements 
		/// that are executed as part of the migrations.
		/// </summary>
		[TaskAttribute("scriptFile")]
		public string ScriptFile
		{
			get { return _scriptFile; }
			set { _scriptFile = value; }
		}

		protected override void ExecuteTask()
		{
			if (! String.IsNullOrEmpty(Directory))
			{
				var engine = new ScriptEngine(Language, null);
				Execute(engine.Compile(Directory));
			}

			if (null != MigrationsAssembly)
			{
				Assembly asm = Assembly.LoadFrom(MigrationsAssembly.FullName);
				Execute(asm);
			}
		}

		void Execute(Assembly asm)
		{
			var mig = new Migrator(Provider, ConnectionString, asm, Trace, new TaskLogger(this));
			mig.DryRun = DryRun;
			if (ScriptChanges)
			{
				using (var writer = new StreamWriter(ScriptFile))
				{
					mig.Logger = new SqlScriptFileLogger(mig.Logger, writer);
					RunMigration(mig);
				}
			}
			else
			{
				RunMigration(mig);
			}
		}

		void RunMigration(Migrator mig)
		{
			if (mig.DryRun)
				mig.Logger.Log("********** Dry run! Not actually applying changes. **********");

			if (_to == -1)
				mig.MigrateToLastVersion();
			else
				mig.MigrateTo(_to);
		}
	}
}