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
using Microsoft.Build.Framework;
using Microsoft.Build.Utilities;
using Migrator.Compile;
using Migrator.Framework.Loggers;
using Migrator.MSBuild.Logger;

namespace Migrator.MSBuild
{
	/// <summary>
	/// Runs migrations on a database
	/// </summary>
	/// <remarks>
	/// To script the changes applied to the database via the migrations into a file, set the <see cref="ScriptChanges"/> 
	/// flag and provide a file to write the changes to via the <see cref="ScriptFile"/> setting.
	/// </remarks>
	/// <example>
	/// <Target name="Migrate" DependsOnTargets="Build">
	///     <Migrate Provider="SqlServer"
	///         Connectionstring="Database=MyDB;Data Source=localhost;User Id=;Password=;"
	///         Migrations="bin/MyProject.dll"/>
	/// </Target>
	/// </example>
	/// <example>
	/// <Target name="Migrate" DependsOnTargets="Build">
	///     <CreateProperty Value="-1"  Condition="'$(SchemaVersion)'==''">
	///        <Output TaskParameter="Value" PropertyName="SchemaVersion"/>
	///     </CreateProperty>
	///     <Migrate Provider="SqlServer"
	///         Connectionstring="Database=MyDB;Data Source=localhost;User Id=;Password=;"
	///         Migrations="bin/MyProject.dll"
	///         To="$(SchemaVersion)"/>
	/// </Target>
	/// </example>
	public class Migrate : Task
	{
		string _scriptFile;
		long _to = -1; // To last revision

		[Required]
		public string Provider { set; get; }

		[Required]
		public string ConnectionString { set; get; }

		/// <summary>
		/// The paths to the assemblies that contain your migrations. 
		/// This will generally just be a single item.
		/// </summary>
		public ITaskItem[] Migrations { set; get; }

		/// <summary>
		/// The paths to the directory that contains your migrations. 
		/// This will generally just be a single item.
		/// </summary>
		public string Directory { set; get; }

		public string Language { set; get; }

		public long To
		{
			set { _to = value; }
			get { return _to; }
		}

		public bool Trace { set; get; }

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
		public string ScriptFile
		{
			get { return _scriptFile; }
			set { _scriptFile = value; }
		}

		public override bool Execute()
		{
			if (! String.IsNullOrEmpty(Directory))
			{
				var engine = new ScriptEngine(Language, null);
				Execute(engine.Compile(Directory));
			}

			if (null != Migrations)
			{
				foreach (ITaskItem assembly in Migrations)
				{
					Assembly asm = Assembly.LoadFrom(assembly.GetMetadata("FullPath"));
					Execute(asm);
				}
			}

			return true;
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