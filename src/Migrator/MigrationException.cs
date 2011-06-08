using System;

namespace Migrator
{
	/// <summary>
	/// Exception thrown for a generic migration issue
	/// </summary>
	[Serializable]
	public class MigrationException : Exception
	{
		public MigrationException() { }
		public MigrationException( string message ) : base( message ) { }
		public MigrationException( string message, Exception inner ) : base( message, inner ) { }
		protected MigrationException( 
			System.Runtime.Serialization.SerializationInfo info, 
			System.Runtime.Serialization.StreamingContext context ) : base( info, context ) { }
	}
}