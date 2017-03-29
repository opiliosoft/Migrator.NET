using System.Reflection;
using System.Runtime.InteropServices;

[assembly: AssemblyProduct("DotNetProjects.Migrator")]
[assembly: AssemblyCopyright("Copyright © 2017")]

[assembly: ComVisible(false)]

#if NETSTANDARD1_6
[assembly: AssemblyVersion("5.0.0.1")]
#else
[assembly: AssemblyVersion("5.0.0.*")]
#endif
