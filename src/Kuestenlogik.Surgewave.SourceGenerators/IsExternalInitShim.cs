// netstandard2.0 lacks System.Runtime.CompilerServices.IsExternalInit but C#
// records / init-only setters require it. Defining it here lets the source
// generator (which must target netstandard2.0) still use modern C# features.
namespace System.Runtime.CompilerServices
{
    using System.ComponentModel;

    [EditorBrowsable(EditorBrowsableState.Never)]
    internal static class IsExternalInit { }
}
