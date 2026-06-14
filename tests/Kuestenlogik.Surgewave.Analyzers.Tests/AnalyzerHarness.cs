using System.Collections.Immutable;
using System.Reflection;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Kuestenlogik.Surgewave.Analyzers.Tests;

/// <summary>
/// Lightweight harness: compiles a C# source snippet in-memory against the
/// real Surgewave.Plugins assembly + the BCL, runs the analyzer, returns
/// its diagnostics. Avoids the Microsoft.CodeAnalysis.Testing dep — these
/// tests are simple enough that direct CSharpCompilation is clearer.
/// </summary>
internal static class AnalyzerHarness
{
    private static readonly IEnumerable<MetadataReference> References = BuildReferences();

    public static ImmutableArray<Diagnostic> Run(DiagnosticAnalyzer analyzer, string source)
    {
        var tree = CSharpSyntaxTree.ParseText(source);
        var compilation = CSharpCompilation.Create(
            assemblyName: "AnalyzerHarness.Test",
            syntaxTrees: [tree],
            references: References,
            options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));

        var withAnalyzers = compilation.WithAnalyzers(ImmutableArray.Create(analyzer));
        return withAnalyzers.GetAnalyzerDiagnosticsAsync().GetAwaiter().GetResult();
    }

    private static IEnumerable<MetadataReference> BuildReferences()
    {
        var refs = new List<MetadataReference>();

        // Trusted-platform assemblies via the runtime — covers System.*, mscorlib, etc.
        var trustedPaths = (string?)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES");
        if (trustedPaths is not null)
        {
            foreach (var path in trustedPaths.Split(Path.PathSeparator))
            {
                if (File.Exists(path)) refs.Add(MetadataReference.CreateFromFile(path));
            }
        }

        // Surgewave plugin contracts (referenced via ProjectReference in the test csproj).
        refs.Add(MetadataReference.CreateFromFile(typeof(Surgewave.Plugins.IBrokerPlugin).Assembly.Location));

        // Microsoft.Extensions.Configuration.Abstractions is reachable via the
        // Plugins assembly's references — load it explicitly so analyzers that
        // resolve IConfiguration symbols work.
        foreach (var refAssemblyName in typeof(Surgewave.Plugins.IBrokerPlugin).Assembly.GetReferencedAssemblies())
        {
            try
            {
                var loaded = Assembly.Load(refAssemblyName);
                if (!string.IsNullOrEmpty(loaded.Location)) refs.Add(MetadataReference.CreateFromFile(loaded.Location));
            }
            catch { /* ignore */ }
        }

        return refs;
    }
}
