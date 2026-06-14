using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Kuestenlogik.Surgewave.Analyzers;

/// <summary>
/// SRWV004: a plugin class must expose a parameterless constructor
/// because <c>BrokerPluginActivator</c> uses <c>Activator.CreateInstance</c>
/// with no arguments. Defaulted ctors count; the rule only fires when
/// every declared ctor requires arguments.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class ParameterlessCtorRequiredAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(SurgewaveDiagnostics.SRWV004_ParameterlessCtorRequired);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSymbolAction(AnalyzeNamedType, SymbolKind.NamedType);
    }

    private static void AnalyzeNamedType(SymbolAnalysisContext context)
    {
        var type = (INamedTypeSymbol)context.Symbol;
        if (type.TypeKind != TypeKind.Class || type.IsAbstract || type.IsStatic)
            return;

        if (!PluginShouldBeSealedAnalyzer.ImplementsPluginInterface(type))
            return;

        // Default behaviour: when no ctors are declared, the compiler synthesises
        // a public parameterless one — that path is fine, nothing to report.
        var declaredCtors = type.InstanceConstructors
            .Where(c => !c.IsImplicitlyDeclared)
            .ToList();
        if (declaredCtors.Count == 0) return;

        var hasParameterless = declaredCtors.Any(c => c.Parameters.Length == 0);
        if (hasParameterless) return;

        var diag = Diagnostic.Create(
            SurgewaveDiagnostics.SRWV004_ParameterlessCtorRequired,
            type.Locations[0],
            type.Name);
        context.ReportDiagnostic(diag);
    }
}
