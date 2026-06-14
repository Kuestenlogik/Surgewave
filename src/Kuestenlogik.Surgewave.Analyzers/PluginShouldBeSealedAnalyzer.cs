using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;

namespace Kuestenlogik.Surgewave.Analyzers;

/// <summary>
/// SRWV001: types implementing an <c>IPlugin</c> interface (IBrokerPlugin,
/// IProtocolPlugin, IStorageEnginePlugin) should be marked <c>sealed</c>.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class PluginShouldBeSealedAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(SurgewaveDiagnostics.SRWV001_PluginShouldBeSealed);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSymbolAction(AnalyzeNamedType, SymbolKind.NamedType);
    }

    private static void AnalyzeNamedType(SymbolAnalysisContext context)
    {
        var type = (INamedTypeSymbol)context.Symbol;
        if (type.TypeKind != TypeKind.Class || type.IsAbstract || type.IsStatic || type.IsSealed)
            return;

        if (!ImplementsPluginInterface(type))
            return;

        var diag = Diagnostic.Create(
            SurgewaveDiagnostics.SRWV001_PluginShouldBeSealed,
            type.Locations[0],
            type.Name);
        context.ReportDiagnostic(diag);
    }

    /// <summary>
    /// True when the type implements at least one of the three plugin
    /// contracts. Matched by name to avoid taking a project reference to
    /// Surgewave.Plugins from the analyzer assembly.
    /// </summary>
    internal static bool ImplementsPluginInterface(INamedTypeSymbol type)
    {
        foreach (var iface in type.AllInterfaces)
        {
            var fullName = iface.ToDisplayString();
            if (fullName == "Kuestenlogik.Surgewave.Plugins.IBrokerPlugin"
                || fullName == "Kuestenlogik.Surgewave.Plugins.IProtocolPlugin"
                || fullName == "Kuestenlogik.Surgewave.Plugins.IStorageEnginePlugin"
                || fullName == "Kuestenlogik.Surgewave.Plugins.IControlPlugin")
            {
                return true;
            }
        }
        return false;
    }
}
