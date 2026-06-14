using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Kuestenlogik.Surgewave.Analyzers;

/// <summary>
/// SRWV010: <c>System.Console</c> writes inside a plugin class bypass the
/// broker's logging pipeline (levels, structured fields, OTel correlation,
/// sink routing). Inject <c>ILogger&lt;T&gt;</c> via the constructor and
/// log through that instead. Triggered for <c>WriteLine</c>, <c>Write</c>,
/// <c>Error.WriteLine</c>, <c>Error.Write</c>.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class LoggerOverConsoleAnalyzer : DiagnosticAnalyzer
{
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(SurgewaveDiagnostics.SRWV010_LoggerOverConsole);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterOperationAction(AnalyzeInvocation, OperationKind.Invocation);
    }

    private static void AnalyzeInvocation(OperationAnalysisContext context)
    {
        var invocation = (IInvocationOperation)context.Operation;
        var target = invocation.TargetMethod;
        if (target.ContainingType?.ToDisplayString() != "System.Console") return;
        if (target.Name != "WriteLine" && target.Name != "Write") return;

        // Only flag the call when it sits inside a class that implements a
        // plugin interface — otherwise this rule would shout at every utility
        // assembly that happens to be in the analyzer's reach.
        var enclosingType = GetEnclosingType(context.ContainingSymbol);
        if (enclosingType is null) return;
        if (!PluginShouldBeSealedAnalyzer.ImplementsPluginInterface(enclosingType)) return;

        var diag = Diagnostic.Create(
            SurgewaveDiagnostics.SRWV010_LoggerOverConsole,
            invocation.Syntax.GetLocation());
        context.ReportDiagnostic(diag);
    }

    private static INamedTypeSymbol? GetEnclosingType(ISymbol? symbol)
    {
        while (symbol is not null)
        {
            if (symbol is INamedTypeSymbol named) return named;
            symbol = symbol.ContainingSymbol;
        }
        return null;
    }
}
