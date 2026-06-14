using Microsoft.CodeAnalysis;

namespace Kuestenlogik.Surgewave.Analyzers;

/// <summary>
/// Central registry of the SRWV-prefix diagnostic descriptors. Each
/// rule is registered here once, the per-rule analyser picks its
/// descriptor by id. Keeps the descriptor strings out of every
/// analyser class so a docs update is a single-file change.
///
/// SRWV-prefix is the Surgewave native-protocol magic-byte string —
/// chosen so the analyser ids are recognisable as Surgewave-owned even
/// without context.
/// </summary>
internal static class SurgewaveDiagnostics
{
    private const string Category = "Surgewave.Plugins";
    private const string HelpLinkBase = "https://surgewave.kuestenlogik.com/docs/analyzers/";

    public static readonly DiagnosticDescriptor SRWV001_PluginShouldBeSealed = new(
        id: "SRWV001",
        title: "Plugin class should be sealed",
        messageFormat: "Plugin class '{0}' should be sealed — plugin types are discovered by interface scan and never inherited from",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Surgewave's plugin loader instantiates the plugin type via parameterless ctor. Subclassing has no use-case and confuses the activator scan; mark the type sealed to make the intent explicit.",
        helpLinkUri: HelpLinkBase + "srwv001");

    public static readonly DiagnosticDescriptor SRWV002_ConfigureMustNotBlock = new(
        id: "SRWV002",
        title: "Configure must not block",
        messageFormat: "Configure should be non-blocking — move I/O into ConfigureAsync",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "(Stub) Reserved for follow-up: detect Wait()/Result/Thread.Sleep inside IBrokerPlugin.Configure or IProtocolPlugin.Configure.",
        helpLinkUri: HelpLinkBase + "srwv002");

    public static readonly DiagnosticDescriptor SRWV003_ManifestClassMustResolve = new(
        id: "SRWV003",
        title: "plugin.json assemblies entry must resolve to a discoverable IPlugin",
        messageFormat: "Reserved for follow-up — rule is registered but inert in this release",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: false,
        description: "(Stub) Reserved for follow-up: cross-check plugin.json 'assemblies' against discovered IPlugin implementations.",
        helpLinkUri: HelpLinkBase + "srwv003");

    public static readonly DiagnosticDescriptor SRWV004_ParameterlessCtorRequired = new(
        id: "SRWV004",
        title: "Plugin class must have a parameterless constructor",
        messageFormat: "Plugin class '{0}' must declare a parameterless constructor — the activator uses Activator.CreateInstance",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "BrokerPluginActivator instantiates plugin types via Activator.CreateInstance with no arguments. A type with only ctors that take parameters cannot be loaded.",
        helpLinkUri: HelpLinkBase + "srwv004");

    public static readonly DiagnosticDescriptor SRWV005_PluginIdUniqueness = new(
        id: "SRWV005",
        title: "Plugin id must be unique across loaded plugins",
        messageFormat: "Reserved for follow-up — rule is registered but inert in this release",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: false,
        description: "(Stub) Reserved for follow-up: cross-project id collision detection.",
        helpLinkUri: HelpLinkBase + "srwv005");

    public static readonly DiagnosticDescriptor SRWV006_XmlDocOnPublicApi = new(
        id: "SRWV006",
        title: "Plugin public API should carry XML documentation",
        messageFormat: "Reserved for follow-up — rule is registered but inert in this release",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Info,
        isEnabledByDefault: false,
        description: "(Stub) Reserved for follow-up: enforce <summary> on public IPlugin members.",
        helpLinkUri: HelpLinkBase + "srwv006");

    public static readonly DiagnosticDescriptor SRWV007_CancellationTokenOnAsyncLifecycle = new(
        id: "SRWV007",
        title: "Async lifecycle methods should accept CancellationToken",
        messageFormat: "Reserved for follow-up — rule is registered but inert in this release",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: false,
        description: "(Stub) Reserved for follow-up: warn when async IBrokerPlugin lifecycle members miss a CancellationToken param.",
        helpLinkUri: HelpLinkBase + "srwv007");

    public static readonly DiagnosticDescriptor SRWV008_NoSurgewaveInternalsImport = new(
        id: "SRWV008",
        title: "Do not import Surgewave internal namespaces",
        messageFormat: "Reserved for follow-up — rule is registered but inert in this release",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: false,
        description: "(Stub) Reserved for follow-up: ban Kuestenlogik.Surgewave.*.Internal namespaces in plugin authoring projects.",
        helpLinkUri: HelpLinkBase + "srwv008");

    public static readonly DiagnosticDescriptor SRWV009_ManifestVersionMatchesPackage = new(
        id: "SRWV009",
        title: "plugin.json version should match the project's package version",
        messageFormat: "Reserved for follow-up — rule is registered but inert in this release",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Info,
        isEnabledByDefault: false,
        description: "(Stub) Reserved for follow-up: AdditionalFiles-based comparison between plugin.json version and the csproj PackageVersion.",
        helpLinkUri: HelpLinkBase + "srwv009");

    public static readonly DiagnosticDescriptor SRWV010_LoggerOverConsole = new(
        id: "SRWV010",
        title: "Use ILogger instead of Console.WriteLine",
        messageFormat: "Console.Write/WriteLine in a Surgewave plugin bypasses the broker logging pipeline — inject ILogger<T> instead",
        category: Category,
        defaultSeverity: DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Surgewave's logging pipeline routes through Microsoft.Extensions.Logging. Console.WriteLine bypasses log levels, structured logging, JSON output and OTel correlation — replace with a ctor-injected ILogger<T>.",
        helpLinkUri: HelpLinkBase + "srwv010");

    /// <summary>Flat list — handed to <see cref="DiagnosticAnalyzer.SupportedDiagnostics"/>.</summary>
    public static readonly ImmutableArrayBuilder<DiagnosticDescriptor> All = new(new[]
    {
        SRWV001_PluginShouldBeSealed,
        SRWV002_ConfigureMustNotBlock,
        SRWV003_ManifestClassMustResolve,
        SRWV004_ParameterlessCtorRequired,
        SRWV005_PluginIdUniqueness,
        SRWV006_XmlDocOnPublicApi,
        SRWV007_CancellationTokenOnAsyncLifecycle,
        SRWV008_NoSurgewaveInternalsImport,
        SRWV009_ManifestVersionMatchesPackage,
        SRWV010_LoggerOverConsole,
    });
}

/// <summary>Tiny wrapper so the static field can hold an <c>ImmutableArray</c> without
/// constructing it in a static initialiser that would race against descriptor construction.</summary>
internal readonly struct ImmutableArrayBuilder<T>
{
    public readonly System.Collections.Immutable.ImmutableArray<T> Items;
    public ImmutableArrayBuilder(T[] items) => Items = System.Collections.Immutable.ImmutableArray.Create(items);
}
