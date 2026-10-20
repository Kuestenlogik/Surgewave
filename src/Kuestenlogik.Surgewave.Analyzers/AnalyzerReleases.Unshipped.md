; Unshipped analyzer release
; https://github.com/dotnet/roslyn-analyzers/blob/main/src/Microsoft.CodeAnalysis.Analyzers/ReleaseTrackingAnalyzers.Help.md

### New Rules

Rule ID | Category | Severity | Notes
--------|----------|----------|-------
SRWV001 | Surgewave.Plugins | Warning | Plugin class should be sealed
SRWV002 | Surgewave.Plugins | Warning | Configure must not block
SRWV003 | Surgewave.Plugins | Disabled | plugin.json assemblies entry must resolve to a discoverable IPlugin
SRWV004 | Surgewave.Plugins | Error | Plugin class must have a parameterless constructor
SRWV005 | Surgewave.Plugins | Disabled | Plugin id must be unique across loaded plugins
SRWV006 | Surgewave.Plugins | Disabled | Plugin public API should carry XML documentation
SRWV007 | Surgewave.Plugins | Disabled | Async lifecycle methods should accept CancellationToken
SRWV008 | Surgewave.Plugins | Disabled | Do not import Surgewave internal namespaces
SRWV009 | Surgewave.Plugins | Disabled | plugin.json version should match the project's package version
SRWV010 | Surgewave.Plugins | Warning | Use ILogger instead of Console.WriteLine
