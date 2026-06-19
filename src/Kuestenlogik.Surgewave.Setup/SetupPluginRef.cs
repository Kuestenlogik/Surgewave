namespace Kuestenlogik.Surgewave.Setup;

/// <summary>
/// Minimal plugin descriptor the generators consume — just the
/// NuGet-style id and version that end up in the `surgewave plugins
/// install` lines. Callers (CLI + Control) map their own richer types
/// (PluginMarketplaceEntry, PluginInfo) into this at the boundary so
/// the generators stay free of marketplace + repository dependencies.
/// </summary>
public sealed record SetupPluginRef(string PackageId, string Version);
