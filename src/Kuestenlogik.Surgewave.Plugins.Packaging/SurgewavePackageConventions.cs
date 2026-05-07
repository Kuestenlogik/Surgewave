namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Shared naming conventions for Surgewave plugin packages.
/// Centralised here so broker, packager, and tooling all agree on the same contract
/// without duplicating magic strings.
/// </summary>
public static class SurgewavePackageConventions
{
    /// <summary>
    /// Namespace prefix that identifies Surgewave host assemblies.
    /// Assemblies with this prefix are provided by the broker host and must not be
    /// bundled inside a <c>.swpkg</c> package's <c>deps/</c> directory.
    /// </summary>
    public const string HostAssemblyPrefix = "Kuestenlogik.Surgewave.";

    /// <summary>File name of the plugin manifest inside a <c>.swpkg</c> archive or installed plugin directory.</summary>
    public const string ManifestFileName = "plugin.json";

    /// <summary>
    /// Default file name for a plugin's bundled default settings, both inside the
    /// <c>.swpkg</c> archive and in the installed plugin directory. The broker layers
    /// this file into its <see cref="Microsoft.Extensions.Configuration.IConfiguration"/>
    /// at a lower priority than the broker's own <c>appsettings.json</c>, so plugin
    /// authors can ship recommended defaults without overriding user values.
    /// </summary>
    public const string PluginSettingsFileName = "pluginsettings.json";
}
