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

    /// <summary>
    /// Kebab-case AssemblyName prefix used by Surgewave executables
    /// (e.g. <c>surgewave-broker.dll</c>, <c>surgewave-control.dll</c>, <c>surgewave-gateway.dll</c>).
    /// These ship the host's own built-in <c>IBrokerPlugin</c>/<c>IProtocolPlugin</c> implementations
    /// and must be included in plugin-discovery scans even though they do not match
    /// <see cref="HostAssemblyPrefix"/>.
    /// </summary>
    public const string ExecutableAssemblyPrefix = "surgewave-";

    /// <summary>
    /// Returns <c>true</c> when the assembly name is a Surgewave host or executable assembly
    /// that should participate in plugin discovery. Centralised so broker / tooling / packager
    /// all apply the same rule.
    /// </summary>
    public static bool IsSurgewaveHostAssembly(string? assemblyName) =>
        assemblyName is not null &&
        (assemblyName.StartsWith(HostAssemblyPrefix, System.StringComparison.OrdinalIgnoreCase)
         || assemblyName.StartsWith(ExecutableAssemblyPrefix, System.StringComparison.OrdinalIgnoreCase));

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
