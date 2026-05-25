namespace Kuestenlogik.Surgewave.Plugins.Packaging.Hosting;

/// <summary>
/// Configuration for the signature verifier that <c>PluginPackageManager.InstallAsync</c> uses
/// at install time. Bound from <c>Surgewave:Plugins:Signer</c> in <c>appsettings.json</c>.
/// </summary>
public sealed class SignerOptions
{
    public const string ConfigSection = "Surgewave:Plugins:Signer";

    /// <summary>
    /// Name of the <see cref="ISppSignerProvider"/> to resolve from
    /// <see cref="PluginPackageSignerRegistry"/>. Defaults to the always-present built-in ECDSA provider.
    /// </summary>
    public string Name { get; set; } = "builtin-ecdsa";

    /// <summary>
    /// Provider-specific options passed verbatim to <see cref="ISppSignerProvider.Create"/>.
    /// Keys and meaning depend on the chosen provider (<c>private-key</c>,
    /// <c>trusted-keys-dir</c>, <c>cert-path</c>, <c>roots</c>, ...).
    /// </summary>
    public Dictionary<string, string> Options { get; set; } = [];

    /// <summary>
    /// When <c>true</c>, unsigned packages are rejected during install. Signed-but-untrusted
    /// packages are always rejected. Defaults to <c>false</c> so a fresh install can run
    /// without a configured trust store.
    /// </summary>
    public bool RequireSignedPackages { get; set; }
}
