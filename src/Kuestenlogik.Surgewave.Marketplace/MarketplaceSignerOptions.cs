namespace Kuestenlogik.Surgewave.Marketplace;

/// <summary>
/// Configuration that tells the marketplace how to handle signatures on uploaded packages.
/// Bound from <c>Surgewave:Marketplace:Signing</c>. Independent from the install-time
/// <c>Surgewave:Plugins:Signer</c> config so the marketplace and the host can use different
/// providers (e.g. the marketplace enforces publisher signatures while the host still
/// trusts its own builtin-ecdsa store at install time).
/// </summary>
public sealed class MarketplaceSignerOptions
{
    public const string ConfigSection = "Surgewave:Marketplace:Signing";

    /// <summary>
    /// Name of the <see cref="Kuestenlogik.Surgewave.Plugins.Packaging.ISppSignerProvider"/> that verifies
    /// uploads. Defaults to <c>builtin-ecdsa</c>; set to <c>sealbolt</c> when the enterprise
    /// signer plugin is installed next to the marketplace.
    /// </summary>
    public string SignerName { get; set; } = "builtin-ecdsa";

    /// <summary>
    /// Provider-specific options passed verbatim to <see cref="Kuestenlogik.Surgewave.Plugins.Packaging.ISppSignerProvider.Create"/>.
    /// For builtin-ecdsa: <c>trusted-keys-dir</c>. For sealbolt: <c>roots</c> / <c>require-revocation-check</c>.
    /// </summary>
    public Dictionary<string, string> SignerOptions { get; set; } = [];

    /// <summary>
    /// Plugin directory used to discover additional <see cref="Kuestenlogik.Surgewave.Plugins.Packaging.ISppSignerProvider"/>
    /// implementations. When empty, the resolved plugin scopes are searched
    /// (installation, machine, user) — see
    /// <see cref="Kuestenlogik.Surgewave.Plugins.Packaging.SurgewavePluginDirectories"/>.
    /// </summary>
    /// <remarks>
    /// The default used to be the literal <c>plugins</c>, resolved against the
    /// marketplace's working directory. A signer provider installed as a plugin was
    /// therefore found only when the marketplace happened to be started from the
    /// directory above it, and an upload signed by that provider was rejected as
    /// misconfigured everywhere else (#158).
    /// </remarks>
    public string PluginsDirectory { get; set; } = "";

    /// <summary>
    /// When <c>true</c>, uploads without a signature sidecar are rejected. When <c>false</c>
    /// (default), unsigned uploads are accepted and published as unverified. Signed-but-invalid
    /// uploads are always rejected regardless of this flag.
    /// </summary>
    public bool RequireSignedUploads { get; set; }
}
