namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Abstraction over signing and verification of <c>.swpkg</c> plugin packages.
/// Implementations differ in trust model (directory of public keys, X.509 chain,
/// HSM-backed, timestamp authority, ...), but the call surface is the same so that
/// <see cref="PluginPackageManager"/> is provider-agnostic.
/// </summary>
/// <remarks>
/// A single instance may be configured for signing only, verification only, or both,
/// depending on the provider's constructor. Calling <see cref="SignAsync"/> on a
/// verification-only instance (or vice versa) throws <see cref="InvalidOperationException"/>.
/// </remarks>
public interface ISppSigner
{
    /// <summary>
    /// Short stable identifier for this provider, e.g. <c>builtin-ecdsa</c> or <c>sealbolt</c>.
    /// Used for logging and to pick a provider by name from config.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Signs the package. Produces a detached signature artifact next to the package
    /// (e.g. <c>foo.swpkg.sig</c>) and returns its path.
    /// </summary>
    Task<string> SignAsync(string packagePath, CancellationToken ct = default);

    /// <summary>
    /// Verifies the package's signature against this provider's trust store.
    /// </summary>
    Task<SignatureVerification> VerifyAsync(string packagePath, CancellationToken ct = default);

    /// <summary>
    /// Whether a signature artifact exists for this package in a form this provider understands.
    /// Returning <c>true</c> does not imply the signature is valid — use <see cref="VerifyAsync"/> for that.
    /// </summary>
    bool HasSignature(string packagePath);
}

/// <summary>
/// Outcome of a signature check.
/// </summary>
public sealed record SignatureVerification(bool IsValid, string? SignerIdentity, string? Reason)
{
    public static SignatureVerification Valid(string signer) => new(true, signer, null);
    public static SignatureVerification Invalid(string reason) => new(false, null, reason);
    public static readonly SignatureVerification Unsigned = new(false, null, "Package has no signature");
}
