using Kuestenlogik.Surgewave.Plugins.Packaging;
using Xunit;

namespace Kuestenlogik.Surgewave.Plugins.Packaging.Testing;

/// <summary>
/// Shared contract test suite that every <see cref="ISppSigner"/> implementation is expected
/// to satisfy. Derive a concrete xunit test class in the provider's own test project and
/// override <see cref="CreateSigner"/> (for signing) and <see cref="CreateVerifier"/> (for
/// verification); the abstract class supplies the test methods so third-party providers can
/// pin themselves to the same behavioural baseline the in-tree providers are held to.
/// </summary>
/// <remarks>
/// Contract assertions covered:
/// <list type="bullet">
/// <item>Round-trip: a signed package verifies cleanly.</item>
/// <item>Tamper detection: modifying the package after signing invalidates the signature.</item>
/// <item>Unsigned short-circuit: <see cref="SignatureVerification.Unsigned"/> is returned without running any crypto.</item>
/// <item><see cref="ISppSigner.HasSignature"/> is <c>false</c> for unsigned packages.</item>
/// <item>The verifier exposes a stable <see cref="ISppSigner.Name"/> matching <see cref="ProviderName"/>.</item>
/// </list>
/// Providers with extra modes (key rotation, HSM, revocation...) add provider-specific tests in
/// their own test project on top of these.
/// </remarks>
public abstract class PluginPackageSignerContract
{
    /// <summary>Provider name that both signer and verifier must expose.</summary>
    protected abstract string ProviderName { get; }

    /// <summary>A fresh signer instance with signing capability wired up.</summary>
    protected abstract ISppSigner CreateSigner();

    /// <summary>
    /// A fresh verifier instance with a trust store that accepts the signatures produced by
    /// <see cref="CreateSigner"/>. May return the same instance as the signer if the provider
    /// supports both roles on one object.
    /// </summary>
    protected abstract ISppSigner CreateVerifier();

    private static string CreateTempPackage()
    {
        var path = Path.Combine(Path.GetTempPath(), $"pluginPackage-contract-{Guid.NewGuid():N}.swpkg");
        File.WriteAllBytes(path, System.Text.Encoding.UTF8.GetBytes("contract-test payload"));
        return path;
    }

    private static void CleanupPackage(string path)
    {
        if (File.Exists(path)) File.Delete(path);
        foreach (var ext in new[] { ".sig", ".cms" })
        {
            var sidecar = path + ext;
            if (File.Exists(sidecar)) File.Delete(sidecar);
        }
    }

    [Fact]
    public void ProviderNameIsStable()
    {
        Assert.Equal(ProviderName, CreateSigner().Name);
        Assert.Equal(ProviderName, CreateVerifier().Name);
    }

    [Fact]
    public void HasSignatureIsFalseForUnsignedPackage()
    {
        var pkg = CreateTempPackage();
        try
        {
            Assert.False(CreateVerifier().HasSignature(pkg));
        }
        finally { CleanupPackage(pkg); }
    }

    [Fact]
    public async Task VerifyUnsignedReturnsUnsignedResult()
    {
        var pkg = CreateTempPackage();
        try
        {
            var result = await CreateVerifier().VerifyAsync(pkg);
            Assert.Equal(SignatureVerification.Unsigned, result);
        }
        finally { CleanupPackage(pkg); }
    }

    [Fact]
    public async Task SignThenVerifySucceeds()
    {
        var pkg = CreateTempPackage();
        try
        {
            var sigPath = await CreateSigner().SignAsync(pkg);
            Assert.True(File.Exists(sigPath), "SignAsync must produce a sidecar signature file");
            Assert.True(CreateVerifier().HasSignature(pkg), "HasSignature must be true after signing");

            var result = await CreateVerifier().VerifyAsync(pkg);
            Assert.True(result.IsValid, $"Expected valid signature, got: {result.Reason}");
            Assert.False(string.IsNullOrWhiteSpace(result.SignerIdentity),
                "Valid verifications must report a non-empty SignerIdentity");
            Assert.Null(result.Reason);
        }
        finally { CleanupPackage(pkg); }
    }

    [Fact]
    public async Task TamperedPackageFailsVerification()
    {
        var pkg = CreateTempPackage();
        try
        {
            await CreateSigner().SignAsync(pkg);
            await File.AppendAllTextAsync(pkg, "tampered!");

            var result = await CreateVerifier().VerifyAsync(pkg);
            Assert.False(result.IsValid, "Tampered package must not validate");
            Assert.False(string.IsNullOrWhiteSpace(result.Reason),
                "Invalid verifications must provide a non-empty Reason");
        }
        finally { CleanupPackage(pkg); }
    }
}
