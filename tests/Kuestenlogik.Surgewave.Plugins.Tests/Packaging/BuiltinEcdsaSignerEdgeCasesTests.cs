using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

/// <summary>
/// Edge-case-Tests fuer <see cref="BuiltinEcdsaSigner"/> — gezielt die Error-Pfade
/// die der Happy-Path-Contract-Test nicht beruehrt: missing private key, missing
/// trust dir, malformed signature file, malformed key in trust store, unsigned-detection.
/// </summary>
public sealed class BuiltinEcdsaSignerEdgeCasesTests : IDisposable
{
    private readonly string _root;

    public BuiltinEcdsaSignerEdgeCasesTests()
    {
        _root = Path.Combine(Path.GetTempPath(), $"sw-signer-edge-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_root);
    }

    [Fact]
    public async Task SignAsync_NoPrivateKey_Throws()
    {
        var signer = new BuiltinEcdsaSigner();  // no private key path
        var pkg = await CreateDummyPackageAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => signer.SignAsync(pkg));
    }

    [Fact]
    public async Task SignAsync_PrivateKeyPathDoesNotExist_Throws()
    {
        var signer = new BuiltinEcdsaSigner(privateKeyPath: Path.Combine(_root, "missing.key"));
        var pkg = await CreateDummyPackageAsync();

        await Assert.ThrowsAsync<FileNotFoundException>(() => signer.SignAsync(pkg));
    }

    [Fact]
    public void HasSignature_NoSidecar_ReturnsFalse()
    {
        var signer = new BuiltinEcdsaSigner();
        var bogusPath = Path.Combine(_root, "no-sig-here.swpkg");

        Assert.False(signer.HasSignature(bogusPath));
    }

    [Fact]
    public async Task VerifyAsync_NoSignature_ReturnsUnsigned()
    {
        var signer = new BuiltinEcdsaSigner();
        var pkg = await CreateDummyPackageAsync();

        var result = await signer.VerifyAsync(pkg);

        Assert.False(result.IsValid);
        // SignatureVerification.Unsigned semantics
    }

    [Fact]
    public async Task VerifyAsync_HasSignatureButNoTrustDir_Throws()
    {
        var pkg = await CreateDummyPackageAsync();
        await File.WriteAllTextAsync(pkg + ".sig", Convert.ToBase64String(new byte[] { 0x01, 0x02 }));

        var signer = new BuiltinEcdsaSigner();  // no trustedKeysDir

        await Assert.ThrowsAsync<InvalidOperationException>(() => signer.VerifyAsync(pkg));
    }

    [Fact]
    public async Task VerifyAsync_TrustDirDoesNotExist_ReturnsInvalid()
    {
        var pkg = await CreateDummyPackageAsync();
        await File.WriteAllTextAsync(pkg + ".sig", Convert.ToBase64String(new byte[] { 0x01, 0x02 }));

        var signer = new BuiltinEcdsaSigner(trustedKeysDir: Path.Combine(_root, "does-not-exist"));

        var result = await signer.VerifyAsync(pkg);

        Assert.False(result.IsValid);
        Assert.Contains("not found", result.Reason ?? "", StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task VerifyAsync_MalformedBase64_ReturnsInvalid()
    {
        var pkg = await CreateDummyPackageAsync();
        await File.WriteAllTextAsync(pkg + ".sig", "this is not valid base64 !!!");
        var trustDir = Path.Combine(_root, "trust");
        Directory.CreateDirectory(trustDir);

        var signer = new BuiltinEcdsaSigner(trustedKeysDir: trustDir);

        var result = await signer.VerifyAsync(pkg);

        Assert.False(result.IsValid);
        Assert.Contains("base64", result.Reason ?? "", StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task VerifyAsync_NoTrustedKeyMatches_ReturnsInvalid()
    {
        // Generate a fresh key pair but DO NOT add the public key to the trust store
        var keyDir = Path.Combine(_root, "keys");
        var trustDir = Path.Combine(_root, "trust");
        Directory.CreateDirectory(trustDir);
        var (privPath, _) = BuiltinEcdsaSigner.GenerateKeyPair(keyDir, "alice");

        // Sign with alice
        var pkg = await CreateDummyPackageAsync();
        var signer = new BuiltinEcdsaSigner(privateKeyPath: privPath);
        await signer.SignAsync(pkg);

        // Verify with empty trust store
        var verifier = new BuiltinEcdsaSigner(trustedKeysDir: trustDir);
        var result = await verifier.VerifyAsync(pkg);

        Assert.False(result.IsValid);
        Assert.Contains("No trusted key", result.Reason ?? "", StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task VerifyAsync_MalformedTrustKey_SkipsAndContinues()
    {
        var keyDir = Path.Combine(_root, "keys");
        var trustDir = Path.Combine(_root, "trust");
        Directory.CreateDirectory(trustDir);
        var (privPath, pubPath) = BuiltinEcdsaSigner.GenerateKeyPair(keyDir, "good");

        // Add good key + a malformed one
        File.Copy(pubPath, Path.Combine(trustDir, "good.pub"));
        await File.WriteAllTextAsync(Path.Combine(trustDir, "bad.pub"), "not a pem key");

        var pkg = await CreateDummyPackageAsync();
        var signer = new BuiltinEcdsaSigner(privateKeyPath: privPath);
        await signer.SignAsync(pkg);

        var verifier = new BuiltinEcdsaSigner(trustedKeysDir: trustDir);
        var result = await verifier.VerifyAsync(pkg);

        Assert.True(result.IsValid);
        Assert.Equal("good", result.SignerIdentity);
    }

    [Fact]
    public void GenerateKeyPair_CreatesBothFiles_NonEmpty()
    {
        var (priv, pub) = BuiltinEcdsaSigner.GenerateKeyPair(_root, "kp");

        Assert.True(File.Exists(priv));
        Assert.True(File.Exists(pub));
        Assert.NotEmpty(File.ReadAllText(priv));
        Assert.NotEmpty(File.ReadAllText(pub));
        Assert.EndsWith(".key", priv);
        Assert.EndsWith(".pub", pub);
    }

    [Fact]
    public void GenerateKeyPair_NonExistentOutputDir_IsCreated()
    {
        var subDir = Path.Combine(_root, "fresh-subdir");
        Assert.False(Directory.Exists(subDir));

        var (priv, _) = BuiltinEcdsaSigner.GenerateKeyPair(subDir, "x");

        Assert.True(Directory.Exists(subDir));
        Assert.True(File.Exists(priv));
    }

    [Fact]
    public void Name_IsBuiltinEcdsa()
    {
        var signer = new BuiltinEcdsaSigner();
        Assert.Equal("builtin-ecdsa", signer.Name);
    }

    private async Task<string> CreateDummyPackageAsync()
    {
        var path = Path.Combine(_root, $"dummy-{Guid.NewGuid():N}.swpkg");
        await File.WriteAllBytesAsync(path, [0x50, 0x4b, 0x03, 0x04, 0x00, 0x00]);
        return path;
    }

    public void Dispose()
    {
        if (Directory.Exists(_root))
        {
            try { Directory.Delete(_root, recursive: true); }
            catch { /* best effort */ }
        }
    }
}
