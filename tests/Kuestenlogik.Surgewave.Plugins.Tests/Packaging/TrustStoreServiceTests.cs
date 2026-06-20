using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

/// <summary>
/// Covers the public surface of <see cref="TrustStoreService"/>: list/upload/
/// delete/generate plus the name validation that protects against path
/// traversal in the Broker REST endpoints.
/// </summary>
public sealed class TrustStoreServiceTests : IDisposable
{
    private readonly string _root;
    private readonly TrustStoreService _svc;

    public TrustStoreServiceTests()
    {
        _root = Directory.CreateTempSubdirectory("surgewave-truststore-").FullName;
        _svc = new TrustStoreService(_root);
    }

    public void Dispose()
    {
        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }

    [Fact]
    public void List_EmptyDir_ReturnsNothing()
    {
        Assert.Empty(_svc.List());
    }

    [Fact]
    public void Generate_WritesPublicKey_AndReturnsPrivatePem()
    {
        var pair = _svc.Generate("alice");

        Assert.Equal("alice", pair.KeyName);
        Assert.Contains("BEGIN PUBLIC KEY", pair.PublicKeyPem);
        Assert.Contains("BEGIN EC PRIVATE KEY", pair.PrivateKeyPem);
        Assert.NotEmpty(pair.Fingerprint);
        Assert.True(File.Exists(Path.Combine(_root, "alice.pub")));
        // Private key MUST NOT be persisted server-side.
        Assert.False(File.Exists(Path.Combine(_root, "alice.key")));
    }

    [Fact]
    public void Generate_TwiceWithSameName_Throws()
    {
        _svc.Generate("alice");
        Assert.Throws<InvalidOperationException>(() => _svc.Generate("alice"));
    }

    [Fact]
    public void List_AfterGenerate_IncludesNewKey()
    {
        var pair = _svc.Generate("alice");
        var entries = _svc.List();

        var alice = Assert.Single(entries);
        Assert.Equal("alice", alice.Name);
        Assert.Equal(pair.Fingerprint, alice.Fingerprint);
        Assert.True(alice.SizeBytes > 0);
    }

    [Fact]
    public async Task UploadAsync_ValidPem_PersistsKey()
    {
        var donor = _svc.Generate("donor");
        _svc.Delete("donor"); // free the name for re-upload

        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes(donor.PublicKeyPem));
        var info = await _svc.UploadAsync("imported", stream);

        Assert.Equal("imported", info.Name);
        Assert.Equal(donor.Fingerprint, info.Fingerprint);
        Assert.True(File.Exists(Path.Combine(_root, "imported.pub")));
    }

    [Fact]
    public async Task UploadAsync_InvalidPem_Throws()
    {
        using var stream = new MemoryStream(System.Text.Encoding.UTF8.GetBytes("-----BEGIN PUBLIC KEY-----\nnot-base64\n-----END PUBLIC KEY-----"));
        await Assert.ThrowsAnyAsync<Exception>(async () => await _svc.UploadAsync("garbage", stream));
    }

    [Theory]
    [InlineData("../escape")]
    [InlineData("with/slash")]
    [InlineData("with\\backslash")]
    [InlineData("")]
    [InlineData("a b")]        // space disallowed
    public void Generate_InvalidName_Throws(string keyName)
    {
        Assert.ThrowsAny<ArgumentException>(() => _svc.Generate(keyName));
    }

    [Fact]
    public void Delete_KnownKey_ReturnsTrueAndRemoves()
    {
        _svc.Generate("alice");
        Assert.True(_svc.Delete("alice"));
        Assert.False(File.Exists(Path.Combine(_root, "alice.pub")));
    }

    [Fact]
    public void Delete_UnknownKey_ReturnsFalse()
    {
        Assert.False(_svc.Delete("never-existed"));
    }

    [Fact]
    public void Generate_TwoDifferentNames_HaveDifferentFingerprints()
    {
        var alice = _svc.Generate("alice");
        var bob = _svc.Generate("bob");
        Assert.NotEqual(alice.Fingerprint, bob.Fingerprint);
    }
}

