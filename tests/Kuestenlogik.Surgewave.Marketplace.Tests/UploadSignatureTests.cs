using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Marketplace.Tests;

public sealed class UploadSignatureTests
{
    private static async Task<HttpResponseMessage> UploadAsync(
        HttpClient client,
        string packagePath,
        string? signaturePath = null)
    {
        using var form = new MultipartFormDataContent();
        var packageStream = File.OpenRead(packagePath);
        var packageContent = new StreamContent(packageStream);
        packageContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/octet-stream");
        form.Add(packageContent, "file", Path.GetFileName(packagePath));

        if (signaturePath is not null)
        {
            var sigStream = File.OpenRead(signaturePath);
            var sigContent = new StreamContent(sigStream);
            form.Add(sigContent, "signature", Path.GetFileName(signaturePath));
        }

        return await client.PutAsync("/api/v1/packages", form);
    }

    [Fact]
    public async Task Unsigned_upload_accepted_when_optional()
    {
        await using var fx = new MarketplaceTestFixture(requireSignedUploads: false);
        var pkg = fx.BuildPackage();

        using var response = await UploadAsync(fx.Client, pkg);

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
    }

    [Fact]
    public async Task Unsigned_upload_rejected_when_required()
    {
        await using var fx = new MarketplaceTestFixture(requireSignedUploads: true);
        var pkg = fx.BuildPackage();

        using var response = await UploadAsync(fx.Client, pkg);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("RequireSignedUploads", body, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Signed_upload_accepted_and_metadata_records_signer()
    {
        await using var fx = new MarketplaceTestFixture(requireSignedUploads: true);
        var pkg = await fx.BuildSignedPackageAsync();
        var sig = pkg + ".sig";

        using var response = await UploadAsync(fx.Client, pkg, sig);
        Assert.Equal(HttpStatusCode.Created, response.StatusCode);

        using var metaResponse = await fx.Client.GetAsync("/api/v1/packages/test.plugin/1.0.0/metadata");
        metaResponse.EnsureSuccessStatusCode();
        var meta = await metaResponse.Content.ReadFromJsonAsync<JsonElement>();

        Assert.True(meta.GetProperty("isSigned").GetBoolean());
        Assert.Equal("test-publisher", meta.GetProperty("signerIdentity").GetString());
        Assert.Equal("builtin-ecdsa", meta.GetProperty("signerProvider").GetString());
    }

    [Fact]
    public async Task Signed_upload_rejected_when_publisher_key_not_trusted()
    {
        await using var fx = new MarketplaceTestFixture(requireSignedUploads: true);
        var pkg = fx.BuildPackage();

        // Sign with a key that was NOT added to the trust store.
        var unknownKeyDir = Path.Combine(Path.GetTempPath(), "unknown-publisher-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(unknownKeyDir);
        try
        {
            var (unknownPrivate, _) = Kuestenlogik.Surgewave.Plugins.Packaging.BuiltinEcdsaSigner.GenerateKeyPair(unknownKeyDir, "unknown");
            var signer = new Kuestenlogik.Surgewave.Plugins.Packaging.BuiltinEcdsaSigner(privateKeyPath: unknownPrivate);
            await signer.SignAsync(pkg);

            using var response = await UploadAsync(fx.Client, pkg, pkg + ".sig");

            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
            var body = await response.Content.ReadAsStringAsync();
            Assert.Contains("Signature verification failed", body, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(unknownKeyDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public async Task Tampered_package_rejected()
    {
        await using var fx = new MarketplaceTestFixture();
        var pkg = await fx.BuildSignedPackageAsync();
        var sig = pkg + ".sig";

        // Tamper: append bytes so the signed hash no longer matches.
        await File.AppendAllTextAsync(pkg, "tampered");

        using var response = await UploadAsync(fx.Client, pkg, sig);

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
    }

    [Fact]
    public async Task Signature_sidecar_is_retrievable_after_upload()
    {
        await using var fx = new MarketplaceTestFixture();
        var pkg = await fx.BuildSignedPackageAsync();
        var sig = pkg + ".sig";

        using var upload = await UploadAsync(fx.Client, pkg, sig);
        Assert.Equal(HttpStatusCode.Created, upload.StatusCode);

        using var sigResponse = await fx.Client.GetAsync("/api/v1/packages/test.plugin/1.0.0/signature");
        Assert.Equal(HttpStatusCode.OK, sigResponse.StatusCode);

        var originalSig = await File.ReadAllBytesAsync(sig);
        var servedSig = await sigResponse.Content.ReadAsByteArrayAsync();
        Assert.Equal(originalSig, servedSig);
    }

    [Fact]
    public async Task Unsigned_package_metadata_has_isSigned_false()
    {
        await using var fx = new MarketplaceTestFixture();
        var pkg = fx.BuildPackage();

        using var response = await UploadAsync(fx.Client, pkg);
        Assert.Equal(HttpStatusCode.Created, response.StatusCode);

        using var metaResponse = await fx.Client.GetAsync("/api/v1/packages/test.plugin/1.0.0/metadata");
        var meta = await metaResponse.Content.ReadFromJsonAsync<JsonElement>();

        Assert.False(meta.GetProperty("isSigned").GetBoolean());
        Assert.Equal(JsonValueKind.Null, meta.GetProperty("signerIdentity").ValueKind);
    }

    [Fact]
    public async Task Missing_signature_endpoint_returns_404()
    {
        await using var fx = new MarketplaceTestFixture();
        var pkg = fx.BuildPackage();

        using var upload = await UploadAsync(fx.Client, pkg);
        Assert.Equal(HttpStatusCode.Created, upload.StatusCode);

        using var sigResponse = await fx.Client.GetAsync("/api/v1/packages/test.plugin/1.0.0/signature");
        Assert.Equal(HttpStatusCode.NotFound, sigResponse.StatusCode);
    }
}
