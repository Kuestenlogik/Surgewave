using System.Net;
using System.Net.Http.Json;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Marketplace.Tests;

public sealed class SbomEndpointTests
{
    private static async Task<HttpResponseMessage> UploadAsync(HttpClient client, string packagePath)
    {
        using var form = new MultipartFormDataContent();
        var packageStream = File.OpenRead(packagePath);
        var packageContent = new StreamContent(packageStream);
        packageContent.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue("application/octet-stream");
        form.Add(packageContent, "file", Path.GetFileName(packagePath));
        return await client.PutAsync("/api/v1/packages", form);
    }

    [Fact]
    public async Task Upload_with_sbom_records_HasSbom_true_and_endpoint_serves_it()
    {
        await using var fx = new MarketplaceTestFixture();
        var pkg = fx.BuildPackage(includeSbom: true);

        using var upload = await UploadAsync(fx.Client, pkg);
        Assert.Equal(HttpStatusCode.Created, upload.StatusCode);

        using var metaResponse = await fx.Client.GetAsync("/api/v1/packages/test.plugin/1.0.0/metadata");
        var meta = await metaResponse.Content.ReadFromJsonAsync<JsonElement>();
        Assert.True(meta.GetProperty("hasSbom").GetBoolean());

        using var sbomResponse = await fx.Client.GetAsync("/api/v1/packages/test.plugin/1.0.0/sbom");
        Assert.Equal(HttpStatusCode.OK, sbomResponse.StatusCode);
        Assert.Equal("application/vnd.cyclonedx+json", sbomResponse.Content.Headers.ContentType?.MediaType);

        using var sbomDoc = JsonDocument.Parse(await sbomResponse.Content.ReadAsStringAsync());
        Assert.Equal("CycloneDX", sbomDoc.RootElement.GetProperty("bomFormat").GetString());
    }

    [Fact]
    public async Task Upload_without_sbom_records_HasSbom_false()
    {
        await using var fx = new MarketplaceTestFixture();
        var pkg = fx.BuildPackage(includeSbom: false);

        using var upload = await UploadAsync(fx.Client, pkg);
        Assert.Equal(HttpStatusCode.Created, upload.StatusCode);

        using var metaResponse = await fx.Client.GetAsync("/api/v1/packages/test.plugin/1.0.0/metadata");
        var meta = await metaResponse.Content.ReadFromJsonAsync<JsonElement>();
        Assert.False(meta.GetProperty("hasSbom").GetBoolean());
    }

    [Fact]
    public async Task Sbom_endpoint_returns_404_when_package_has_no_sbom()
    {
        await using var fx = new MarketplaceTestFixture();
        var pkg = fx.BuildPackage(includeSbom: false);

        using var upload = await UploadAsync(fx.Client, pkg);
        Assert.Equal(HttpStatusCode.Created, upload.StatusCode);

        using var sbomResponse = await fx.Client.GetAsync("/api/v1/packages/test.plugin/1.0.0/sbom");
        Assert.Equal(HttpStatusCode.NotFound, sbomResponse.StatusCode);
    }
}
