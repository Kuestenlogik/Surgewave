using System.Net;
using System.Text;
using Kuestenlogik.Surgewave.Cli.Commands.Sdk;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Tool.Tests;

[Trait("Category", TestCategories.Unit)]
public sealed class SdkInstallerTests
{
    [Fact]
    public async Task ResolveTagAsync_LiteralVersion_PrependsV()
    {
        var http = new HttpClient(new StubHandler((_, _) =>
            throw new InvalidOperationException("Network must not be touched when a literal version is given.")));
        var installer = new SdkInstaller(http);

        Assert.Equal("v0.1.13", await installer.ResolveTagAsync("Kuestenlogik", "Surgewave", "0.1.13", CancellationToken.None));
        Assert.Equal("v0.1.13", await installer.ResolveTagAsync("Kuestenlogik", "Surgewave", "v0.1.13", CancellationToken.None));
    }

    [Fact]
    public async Task ResolveTagAsync_Latest_HitsReleasesLatest()
    {
        var http = new HttpClient(new StubHandler((req, _) =>
        {
            Assert.Equal("https://api.github.com/repos/Kuestenlogik/Surgewave/releases/latest", req.RequestUri!.ToString());
            return Json("""{ "tag_name": "v0.1.99" }""");
        }));
        var installer = new SdkInstaller(http);

        Assert.Equal("v0.1.99", await installer.ResolveTagAsync("Kuestenlogik", "Surgewave", "latest", CancellationToken.None));
    }

    [Fact]
    public async Task ListNupkgAssetsAsync_FiltersSymbolPackages()
    {
        var http = new HttpClient(new StubHandler((req, _) =>
        {
            Assert.Contains("/releases/tags/v0.1.13", req.RequestUri!.ToString());
            return Json("""
                {
                  "assets": [
                    { "name": "Pkg.A.0.1.13.nupkg", "browser_download_url": "https://x/A.nupkg", "size": 100 },
                    { "name": "Pkg.A.0.1.13.symbols.nupkg", "browser_download_url": "https://x/As.nupkg", "size": 200 },
                    { "name": "Pkg.A.0.1.13.snupkg", "browser_download_url": "https://x/Asn.nupkg", "size": 300 },
                    { "name": "Pkg.B.0.1.13.nupkg", "browser_download_url": "https://x/B.nupkg", "size": 400 },
                    { "name": "broker-linux.tar.gz", "browser_download_url": "https://x/broker.tar.gz", "size": 500 }
                  ]
                }
                """);
        }));
        var installer = new SdkInstaller(http);

        var assets = await installer.ListNupkgAssetsAsync("o", "r", "v0.1.13", CancellationToken.None);

        Assert.Equal(2, assets.Count);
        Assert.Equal("Pkg.A.0.1.13.nupkg", assets[0].Name);
        Assert.Equal("Pkg.B.0.1.13.nupkg", assets[1].Name);
    }

    [Fact]
    public async Task ListNupkgAssetsAsync_NoNupkgs_Throws()
    {
        var http = new HttpClient(new StubHandler((_, _) => Json("""{ "assets": [{ "name": "broker.tar.gz", "browser_download_url": "https://x/b", "size": 1 }] }""")));
        var installer = new SdkInstaller(http);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            installer.ListNupkgAssetsAsync("o", "r", "v0.1.13", CancellationToken.None));
    }

    [Fact]
    public async Task DownloadAsync_SkipsExistingFiles()
    {
        var tempDir = NewTempDir();
        var existingPath = Path.Combine(tempDir, "Pkg.0.1.13.nupkg");
        File.WriteAllText(existingPath, "old-content");

        var http = new HttpClient(new StubHandler((_, _) =>
            throw new InvalidOperationException("Should not download when file already exists and force=false.")));
        var installer = new SdkInstaller(http);

        var result = await installer.DownloadAsync(
            [new NupkgAsset("Pkg.0.1.13.nupkg", "https://x/p.nupkg", 100)],
            tempDir, force: false, CancellationToken.None);

        Assert.Equal(0, result.Downloaded);
        Assert.Equal(1, result.Skipped);
        Assert.Equal("old-content", File.ReadAllText(existingPath));
    }

    [Fact]
    public async Task DownloadAsync_Force_RedownloadsExistingFiles()
    {
        var tempDir = NewTempDir();
        var existingPath = Path.Combine(tempDir, "Pkg.0.1.13.nupkg");
        File.WriteAllText(existingPath, "old-content");

        var http = new HttpClient(new StubHandler((_, _) =>
            new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("new-content") }));
        var installer = new SdkInstaller(http);

        var result = await installer.DownloadAsync(
            [new NupkgAsset("Pkg.0.1.13.nupkg", "https://x/p.nupkg", 100)],
            tempDir, force: true, CancellationToken.None);

        Assert.Equal(1, result.Downloaded);
        Assert.Equal(0, result.Skipped);
        Assert.Equal("new-content", File.ReadAllText(existingPath));
    }

    [Fact]
    public void WriteNugetConfig_CreatesPackageSourceEntry()
    {
        var projectDir = NewTempDir();
        var feedDir = NewTempDir();

        SdkInstaller.WriteNugetConfig(projectDir, feedDir);

        var configPath = Path.Combine(projectDir, "nuget.config");
        var xml = File.ReadAllText(configPath);

        Assert.Contains("<add key=\"surgewave-sdk-local\"", xml);
        Assert.Contains($"value=\"{feedDir}\"", xml);
        Assert.Contains("<clear />", xml);
        Assert.Contains("nuget.org", xml);
    }

    private static HttpResponseMessage Json(string body) =>
        new(HttpStatusCode.OK) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    private static string NewTempDir()
    {
        var d = Path.Combine(Path.GetTempPath(), $"sdk-installer-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(d);
        return d;
    }

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, CancellationToken, HttpResponseMessage> _handler;
        public StubHandler(Func<HttpRequestMessage, CancellationToken, HttpResponseMessage> handler) => _handler = handler;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct) =>
            Task.FromResult(_handler(request, ct));
    }
}
