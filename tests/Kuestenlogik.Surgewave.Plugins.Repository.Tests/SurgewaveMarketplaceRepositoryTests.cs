using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Repository.Tests;

/// <summary>
/// Tests fuer <see cref="SurgewaveMarketplaceRepository"/>. Die Klasse hat keinen
/// HttpClient-Hook im Konstruktor — daher wird der interne <c>_httpClient</c> via
/// Reflection durch eine Instanz mit <see cref="StubHandler"/> ersetzt, bevor die
/// Methoden gerufen werden.
/// </summary>
public sealed class SurgewaveMarketplaceRepositoryTests
{
    private static (SurgewaveMarketplaceRepository repo, StubHandler handler) Create(string source = "https://market.example.com")
    {
        var repo = new SurgewaveMarketplaceRepository("marketplace", source);
        var handler = new StubHandler();
        var stubbed = new HttpClient(handler) { BaseAddress = new Uri(source.TrimEnd('/')) };

        var field = typeof(SurgewaveMarketplaceRepository)
            .GetField("_httpClient", BindingFlags.NonPublic | BindingFlags.Instance)!;
        ((HttpClient)field.GetValue(repo)!).Dispose();
        field.SetValue(repo, stubbed);

        return (repo, handler);
    }

    [Fact]
    public void Constructor_TrimsTrailingSlashFromSource()
    {
        using var repo = new SurgewaveMarketplaceRepository("m", "https://market.example.com/");

        Assert.Equal("https://market.example.com", repo.Source);
        Assert.Equal("m", repo.Name);
    }

    [Fact]
    public async Task SearchAsync_ReturnsMappedPackages()
    {
        var (repo, handler) = Create();
        try
        {
            handler.SetJsonResponse(
                "https://market.example.com/api/v1/search?skip=0&take=20&q=akka",
                new
                {
                    totalHits = 1,
                    data = new[]
                    {
                        new
                        {
                            id = "Kuestenlogik.Surgewave.Connector.Akka",
                            version = "1.2.0",
                            name = "Akka Connector",
                            description = "Akka.NET integration",
                            authors = new[] { "Kuestenlogik", "Community" },
                            tags = new[] { "akka", "streaming" },
                            license = "Apache-2.0",
                            downloadCount = 4711L,
                            publishedAt = "2026-05-01T10:00:00Z",
                            allVersions = new[] { "1.2.0", "1.1.0" },
                            isSigned = true,
                            signerIdentity = "kuestenlogik",
                            signerProvider = "builtin-ecdsa",
                        },
                    },
                });

            var results = await repo.SearchAsync(query: "akka");

            var pkg = Assert.Single(results);
            Assert.Equal("Kuestenlogik.Surgewave.Connector.Akka", pkg.PackageId);
            Assert.Equal("1.2.0", pkg.Version);
            Assert.Equal("Akka Connector", pkg.Name);
            Assert.Equal("Kuestenlogik, Community", pkg.Author);
            Assert.Equal(["akka", "streaming"], pkg.Tags);
            Assert.Equal("Apache-2.0", pkg.License);
            Assert.Equal(4711L, pkg.DownloadCount);
            Assert.True(pkg.IsSigned);
            Assert.Equal("kuestenlogik", pkg.SignerIdentity);
            Assert.Equal("builtin-ecdsa", pkg.SignerProvider);
            Assert.Equal(["1.2.0", "1.1.0"], pkg.AvailableVersions);
        }
        finally
        {
            repo.Dispose();
        }
    }

    [Fact]
    public async Task SearchAsync_EmptyData_ReturnsEmptyList()
    {
        var (repo, handler) = Create();
        try
        {
            handler.SetJsonResponse(
                "https://market.example.com/api/v1/search?skip=0&take=20",
                new { totalHits = 0, data = Array.Empty<object>() });

            var results = await repo.SearchAsync(query: null);

            Assert.Empty(results);
        }
        finally
        {
            repo.Dispose();
        }
    }

    [Fact]
    public async Task SearchAsync_NullData_ReturnsEmptyList()
    {
        var (repo, handler) = Create();
        try
        {
            handler.SetJsonResponse(
                "https://market.example.com/api/v1/search?skip=0&take=20",
                new { totalHits = 0, data = (object?)null });

            var results = await repo.SearchAsync(query: null);

            Assert.Empty(results);
        }
        finally
        {
            repo.Dispose();
        }
    }

    [Fact]
    public async Task GetVersionsAsync_Found_ReturnsList()
    {
        var (repo, handler) = Create();
        try
        {
            handler.SetJsonResponse(
                "https://market.example.com/api/v1/packages/foo/index.json",
                new { versions = new[] { "2.0.0", "1.0.0" } });

            var versions = await repo.GetVersionsAsync("foo");

            Assert.Equal(["2.0.0", "1.0.0"], versions);
        }
        finally
        {
            repo.Dispose();
        }
    }

    [Fact]
    public async Task GetVersionsAsync_HttpError_ReturnsEmptyList()
    {
        var (repo, handler) = Create();
        try
        {
            handler.SetException(
                "https://market.example.com/api/v1/packages/foo/index.json",
                new HttpRequestException("nope"));

            var versions = await repo.GetVersionsAsync("foo");

            Assert.Empty(versions);
        }
        finally
        {
            repo.Dispose();
        }
    }

    [Fact]
    public async Task GetVersionsAsync_NullVersionsField_ReturnsEmptyList()
    {
        var (repo, handler) = Create();
        try
        {
            handler.SetJsonResponse(
                "https://market.example.com/api/v1/packages/foo/index.json",
                new { });  // no "versions" field

            var versions = await repo.GetVersionsAsync("foo");

            Assert.Empty(versions);
        }
        finally
        {
            repo.Dispose();
        }
    }

    [Fact]
    public async Task GetPackageAsync_NoVersion_FetchesVersionsFirstThenMetadata()
    {
        var (repo, handler) = Create();
        try
        {
            handler.SetJsonResponse(
                "https://market.example.com/api/v1/packages/foo/index.json",
                new { versions = new[] { "1.0.0", "2.0.0" } });
            handler.SetJsonResponse(
                "https://market.example.com/api/v1/packages/foo/2.0.0/metadata",
                new
                {
                    id = "foo",
                    version = "2.0.0",
                    name = "Foo",
                    authors = new[] { "Kuestenlogik" },
                });

            var pkg = await repo.GetPackageAsync("foo");

            Assert.NotNull(pkg);
            Assert.Equal("2.0.0", pkg!.Version);
            Assert.Equal("Kuestenlogik", pkg.Author);
        }
        finally
        {
            repo.Dispose();
        }
    }

    [Fact]
    public async Task GetPackageAsync_NoVersionsAvailable_ReturnsNull()
    {
        var (repo, handler) = Create();
        try
        {
            handler.SetJsonResponse(
                "https://market.example.com/api/v1/packages/foo/index.json",
                new { versions = Array.Empty<string>() });

            var pkg = await repo.GetPackageAsync("foo");

            Assert.Null(pkg);
        }
        finally
        {
            repo.Dispose();
        }
    }

    [Fact]
    public async Task GetPackageAsync_ExplicitVersion_FetchesMetadataDirectly()
    {
        var (repo, handler) = Create();
        try
        {
            handler.SetJsonResponse(
                "https://market.example.com/api/v1/packages/foo/1.5.0/metadata",
                new
                {
                    id = "foo",
                    version = "1.5.0",
                    name = "Foo",
                });

            var pkg = await repo.GetPackageAsync("foo", version: "1.5.0");

            Assert.NotNull(pkg);
            Assert.Equal("1.5.0", pkg!.Version);
        }
        finally
        {
            repo.Dispose();
        }
    }

    [Fact]
    public async Task GetPackageAsync_HttpError_ReturnsNull()
    {
        var (repo, handler) = Create();
        try
        {
            handler.SetException(
                "https://market.example.com/api/v1/packages/foo/1.0.0/metadata",
                new HttpRequestException("nope"));

            var pkg = await repo.GetPackageAsync("foo", version: "1.0.0");

            Assert.Null(pkg);
        }
        finally
        {
            repo.Dispose();
        }
    }

    [Fact]
    public async Task DownloadAsync_WritesPayloadAndReturnsPath()
    {
        var targetDir = Path.Combine(Path.GetTempPath(), $"sw-market-dl-{Guid.NewGuid():N}");
        var (repo, handler) = Create();
        try
        {
            var payload = new byte[] { 0x50, 0x4b, 0xAA, 0xBB };
            handler.SetBinaryResponse(
                "https://market.example.com/api/v1/packages/foo/1.0.0/download", payload);

            var path = await repo.DownloadAsync("foo", "1.0.0", targetDir);

            Assert.True(File.Exists(path));
            Assert.EndsWith("foo-1.0.0.swpkg", path);
            Assert.Equal(payload, await File.ReadAllBytesAsync(path));
        }
        finally
        {
            repo.Dispose();
            if (Directory.Exists(targetDir)) Directory.Delete(targetDir, true);
        }
    }

    [Fact]
    public async Task DownloadAsync_404_Throws()
    {
        var targetDir = Path.Combine(Path.GetTempPath(), $"sw-market-dl-404-{Guid.NewGuid():N}");
        var (repo, handler) = Create();
        try
        {
            handler.SetResponse(
                "https://market.example.com/api/v1/packages/foo/1.0.0/download",
                new HttpResponseMessage(HttpStatusCode.NotFound));

            await Assert.ThrowsAsync<HttpRequestException>(() => repo.DownloadAsync("foo", "1.0.0", targetDir));
        }
        finally
        {
            repo.Dispose();
            if (Directory.Exists(targetDir)) Directory.Delete(targetDir, true);
        }
    }

    // --- HttpMessageHandler stub (mirror of HttpConnectorRepositoryTests') ---

    private sealed class StubHandler : HttpMessageHandler
    {
        private static readonly JsonSerializerOptions JsonOpts = new() { PropertyNameCaseInsensitive = true };

        private readonly Dictionary<string, Func<HttpResponseMessage>> _responses = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, Exception> _exceptions = new(StringComparer.OrdinalIgnoreCase);

        public void SetResponse(string url, HttpResponseMessage response)
            => _responses[url] = () => response;

        public void SetJsonResponse<T>(string url, T body)
        {
            var json = JsonSerializer.Serialize(body, JsonOpts);
            _responses[url] = () => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json"),
            };
        }

        public void SetBinaryResponse(string url, byte[] body)
        {
            _responses[url] = () => new HttpResponseMessage(HttpStatusCode.OK)
            {
                Content = new ByteArrayContent(body),
            };
        }

        public void SetException(string url, Exception ex)
            => _exceptions[url] = ex;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var url = request.RequestUri!.ToString();
            if (_exceptions.TryGetValue(url, out var ex))
                throw ex;
            if (_responses.TryGetValue(url, out var factory))
                return Task.FromResult(factory());
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound)
            {
                Content = new StringContent($"no stub for {url}"),
            });
        }
    }
}
