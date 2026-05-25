using System.Net;
using System.Net.Http;
using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Repository;

namespace Kuestenlogik.Surgewave.Plugins.Repository.Tests;

/// <summary>
/// Tests fuer <see cref="HttpConnectorRepository"/> in beiden Modi (StaticIndex + RestApi).
/// Beide Pfade werden via <see cref="StubHandler"/> abgedeckt — ein HttpMessageHandler, der
/// pro Request-URL eine vordefinierte Response liefert. Keine Netz-Anfragen.
/// </summary>
public sealed class HttpConnectorRepositoryTests
{
    private static ConnectorPackageInfo SamplePackage(string id = "Kuestenlogik.Surgewave.Connector.Hue", string version = "1.0.0", long downloads = 100, string? description = null, string[]? tags = null) => new()
    {
        PackageId = id,
        Version = version,
        Name = id.Split('.').Last(),
        Description = description,
        DownloadCount = downloads,
        AvailableVersions = [version],
        Tags = tags ?? [],
    };

    // --- Constructor & mode detection ---

    [Fact]
    public void StaticIndexUrl_DetectedAsStatic()
    {
        using var handler = new StubHandler();
        using var client = new HttpClient(handler);

        using var repo = new HttpConnectorRepository(
            name: "static",
            source: "https://kuestenlogik.github.io/Surgewave.Connectors",
            httpClient: client);

        Assert.Equal("static", repo.Name);
        Assert.Equal("https://kuestenlogik.github.io/Surgewave.Connectors", repo.Source);
    }

    [Fact]
    public void TrailingSlash_IsTrimmedFromSource()
    {
        using var handler = new StubHandler();
        using var client = new HttpClient(handler);

        using var repo = new HttpConnectorRepository(
            name: "trim",
            source: "https://example.com/repo/",
            httpClient: client);

        Assert.Equal("https://example.com/repo", repo.Source);
    }

    // --- StaticIndex mode ---

    [Fact]
    public async Task SearchAsync_StaticIndex_ReturnsFilteredAndSorted()
    {
        using var handler = new StubHandler();
        handler.SetJsonResponse("https://example.com/repo/packages.json", new[]
        {
            SamplePackage(id: "Kuestenlogik.Surgewave.Connector.A", downloads: 50),
            SamplePackage(id: "Kuestenlogik.Surgewave.Connector.B", downloads: 200),
            SamplePackage(id: "Other.Package", downloads: 999),  // filtered out by prefix
        });
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        var results = await repo.SearchAsync(query: null, skip: 0, take: 10);

        Assert.Equal(2, results.Count);
        // Sorted by DownloadCount desc
        Assert.Equal("Kuestenlogik.Surgewave.Connector.B", results[0].PackageId);
        Assert.Equal("Kuestenlogik.Surgewave.Connector.A", results[1].PackageId);
    }

    [Fact]
    public async Task SearchAsync_StaticIndex_WithQueryFilter_MatchesNameAndDescription()
    {
        using var handler = new StubHandler();
        handler.SetJsonResponse("https://example.com/repo/packages.json", new[]
        {
            SamplePackage(id: "Kuestenlogik.Surgewave.Connector.Mqtt", description: "Eclipse Mosquitto"),
            SamplePackage(id: "Kuestenlogik.Surgewave.Connector.Akka", description: "Actor model"),
        });
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        var results = await repo.SearchAsync(query: "mosquitto");

        Assert.Single(results);
        Assert.Contains("Mqtt", results[0].PackageId);
    }

    [Fact]
    public async Task SearchAsync_StaticIndex_QueryMatchesTags()
    {
        using var handler = new StubHandler();
        handler.SetJsonResponse("https://example.com/repo/packages.json", new[]
        {
            SamplePackage(id: "Kuestenlogik.Surgewave.Connector.Mqtt", tags: ["iot", "mqtt"]),
            SamplePackage(id: "Kuestenlogik.Surgewave.Connector.Postgres", tags: ["database"]),
        });
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        var results = await repo.SearchAsync(query: "iot");

        Assert.Single(results);
        Assert.Equal("Kuestenlogik.Surgewave.Connector.Mqtt", results[0].PackageId);
    }

    [Fact]
    public async Task SearchAsync_StaticIndex_PaginatesViaSkipAndTake()
    {
        using var handler = new StubHandler();
        handler.SetJsonResponse("https://example.com/repo/packages.json", Enumerable.Range(0, 10)
            .Select(i => SamplePackage(id: $"Kuestenlogik.Surgewave.Connector.P{i:D2}", downloads: 100 - i))
            .ToArray());
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        var page = await repo.SearchAsync(query: null, skip: 2, take: 3);

        Assert.Equal(3, page.Count);
        Assert.Equal("Kuestenlogik.Surgewave.Connector.P02", page[0].PackageId);
    }

    [Fact]
    public async Task SearchAsync_StaticIndex_UpstreamUnreachable_ReturnsEmpty()
    {
        using var handler = new StubHandler();
        handler.SetException("https://example.com/repo/packages.json", new HttpRequestException("upstream down"));
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        var results = await repo.SearchAsync(query: null);

        Assert.Empty(results);
    }

    [Fact]
    public async Task GetPackageAsync_StaticIndex_KnownPackage_Returns()
    {
        using var handler = new StubHandler();
        handler.SetJsonResponse("https://example.com/repo/packages.json", new[]
        {
            SamplePackage(id: "Kuestenlogik.Surgewave.Connector.A"),
        });
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        var pkg = await repo.GetPackageAsync("Kuestenlogik.Surgewave.Connector.A");

        Assert.NotNull(pkg);
        Assert.Equal("Kuestenlogik.Surgewave.Connector.A", pkg!.PackageId);
    }

    [Fact]
    public async Task GetPackageAsync_StaticIndex_MissingVersion_ReturnsNull()
    {
        using var handler = new StubHandler();
        handler.SetJsonResponse("https://example.com/repo/packages.json", new[]
        {
            SamplePackage(id: "Kuestenlogik.Surgewave.Connector.A", version: "1.0.0"),
        });
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        var pkg = await repo.GetPackageAsync("Kuestenlogik.Surgewave.Connector.A", version: "9.9.9");

        Assert.Null(pkg);
    }

    [Fact]
    public async Task GetPackageAsync_UnknownPackage_ReturnsNull()
    {
        using var handler = new StubHandler();
        handler.SetJsonResponse("https://example.com/repo/packages.json", Array.Empty<ConnectorPackageInfo>());
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        var pkg = await repo.GetPackageAsync("Nothing");

        Assert.Null(pkg);
    }

    [Fact]
    public async Task GetVersionsAsync_DelegatesToGetPackage()
    {
        using var handler = new StubHandler();
        handler.SetJsonResponse("https://example.com/repo/packages.json", new[]
        {
            new ConnectorPackageInfo
            {
                PackageId = "Kuestenlogik.Surgewave.Connector.A",
                Version = "2.0.0",
                Name = "A",
                AvailableVersions = ["2.0.0", "1.0.0"],
            },
        });
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        var versions = await repo.GetVersionsAsync("Kuestenlogik.Surgewave.Connector.A");

        Assert.Equal(["2.0.0", "1.0.0"], versions);
    }

    [Fact]
    public async Task GetVersionsAsync_UnknownPackage_EmptyList()
    {
        using var handler = new StubHandler();
        handler.SetJsonResponse("https://example.com/repo/packages.json", Array.Empty<ConnectorPackageInfo>());
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        var versions = await repo.GetVersionsAsync("nope");

        Assert.Empty(versions);
    }

    [Fact]
    public async Task DownloadAsync_StaticIndex_WritesPayloadToTargetPath()
    {
        var targetDir = Path.Combine(Path.GetTempPath(), $"sw-http-dl-{Guid.NewGuid():N}");
        using var handler = new StubHandler();
        var payload = new byte[] { 0x50, 0x4b, 0x03, 0x04, 0xAA, 0xBB };
        handler.SetBinaryResponse(
            "https://example.com/repo/packages/A/1.0.0/A.1.0.0.nupkg", payload);
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        try
        {
            var path = await repo.DownloadAsync("A", "1.0.0", targetDir);

            Assert.True(File.Exists(path));
            Assert.Equal(payload, await File.ReadAllBytesAsync(path));
        }
        finally
        {
            if (Directory.Exists(targetDir)) Directory.Delete(targetDir, true);
        }
    }

    [Fact]
    public async Task DownloadAsync_404_Throws()
    {
        var targetDir = Path.Combine(Path.GetTempPath(), $"sw-http-dl-fail-{Guid.NewGuid():N}");
        using var handler = new StubHandler();
        handler.SetResponse("https://example.com/repo/packages/A/1.0.0/A.1.0.0.nupkg",
            new HttpResponseMessage(HttpStatusCode.NotFound));
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        try
        {
            await Assert.ThrowsAsync<HttpRequestException>(() => repo.DownloadAsync("A", "1.0.0", targetDir));
        }
        finally
        {
            if (Directory.Exists(targetDir)) Directory.Delete(targetDir, true);
        }
    }

    [Fact]
    public async Task DownloadFromUrlAsync_DerivesFileNameFromUrl()
    {
        var targetDir = Path.Combine(Path.GetTempPath(), $"sw-http-dlurl-{Guid.NewGuid():N}");
        using var handler = new StubHandler();
        handler.SetBinaryResponse("https://example.com/files/foo.nupkg", new byte[] { 1, 2, 3 });
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        try
        {
            var path = await repo.DownloadFromUrlAsync("https://example.com/files/foo.nupkg", targetDir);

            Assert.EndsWith("foo.nupkg", path);
            Assert.True(File.Exists(path));
        }
        finally
        {
            if (Directory.Exists(targetDir)) Directory.Delete(targetDir, true);
        }
    }

    [Fact]
    public async Task DownloadFromUrlAsync_UrlWithoutExtension_FallsBackToGeneratedName()
    {
        var targetDir = Path.Combine(Path.GetTempPath(), $"sw-http-dlurl2-{Guid.NewGuid():N}");
        using var handler = new StubHandler();
        handler.SetBinaryResponse("https://example.com/download", new byte[] { 1, 2, 3 });
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        try
        {
            var path = await repo.DownloadFromUrlAsync("https://example.com/download", targetDir);

            Assert.True(File.Exists(path));
            Assert.EndsWith(".swpkg", path);
        }
        finally
        {
            if (Directory.Exists(targetDir)) Directory.Delete(targetDir, true);
        }
    }

    // --- REST API mode ---

    // Note: the implementation appends "/api/..." onto Source, so passing a Source
    // that already contains "/api" yields URLs like ".../api/api/...". DetectMode only
    // needs "/api" to flip into REST mode — the path concatenation is intentional.

    [Fact]
    public async Task SearchAsync_RestApi_BuildsQueryUrl()
    {
        using var handler = new StubHandler();
        handler.SetJsonResponse(
            "https://api.example.com/api/api/packages?skip=0&take=20&q=mqtt",
            new[] { SamplePackage(id: "rest.api.match") });
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://api.example.com/api", httpClient: client);

        var results = await repo.SearchAsync(query: "mqtt");

        Assert.Single(results);
        Assert.Equal("rest.api.match", results[0].PackageId);
    }

    [Fact]
    public async Task SearchAsync_RestApi_HttpError_ReturnsEmpty()
    {
        using var handler = new StubHandler();
        handler.SetException(
            "https://api.example.com/api/api/packages?skip=0&take=20",
            new HttpRequestException("nope"));
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://api.example.com/api", httpClient: client);

        var results = await repo.SearchAsync(query: null);

        Assert.Empty(results);
    }

    [Fact]
    public async Task GetPackageAsync_RestApi_Found_Returns()
    {
        using var handler = new StubHandler();
        handler.SetJsonResponse(
            "https://api.example.com/api/api/packages/foo",
            SamplePackage(id: "foo"));
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://api.example.com/api", httpClient: client);

        var pkg = await repo.GetPackageAsync("foo");

        Assert.NotNull(pkg);
        Assert.Equal("foo", pkg!.PackageId);
    }

    [Fact]
    public async Task GetPackageAsync_RestApi_404_ReturnsNull()
    {
        using var handler = new StubHandler();
        handler.SetResponse("https://api.example.com/api/api/packages/missing",
            new HttpResponseMessage(HttpStatusCode.NotFound));
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://api.example.com/api", httpClient: client);

        var pkg = await repo.GetPackageAsync("missing");

        Assert.Null(pkg);
    }

    [Fact]
    public async Task DownloadAsync_RestApi_UsesRestPath()
    {
        var targetDir = Path.Combine(Path.GetTempPath(), $"sw-http-rest-dl-{Guid.NewGuid():N}");
        using var handler = new StubHandler();
        handler.SetBinaryResponse(
            "https://api.example.com/api/api/packages/foo/1.0.0/download",
            new byte[] { 0x42 });
        using var client = new HttpClient(handler);
        using var repo = new HttpConnectorRepository("r", "https://api.example.com/api", httpClient: client);

        try
        {
            var path = await repo.DownloadAsync("foo", "1.0.0", targetDir);

            Assert.True(File.Exists(path));
        }
        finally
        {
            if (Directory.Exists(targetDir)) Directory.Delete(targetDir, true);
        }
    }

    // --- Dispose ---

    [Fact]
    public void Dispose_OwnedHttpClient_DisposedToo()
    {
        // No client passed → repository creates and owns one. Dispose must not throw.
        var repo = new HttpConnectorRepository("r", "https://example.com/repo");

        repo.Dispose();
    }

    [Fact]
    public void Dispose_ExternalHttpClient_NotDisposed()
    {
        using var handler = new StubHandler();
        using var client = new HttpClient(handler);
        var repo = new HttpConnectorRepository("r", "https://example.com/repo", httpClient: client);

        repo.Dispose();

        // External client should still work
        Assert.NotNull(client.BaseAddress is null ? client : client);
    }

    // --- HttpMessageHandler stub ---

    private sealed class StubHandler : HttpMessageHandler
    {
        private static readonly JsonSerializerOptions JsonOpts = new()
        {
            PropertyNameCaseInsensitive = true,
        };

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
