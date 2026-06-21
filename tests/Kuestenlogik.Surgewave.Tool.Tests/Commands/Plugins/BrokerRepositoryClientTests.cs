using System.Net;
using System.Net.Http.Json;
using Kuestenlogik.Surgewave.Cli.Commands.Plugins;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Xunit;

namespace Kuestenlogik.Surgewave.Tool.Tests.Commands.Plugins;

/// <summary>
/// Verifies the HTTP shape <see cref="BrokerRepositoryClient"/> produces
/// against the broker's /api/plugins/repositories surface. Uses a stub
/// HttpMessageHandler so the tests don't need a running broker — guards
/// against URL / verb / payload regressions if the contract on the broker
/// side ever changes.
/// </summary>
public sealed class BrokerRepositoryClientTests
{
    private static BrokerRepositoryClient ClientWith(Func<HttpRequestMessage, HttpResponseMessage> respond, out CaptureHandler capture)
    {
        capture = new CaptureHandler(respond);
        var http = new HttpClient(capture) { BaseAddress = new Uri("https://broker.test/") };
        return new BrokerRepositoryClient(http);
    }

    [Fact]
    public async Task ListAsync_GET_AtCollectionPath_ReturnsRepositories()
    {
        var payload = new
        {
            configPath = "/data/surgewave-repositories.json",
            repositories = new object[]
            {
                new { name = "nuget.org", type = "NuGet", source = "https://api.nuget.org/v3/index.json", enabled = true, packagePrefix = (string?)null },
                new { name = "internal", type = "Http", source = "https://feed.example/", enabled = false, packagePrefix = (string?)"Acme." },
            },
        };
        using var client = ClientWith(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = JsonContent.Create(payload),
        }, out var cap);

        var list = await client.ListAsync();

        Assert.Equal(HttpMethod.Get, cap.LastRequest!.Method);
        Assert.Equal("/api/plugins/repositories/", cap.LastRequest.RequestUri!.AbsolutePath);
        Assert.Equal(2, list.Count);
        Assert.Equal(RepositoryType.Http, list[1].Type);
        Assert.False(list[1].Enabled);
        Assert.Equal("Acme.", list[1].PackagePrefix);
    }

    [Fact]
    public async Task AddAsync_POST_AtCollectionPath_EchoesEntry()
    {
        var entry = new RepositoryEntry { Name = "acme", Type = RepositoryType.NuGet, Source = "https://acme/v3" };
        using var client = ClientWith(_ => new HttpResponseMessage(HttpStatusCode.Created)
        {
            Content = JsonContent.Create(entry),
        }, out var cap);

        var saved = await client.AddAsync(entry);

        Assert.Equal(HttpMethod.Post, cap.LastRequest!.Method);
        Assert.Equal("/api/plugins/repositories/", cap.LastRequest.RequestUri!.AbsolutePath);
        Assert.Equal("acme", saved.Name);
    }

    [Fact]
    public async Task RemoveAsync_DELETE_AtItemPath_Succeeds()
    {
        using var client = ClientWith(_ => new HttpResponseMessage(HttpStatusCode.OK), out var cap);
        await client.RemoveAsync("acme");
        Assert.Equal(HttpMethod.Delete, cap.LastRequest!.Method);
        Assert.EndsWith("/api/plugins/repositories/acme", cap.LastRequest.RequestUri!.AbsolutePath);
    }

    [Fact]
    public async Task RemoveAsync_NotFound_ThrowsRepositoryNotFound()
    {
        using var client = ClientWith(_ => new HttpResponseMessage(HttpStatusCode.NotFound), out _);
        await Assert.ThrowsAsync<RepositoryNotFoundException>(() => client.RemoveAsync("ghost"));
    }

    [Fact]
    public async Task AddAsync_BrokerError_SurfacesBodyInException()
    {
        var entry = new RepositoryEntry { Name = "dup", Type = RepositoryType.NuGet, Source = "https://x.example/" };
        using var client = ClientWith(_ => new HttpResponseMessage(HttpStatusCode.Conflict)
        {
            Content = new StringContent("{\"error\":\"already exists\"}"),
        }, out _);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => client.AddAsync(entry));
        Assert.Contains("already exists", ex.Message);
    }

    [Fact]
    public async Task ListAsync_ConnectionRefused_ThrowsBrokerUnreachable()
    {
        using var client = ClientWith(_ => throw new HttpRequestException("connection refused"), out _);
        await Assert.ThrowsAsync<BrokerUnreachableException>(() => client.ListAsync());
    }

    private sealed class CaptureHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, HttpResponseMessage> _respond;
        public HttpRequestMessage? LastRequest { get; private set; }

        public CaptureHandler(Func<HttpRequestMessage, HttpResponseMessage> respond) => _respond = respond;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            LastRequest = request;
            return Task.FromResult(_respond(request));
        }
    }
}
