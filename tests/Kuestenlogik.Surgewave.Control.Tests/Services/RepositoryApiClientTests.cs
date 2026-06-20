using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Kuestenlogik.Surgewave.Control.Services;
using Kuestenlogik.Surgewave.Plugins.Repository;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Control.Tests.Services;

/// <summary>
/// Verifies the HTTP shape <see cref="RepositoryApiClient"/> produces — the
/// broker is too heavy to spin up for a unit test, so a stub HttpMessageHandler
/// captures requests and returns canned responses. This guards against URL /
/// verb / payload regressions when the broker REST contract changes.
/// </summary>
public sealed class RepositoryApiClientTests
{
    private static RepositoryApiClient ClientWith(Func<HttpRequestMessage, HttpResponseMessage> respond, out CaptureHandler capture)
    {
        capture = new CaptureHandler(respond);
        var http = new HttpClient(capture) { BaseAddress = new Uri("https://broker.test/") };
        return new RepositoryApiClient(http, NullLogger<RepositoryApiClient>.Instance);
    }

    [Fact]
    public async Task ListAsync_ParsesPayload_AndCallsCorrectPath()
    {
        var payload = new
        {
            configPath = "/data/surgewave-repositories.json",
            repositories = new[]
            {
                new { name = "nuget.org", type = "NuGet", source = "https://api.nuget.org/v3/index.json", enabled = true },
            },
        };
        var client = ClientWith(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = JsonContent.Create(payload),
        }, out var cap);

        var list = await client.ListAsync();

        Assert.Equal(HttpMethod.Get, cap.LastRequest!.Method);
        Assert.Equal("/api/plugins/repositories/", cap.LastRequest.RequestUri!.AbsolutePath);
        Assert.Single(list);
        Assert.Equal("nuget.org", list[0].Name);
        Assert.Equal(RepositoryType.NuGet, list[0].Type);
    }

    [Fact]
    public async Task AddAsync_PostsEntryToCollection_AndReturnsEcho()
    {
        var entry = new RepositoryEntry { Name = "acme", Type = RepositoryType.Http, Source = "https://acme.example/feed" };
        var client = ClientWith(_ => new HttpResponseMessage(HttpStatusCode.Created)
        {
            Content = JsonContent.Create(entry),
        }, out var cap);

        var saved = await client.AddAsync(entry);

        Assert.Equal(HttpMethod.Post, cap.LastRequest!.Method);
        Assert.Equal("/api/plugins/repositories/", cap.LastRequest.RequestUri!.AbsolutePath);
        Assert.Equal("acme", saved.Name);
    }

    [Fact]
    public async Task AddAsync_OnServerError_ThrowsWithBody()
    {
        var entry = new RepositoryEntry { Name = "dup", Type = RepositoryType.NuGet, Source = "https://x.example/" };
        var client = ClientWith(_ => new HttpResponseMessage(HttpStatusCode.Conflict)
        {
            Content = new StringContent("{\"error\":\"already exists\"}"),
        }, out _);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => client.AddAsync(entry));
        Assert.Contains("already exists", ex.Message);
    }

    [Fact]
    public async Task UpdateAsync_TargetsNameInPath_WithPutVerb()
    {
        var entry = new RepositoryEntry { Name = "acme", Type = RepositoryType.NuGet, Source = "https://acme/v3" };
        var client = ClientWith(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = JsonContent.Create(entry),
        }, out var cap);

        await client.UpdateAsync("acme", entry);

        Assert.Equal(HttpMethod.Put, cap.LastRequest!.Method);
        Assert.EndsWith("/api/plugins/repositories/acme", cap.LastRequest.RequestUri!.AbsolutePath);
    }

    [Fact]
    public async Task DeleteAsync_UsesEscapedNameInPath()
    {
        var client = ClientWith(_ => new HttpResponseMessage(HttpStatusCode.OK), out var cap);

        await client.DeleteAsync("name with space");

        Assert.Equal(HttpMethod.Delete, cap.LastRequest!.Method);
        Assert.EndsWith("name%20with%20space", cap.LastRequest.RequestUri!.AbsolutePath);
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
