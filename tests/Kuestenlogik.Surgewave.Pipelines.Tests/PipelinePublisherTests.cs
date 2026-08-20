using System.Net;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Pipelines;
using Kuestenlogik.Surgewave.Pipelines.Publishing;

namespace Kuestenlogik.Surgewave.Pipelines.Tests;

public class PipelinePublisherTests
{
    private sealed record Order(string Status, double Amount);

    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private sealed class StubHandler : HttpMessageHandler
    {
        public List<(string Method, string Path, string? Body)> Requests { get; } = [];

        public Func<HttpRequestMessage, HttpResponseMessage> Responder { get; set; } =
            _ => Json(HttpStatusCode.OK, "{}");

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            var body = request.Content is null
                ? null
                : await request.Content.ReadAsStringAsync(cancellationToken);
            Requests.Add((request.Method.Method, request.RequestUri!.AbsolutePath, body));
            return Responder(request);
        }
    }

    private static HttpResponseMessage Json(HttpStatusCode status, string payload)
    {
        return new HttpResponseMessage(status)
        {
            Content = new StringContent(payload, Encoding.UTF8, "application/json"),
        };
    }

    private static PipelinePublisher CreatePublisher(StubHandler handler)
    {
        return new PipelinePublisher(new HttpClient(handler) { BaseAddress = new Uri("https://broker.test/") });
    }

    private static string Definition(string id, string name, PipelineStatus status = PipelineStatus.Draft)
    {
        return JsonSerializer.Serialize(new PipelineDefinition
        {
            Id = id,
            Name = name,
            Nodes = [],
            Connections = [],
            Status = status,
        }, Web);
    }

    private static BuiltPipeline Sample(string? name = "sample")
    {
        var flow = Pipeline.From<Order>("orders");
        if (name is not null)
        {
            flow = flow.Named(name);
        }

        flow.Filter(o => o.Amount > 1).To("out");
        return flow.Build();
    }

    [Fact]
    public async Task Publish_CreatesPipeline()
    {
        var handler = new StubHandler
        {
            Responder = request => request.Method.Method switch
            {
                "POST" => Json(HttpStatusCode.Created, Definition("p1", "sample")),
                _ => Json(HttpStatusCode.OK, "[]"),
            },
        };

        using var publisher = CreatePublisher(handler);
        var result = await publisher.PublishAsync(Sample());

        var (method, path, body) = Assert.Single(handler.Requests);
        Assert.Equal("POST", method);
        Assert.Equal("/api/pipelines", path);

        using var doc = JsonDocument.Parse(body!);
        Assert.Equal("sample", doc.RootElement.GetProperty("name").GetString());
        Assert.Equal("filter-1", doc.RootElement.GetProperty("nodes")[0].GetProperty("id").GetString());

        Assert.Equal("p1", result.PipelineId);
        Assert.False(result.Replaced);
        Assert.False(result.Started);
    }

    [Fact]
    public async Task Publish_WithStart_StartsAfterCreate()
    {
        var handler = new StubHandler
        {
            Responder = request => request.RequestUri!.AbsolutePath switch
            {
                "/api/pipelines" => Json(HttpStatusCode.Created, Definition("p1", "sample")),
                "/api/pipelines/p1/start" => new HttpResponseMessage(HttpStatusCode.Accepted),
                _ => Json(HttpStatusCode.NotFound, "{}"),
            },
        };

        using var publisher = CreatePublisher(handler);
        var result = await publisher.PublishAsync(Sample(), new PipelinePublishOptions { Start = true });

        Assert.True(result.Started);
        Assert.Equal(new[] { "/api/pipelines", "/api/pipelines/p1/start" }, handler.Requests.Select(r => r.Path));
    }

    [Fact]
    public async Task Publish_WithSchedule_PutsSchedule()
    {
        var pipeline = Pipeline.Create("scheduled")
            .WithSchedule("0 * * * *")
            .FromTopic("in")
            .To("out")
            .Build();

        var handler = new StubHandler
        {
            Responder = request => request.RequestUri!.AbsolutePath switch
            {
                "/api/pipelines" => Json(HttpStatusCode.Created, Definition("p1", "scheduled")),
                "/api/pipelines/p1/schedule" => Json(HttpStatusCode.OK, "{}"),
                _ => Json(HttpStatusCode.NotFound, "{}"),
            },
        };

        using var publisher = CreatePublisher(handler);
        await publisher.PublishAsync(pipeline);

        Assert.Contains(handler.Requests, r => r is { Method: "PUT", Path: "/api/pipelines/p1/schedule" });
    }

    [Fact]
    public async Task ReplaceByName_StopsUpdatesAndRestartsRunningPipeline()
    {
        var handler = new StubHandler();
        handler.Responder = request => (request.Method.Method, request.RequestUri!.AbsolutePath) switch
        {
            ("GET", "/api/pipelines") => Json(HttpStatusCode.OK, $"[{Definition("p7", "sample", PipelineStatus.Running)}]"),
            ("POST", "/api/pipelines/p7/stop") => new HttpResponseMessage(HttpStatusCode.Accepted),
            ("PUT", "/api/pipelines/p7") => Json(HttpStatusCode.OK, Definition("p7", "sample")),
            ("PUT", "/api/pipelines/p7/schedule") => Json(HttpStatusCode.OK, "{}"),
            ("POST", "/api/pipelines/p7/start") => new HttpResponseMessage(HttpStatusCode.Accepted),
            _ => Json(HttpStatusCode.NotFound, "{}"),
        };

        using var publisher = CreatePublisher(handler);
        var result = await publisher.PublishAsync(Sample(), new PipelinePublishOptions { Mode = PublishMode.ReplaceByName });

        Assert.True(result.Replaced);
        Assert.True(result.WasRunning);
        Assert.True(result.Started);
        Assert.Equal(
            new[] { "/api/pipelines", "/api/pipelines/p7/stop", "/api/pipelines/p7", "/api/pipelines/p7/schedule", "/api/pipelines/p7/start" },
            handler.Requests.Select(r => r.Path));

        // a redeploy without WithSchedule must disable any schedule left on the broker
        var scheduleBody = handler.Requests.Single(r => r.Path == "/api/pipelines/p7/schedule").Body!;
        using var scheduleDoc = JsonDocument.Parse(scheduleBody);
        Assert.False(scheduleDoc.RootElement.GetProperty("enabled").GetBoolean());

        // and parameters are replaced, not merged — an empty object, never null
        var updateBody = handler.Requests.Single(r => r is { Method: "PUT", Path: "/api/pipelines/p7" }).Body!;
        using var updateDoc = JsonDocument.Parse(updateBody);
        Assert.Equal(JsonValueKind.Object, updateDoc.RootElement.GetProperty("parameters").ValueKind);
    }

    [Fact]
    public async Task FailedReplace_RestartsTheStoppedPipeline()
    {
        var startCalls = 0;
        var handler = new StubHandler();
        handler.Responder = request => (request.Method.Method, request.RequestUri!.AbsolutePath) switch
        {
            ("GET", "/api/pipelines") => Json(HttpStatusCode.OK, $"[{Definition("p7", "sample", PipelineStatus.Running)}]"),
            ("POST", "/api/pipelines/p7/stop") => new HttpResponseMessage(HttpStatusCode.Accepted),
            ("PUT", "/api/pipelines/p7") => Json(HttpStatusCode.BadRequest, "{\"detail\":\"boom\"}"),
            ("POST", "/api/pipelines/p7/start") => startCalls++ >= 0 ? new HttpResponseMessage(HttpStatusCode.Accepted) : throw new InvalidOperationException(),
            _ => Json(HttpStatusCode.NotFound, "{}"),
        };

        using var publisher = CreatePublisher(handler);
        var ex = await Assert.ThrowsAsync<PipelinePublishException>(
            () => publisher.PublishAsync(Sample(), new PipelinePublishOptions { Mode = PublishMode.ReplaceByName }));

        Assert.Contains("boom", ex.Message);
        Assert.Equal(1, startCalls); // the stopped pipeline was restarted despite the failure
    }

    [Fact]
    public async Task ReplaceByName_CreatesWhenNothingMatches()
    {
        var handler = new StubHandler();
        handler.Responder = request => (request.Method.Method, request.RequestUri!.AbsolutePath) switch
        {
            ("GET", "/api/pipelines") => Json(HttpStatusCode.OK, "[]"),
            ("POST", "/api/pipelines") => Json(HttpStatusCode.Created, Definition("p1", "sample")),
            _ => Json(HttpStatusCode.NotFound, "{}"),
        };

        using var publisher = CreatePublisher(handler);
        var result = await publisher.PublishAsync(Sample(), new PipelinePublishOptions { Mode = PublishMode.ReplaceByName });

        Assert.False(result.Replaced);
        Assert.Equal("p1", result.PipelineId);
    }

    [Fact]
    public async Task ReplaceByName_WithAmbiguousName_Throws()
    {
        var handler = new StubHandler
        {
            Responder = _ => Json(HttpStatusCode.OK, $"[{Definition("a", "sample")},{Definition("b", "sample")}]"),
        };

        using var publisher = CreatePublisher(handler);
        await Assert.ThrowsAsync<PipelinePublishException>(
            () => publisher.PublishAsync(Sample(), new PipelinePublishOptions { Mode = PublishMode.ReplaceByName }));
    }

    [Fact]
    public async Task UnnamedPipeline_WithoutOverride_Throws()
    {
        using var publisher = CreatePublisher(new StubHandler());
        await Assert.ThrowsAsync<PipelineBuildException>(() => publisher.PublishAsync(Sample(name: null)));
    }

    [Fact]
    public async Task NameOverride_WinsOverBuiltName()
    {
        var handler = new StubHandler
        {
            Responder = _ => Json(HttpStatusCode.Created, Definition("p1", "renamed")),
        };

        using var publisher = CreatePublisher(handler);
        await publisher.PublishAsync(Sample(), new PipelinePublishOptions { NameOverride = "renamed" });

        using var doc = JsonDocument.Parse(handler.Requests[0].Body!);
        Assert.Equal("renamed", doc.RootElement.GetProperty("name").GetString());
    }

    [Fact]
    public async Task ApiError_SurfacesProblemDetail()
    {
        var handler = new StubHandler
        {
            Responder = _ => Json(HttpStatusCode.BadRequest, "{\"detail\":\"Pipeline name is required\"}"),
        };

        using var publisher = CreatePublisher(handler);
        var ex = await Assert.ThrowsAsync<PipelinePublishException>(() => publisher.PublishAsync(Sample()));

        Assert.Contains("Pipeline name is required", ex.Message);
        Assert.Equal(HttpStatusCode.BadRequest, ex.StatusCode);
    }

    [Fact]
    public async Task PublishExport_KeepsNodeIds()
    {
        var export = Sample().ToExport();
        var handler = new StubHandler
        {
            Responder = _ => Json(HttpStatusCode.Created, Definition("p1", "sample")),
        };

        using var publisher = CreatePublisher(handler);
        await publisher.PublishAsync(export);

        using var doc = JsonDocument.Parse(handler.Requests[0].Body!);
        Assert.Equal("filter-1", doc.RootElement.GetProperty("nodes")[0].GetProperty("id").GetString());
    }

    [Fact]
    public async Task FindAsync_FallsBackToNameLookup()
    {
        var handler = new StubHandler();
        handler.Responder = request => (request.Method.Method, request.RequestUri!.AbsolutePath) switch
        {
            ("GET", "/api/pipelines/sample") => Json(HttpStatusCode.NotFound, "{}"),
            ("GET", "/api/pipelines") => Json(HttpStatusCode.OK, $"[{Definition("p9", "sample")}]"),
            _ => Json(HttpStatusCode.NotFound, "{}"),
        };

        using var publisher = CreatePublisher(handler);
        var found = await publisher.FindAsync("sample");

        Assert.NotNull(found);
        Assert.Equal("p9", found.Id);
    }

    [Fact]
    public async Task FindAsync_ReturnsNullWhenNothingMatches()
    {
        var handler = new StubHandler();
        handler.Responder = request => request.RequestUri!.AbsolutePath == "/api/pipelines"
            ? Json(HttpStatusCode.OK, "[]")
            : Json(HttpStatusCode.NotFound, "{}");

        using var publisher = CreatePublisher(handler);
        Assert.Null(await publisher.FindAsync("ghost"));
    }
}
