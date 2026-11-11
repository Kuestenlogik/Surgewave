using Kuestenlogik.Surgewave.Broker.Health;
using Kuestenlogik.Surgewave.Broker.Startup;
using Kuestenlogik.Surgewave.Clustering;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// A broker that has not caught up with its metadata log answers no API request over HTTP and is not ready (#250);
/// its health probes and metrics are answered all the same, because a broker waiting for a quorum is alive.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class MetadataCatchUpGateTests : IAsyncDisposable
{
    private readonly string _logDir = Path.Combine(Path.GetTempPath(), "surgewave-catch-up-gate-" + Guid.NewGuid().ToString("N"));
    private readonly LogManager _logs;
    private readonly ClusterController _notCaughtUp;

    public MetadataCatchUpGateTests()
    {
        // A controller that was never started has applied nothing.
        var state = new ClusterState();
        var config = new ClusteringConfig { BrokerId = 0 };
        _logs = new LogManager(_logDir, new MemoryLogSegmentFactory(), persistTopicsToFile: false);
        _notCaughtUp = new ClusterController(NullLogger<ClusterController>.Instance, state,
            new ReplicaManager(NullLogger<ReplicaManager>.Instance, state, _logs, config, new TcpPeerTransport()), config);
    }

    public async ValueTask DisposeAsync()
    {
        await _notCaughtUp.DisposeAsync();
        _logs.Dispose();
        try { if (Directory.Exists(_logDir)) Directory.Delete(_logDir, recursive: true); }
        catch (IOException) { /* a file still held is not a failure */ }
    }

    [Theory]
    [InlineData("/v3/topics")]
    [InlineData("/api/sql")]
    [InlineData("/surgewave.v1.TopicService/ListTopics")]
    [InlineData("/sd-targets")]
    public async Task AnApiRequestIsRefusedWith503(string path)
    {
        var context = new DefaultHttpContext();
        context.Request.Path = path;
        var passedOn = false;

        await MetadataCatchUpGateExtensions.AnswerOrRefuseAsync(context, _notCaughtUp, _ => { passedOn = true; return Task.CompletedTask; });

        Assert.False(passedOn);
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, context.Response.StatusCode);
        Assert.Equal("1", context.Response.Headers.RetryAfter);
    }

    [Theory]
    [InlineData("/health")]
    [InlineData("/health/live")]
    [InlineData("/health/ready")]
    [InlineData("/metrics")]
    public async Task HealthAndMetricsAreAnswered(string path)
    {
        var context = new DefaultHttpContext();
        context.Request.Path = path;
        var passedOn = false;

        await MetadataCatchUpGateExtensions.AnswerOrRefuseAsync(context, _notCaughtUp, _ => { passedOn = true; return Task.CompletedTask; });

        Assert.True(passedOn);
    }

    [Fact]
    public async Task TheBrokerIsNotReady()
    {
        var result = await new MetadataCaughtUpHealthCheck(_notCaughtUp).CheckHealthAsync(
            new HealthCheckContext(), TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }
}
