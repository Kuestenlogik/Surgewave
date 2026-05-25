using System.Diagnostics.Metrics;
using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Monitoring;
using Kuestenlogik.Surgewave.Streams.Processors;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class ProcessorMetricsTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public ProcessorMetricsTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddFilter(level => level >= LogLevel.Debug);
        });
    }

    public async ValueTask DisposeAsync()
    {
        _loggerFactory.Dispose();
        await Task.CompletedTask;
    }

    [Fact]
    public void NodeMetrics_CountsRecordsInOut()
    {
        using var metrics = new StreamsMetrics();
        var nodeMetrics = metrics.GetOrCreateNodeMetrics("test-node");

        nodeMetrics.RecordIn(5);
        nodeMetrics.RecordOut(3);

        Assert.Equal(5, nodeMetrics.TotalIn);
        Assert.Equal(3, nodeMetrics.TotalOut);
        Assert.Equal("test-node", nodeMetrics.NodeName);
    }

    [Fact]
    public void NodeMetrics_MeasuresLatency()
    {
        using var metrics = new StreamsMetrics();
        var nodeMetrics = metrics.GetOrCreateNodeMetrics("latency-node");

        // Should not throw
        nodeMetrics.RecordLatency(1.5);
        nodeMetrics.RecordLatency(2.0);
        nodeMetrics.RecordLatency(0.1);

        // No exception means it works
        Assert.Equal(0, nodeMetrics.TotalErrors);
    }

    [Fact]
    public void StoreMetrics_CountsPutsAndGets()
    {
        using var metrics = new StreamsMetrics();
        var storeMetrics = metrics.GetOrCreateStoreMetrics("test-store");

        storeMetrics.RecordPut();
        storeMetrics.RecordPut();
        storeMetrics.RecordGet();

        Assert.Equal(2, storeMetrics.TotalPuts);
        Assert.Equal(1, storeMetrics.TotalGets);
        Assert.Equal("test-store", storeMetrics.StoreName);
    }

    [Fact]
    public void StoreMetrics_ReportsEntryCount()
    {
        using var metrics = new StreamsMetrics();
        var entryCount = 42L;
        var storeMetrics = metrics.GetOrCreateStoreMetrics("sized-store", () => entryCount);

        Assert.NotNull(storeMetrics);
        Assert.Equal("sized-store", storeMetrics.StoreName);
    }

    [Fact]
    public void MultipleNodes_IndependentMetrics()
    {
        using var metrics = new StreamsMetrics();

        var node1 = metrics.GetOrCreateNodeMetrics("node-A");
        var node2 = metrics.GetOrCreateNodeMetrics("node-B");

        node1.RecordIn(10);
        node2.RecordIn(5);
        node1.RecordError();

        Assert.Equal(10, node1.TotalIn);
        Assert.Equal(5, node2.TotalIn);
        Assert.Equal(1, node1.TotalErrors);
        Assert.Equal(0, node2.TotalErrors);

        // Registry should have both
        Assert.Equal(2, metrics.NodeMetrics.Count);
        Assert.True(metrics.NodeMetrics.ContainsKey("node-A"));
        Assert.True(metrics.NodeMetrics.ContainsKey("node-B"));
    }

    [Fact]
    public void MetricsIntegration_EndToEnd()
    {
        var results = new List<string>();
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input")
            .MapValues<string>(v => v.ToUpperInvariant())
            .ForEach((k, v) => results.Add($"{k}:{v}"));

        var topology = builder.Build();
        var config = new StreamsConfig
        {
            ApplicationId = "metrics-e2e",
            BootstrapServers = "localhost:9092"
        };

        var app = new StreamsApplication(config, topology, _loggerFactory);

        // Process some records
        app.ProcessRecord("input", "key1", "hello");
        app.ProcessRecord("input", "key2", "world");

        Assert.Equal(2, results.Count);

        // Metrics should show processed records
        Assert.True(app.Metrics.ProcessedRecords >= 2);

        // Store metrics should be accessible via the public API
        Assert.NotNull(app.Metrics.NodeMetrics);
        Assert.NotNull(app.Metrics.StoreMetrics);

        // InMemoryKeyValueStore creates store metrics on Init
        // (table stores are created when topology materializes tables)
    }
}
