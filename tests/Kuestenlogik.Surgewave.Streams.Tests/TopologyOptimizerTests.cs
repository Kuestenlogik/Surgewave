using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Processors;
using Kuestenlogik.Surgewave.Streams.Runtime;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class TopologyOptimizerTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public TopologyOptimizerTests(ITestOutputHelper output)
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
    public void Optimizer_MergesSourceNodes_SameTopic()
    {
        // Two streams reading from the same topic
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("shared-topic").ForEach((k, v) => { });
        builder.Stream<string, string>("shared-topic").ForEach((k, v) => { });

        var topology = builder.Build();
        var optimizer = new TopologyOptimizer();

        // Before optimization: 2 source nodes
        Assert.Equal(2, topology.Sources.Count);

        var optimized = optimizer.Optimize(topology);

        // After optimization: 1 source node with both children
        Assert.Single(optimized.Sources);
        Assert.True(optimizer.NodesRemoved > 0);
        Assert.Contains(optimizer.Optimizations, o => o.Type == OptimizationType.SourceMerge);
    }

    [Fact]
    public void Optimizer_DifferentTopics_NoMerge()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("topic-a").ForEach((k, v) => { });
        builder.Stream<string, string>("topic-b").ForEach((k, v) => { });

        var topology = builder.Build();
        var optimizer = new TopologyOptimizer();

        var optimized = optimizer.Optimize(topology);

        // No merge for different topics
        Assert.Equal(2, optimized.Sources.Count);
        Assert.Equal(0, optimizer.NodesRemoved);
    }

    [Fact]
    public void Optimizer_PreservesStateStoreSuppliers()
    {
        var builder = new StreamsBuilder();
        builder.AddStateStore(Stores.KeyValueStore<string, string>("my-store"));
        builder.Stream<string, string>("input").ForEach((k, v) => { });

        var topology = builder.Build();
        var optimizer = new TopologyOptimizer();

        var optimized = optimizer.Optimize(topology);

        Assert.Equal(topology.StateStoreSuppliers.Count, optimized.StateStoreSuppliers.Count);
    }

    [Fact]
    public void Optimizer_ReportsOptimizations()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("shared").ForEach((k, v) => { });
        builder.Stream<string, string>("shared").ForEach((k, v) => { });
        builder.Stream<string, string>("shared").ForEach((k, v) => { });

        var topology = builder.Build();
        var optimizer = new TopologyOptimizer();

        optimizer.Optimize(topology);

        Assert.NotEmpty(optimizer.Optimizations);
        var mergeOpt = optimizer.Optimizations.First(o => o.Type == OptimizationType.SourceMerge);
        Assert.Equal(2, mergeOpt.NodesRemoved); // 3 sources merged into 1 = 2 removed
    }

    [Fact]
    public void Optimizer_SingleSource_NoOptimization()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("single").ForEach((k, v) => { });

        var topology = builder.Build();
        var optimizer = new TopologyOptimizer();

        var optimized = optimizer.Optimize(topology);

        Assert.Single(optimized.Sources);
        Assert.Equal(0, optimizer.NodesRemoved);
    }

    [Fact]
    public void Optimizer_EmptyTopology_NoError()
    {
        var builder = new StreamsBuilder();
        var topology = builder.Build();
        var optimizer = new TopologyOptimizer();

        var optimized = optimizer.Optimize(topology);

        Assert.Empty(optimized.Sources);
        Assert.Equal(0, optimizer.NodesRemoved);
    }
}
