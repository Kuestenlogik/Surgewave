using Kuestenlogik.Surgewave.Streams;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class NamedTopologyTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public NamedTopologyTests(ITestOutputHelper output)
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
    public void NamedTopologyBuilder_CreatesTopologyWithName()
    {
        var ntb = new NamedTopologyBuilder("orders-processing");
        ntb.Stream<string, string>("orders").ForEach((k, v) => { });
        var nt = ntb.Build();

        Assert.Equal("orders-processing", nt.Name);
        Assert.NotNull(nt.Topology);
        Assert.Single(nt.SourceTopics);
        Assert.Equal("orders", nt.SourceTopics[0]);
    }

    [Fact]
    public void NamedTopology_MultipleTopics_AllDiscovered()
    {
        var ntb = new NamedTopologyBuilder("multi");
        ntb.Stream<string, string>("topic-a").ForEach((k, v) => { });
        ntb.Stream<string, string>("topic-b").ForEach((k, v) => { });
        var nt = ntb.Build();

        Assert.Equal(2, nt.SourceTopics.Count);
        Assert.Contains("topic-a", nt.SourceTopics);
        Assert.Contains("topic-b", nt.SourceTopics);
    }

    [Fact]
    public void NamedTopology_IndependentProcessing()
    {
        var results1 = new List<string>();
        var results2 = new List<string>();

        // Two independent named topologies
        var ntb1 = new NamedTopologyBuilder("topology-1");
        ntb1.Stream<string, string>("input-1")
            .MapValues<string>(v => v.ToUpperInvariant())
            .ForEach((k, v) => results1.Add(v));

        var ntb2 = new NamedTopologyBuilder("topology-2");
        ntb2.Stream<string, string>("input-2")
            .MapValues<string>(v => v.ToLowerInvariant())
            .ForEach((k, v) => results2.Add(v));

        var nt1 = ntb1.Build();
        var nt2 = ntb2.Build();

        // Each has its own topology
        Assert.NotEqual(nt1.Name, nt2.Name);
        Assert.NotSame(nt1.Topology, nt2.Topology);

        // Process through topology 1
        var config1 = new StreamsConfig { ApplicationId = "topo1", BootstrapServers = "localhost:9092" };
        var app1 = new StreamsApplication(config1, nt1.Topology, _loggerFactory);
        app1.ProcessRecord("input-1", "k", "hello");

        Assert.Single(results1);
        Assert.Equal("HELLO", results1[0]);
        Assert.Empty(results2);

        // Process through topology 2
        var config2 = new StreamsConfig { ApplicationId = "topo2", BootstrapServers = "localhost:9092" };
        var app2 = new StreamsApplication(config2, nt2.Topology, _loggerFactory);
        app2.ProcessRecord("input-2", "k", "WORLD");

        Assert.Single(results2);
        Assert.Equal("world", results2[0]);
    }

    [Fact]
    public void NamedTopology_WithStateStore()
    {
        var ntb = new NamedTopologyBuilder("stateful");
        ntb.AddStateStore(Stores.KeyValueStore<string, int>("my-store"));
        ntb.Stream<string, int>("input").ForEach((k, v) => { });
        var nt = ntb.Build();

        Assert.Equal("stateful", nt.Name);
        Assert.NotEmpty(nt.Topology.StateStoreSuppliers);
    }

    [Fact]
    public void NamedTopology_WithTable()
    {
        var results = new List<KeyValuePair<string, string>>();
        var ntb = new NamedTopologyBuilder("table-topo");
        ntb.Table<string, string>("users")
            .ToStream()
            .ForEach((k, v) => results.Add(new(k, v)));

        var nt = ntb.Build();

        var config = new StreamsConfig { ApplicationId = "table-test", BootstrapServers = "localhost:9092" };
        var app = new StreamsApplication(config, nt.Topology, _loggerFactory);

        app.ProcessRecord("users", "u1", "Alice");

        Assert.Single(results);
        Assert.Equal("Alice", results[0].Value);
    }

    [Fact]
    public void NamedTopology_DescribeReturnsTopologyDescription()
    {
        var ntb = new NamedTopologyBuilder("analytics");
        ntb.Stream<string, int>("events")
            .Filter((k, v) => v > 0)
            .ForEach((k, v) => { });

        var nt = ntb.Build();
        var description = nt.Topology.Describe();

        Assert.Contains("Topologies:", description);
        Assert.Contains("Sub-topology: 0", description);
    }
}
