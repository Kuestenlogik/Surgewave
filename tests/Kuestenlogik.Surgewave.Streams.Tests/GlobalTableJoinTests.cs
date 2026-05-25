using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class GlobalTableJoinTests
{
    private static StreamsApplication CreateApp(Topology topology)
    {
        var config = new StreamsConfig { ApplicationId = "test-app", BootstrapServers = "localhost:9092" };
        return new StreamsApplication(config, topology, NullLoggerFactory.Instance);
    }

    [Fact]
    public async Task GlobalTableJoin_LooksUpValueInStore()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("orders");
        var globalTable = builder.GlobalTable<string, string>("countries");

        var results = new List<(string Key, string Value)>();

        stream.Join<string, string, string>(
            globalTable,
            (key, value) => key,
            (orderValue, countryName) => $"{orderValue}-{countryName}")
            .ForEach((k, v) => results.Add((k, v)));

        var topology = builder.Build();
        await using var app = CreateApp(topology);

        var store = app.GetStateStore<IKeyValueStore<string, string>>(
            globalTable.QueryableStoreName);
        Assert.NotNull(store);
        store!.Put("US", "United States");
        store.Put("DE", "Germany");

        app.ProcessRecord("orders", "US", 100);
        app.ProcessRecord("orders", "DE", 200);

        Assert.Equal(2, results.Count);
        Assert.Equal("100-United States", results[0].Value);
        Assert.Equal("200-Germany", results[1].Value);
    }

    [Fact]
    public async Task GlobalTableJoin_NoMatch_SkipsRecord()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("orders");
        var globalTable = builder.GlobalTable<string, string>("countries");

        var results = new List<string>();

        stream.Join<string, string, string>(
            globalTable,
            (key, value) => key,
            (orderValue, countryName) => $"{orderValue}-{countryName}")
            .ForEach((k, v) => results.Add(v));

        var topology = builder.Build();
        await using var app = CreateApp(topology);

        app.ProcessRecord("orders", "US", 100);

        Assert.Empty(results);
    }

    [Fact]
    public async Task GlobalTableJoin_KeySelector_MapsCorrectly()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, string>("events");
        var globalTable = builder.GlobalTable<string, string>("lookup");

        var results = new List<(string Key, string Value)>();

        stream.Join<string, string, string>(
            globalTable,
            (key, value) => value.Split(':')[0],
            (streamValue, lookupValue) => $"{streamValue}={lookupValue}")
            .ForEach((k, v) => results.Add((k, v)));

        var topology = builder.Build();
        await using var app = CreateApp(topology);

        var store = app.GetStateStore<IKeyValueStore<string, string>>(
            globalTable.QueryableStoreName);
        store!.Put("region1", "North");

        app.ProcessRecord("events", "event1", "region1:data");

        Assert.Single(results);
        Assert.Equal("region1:data=North", results[0].Value);
    }

    [Fact]
    public async Task GlobalTableJoin_StoreUpdate_ReflectedInJoin()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("input");
        var globalTable = builder.GlobalTable<string, string>("ref");

        var results = new List<string>();

        stream.Join<string, string, string>(
            globalTable,
            (key, value) => key,
            (v, refValue) => $"{v}:{refValue}")
            .ForEach((k, v) => results.Add(v));

        var topology = builder.Build();
        await using var app = CreateApp(topology);

        var store = app.GetStateStore<IKeyValueStore<string, string>>(
            globalTable.QueryableStoreName);
        store!.Put("k1", "v1");

        app.ProcessRecord("input", "k1", 10);
        Assert.Equal("10:v1", results[0]);

        store.Put("k1", "v2");
        app.ProcessRecord("input", "k1", 20);
        Assert.Equal("20:v2", results[1]);
    }

    [Fact]
    public void GlobalTableJoin_TopologyBuild_Succeeds()
    {
        var builder = new StreamsBuilder();
        var stream = builder.Stream<string, int>("input");
        var globalTable = builder.GlobalTable<string, string>("global-topic");

        stream.Join<string, string, string>(
            globalTable,
            (key, value) => key,
            (v, gv) => $"{v}-{gv}")
            .To("output");

        var topology = builder.Build();
        Assert.NotEmpty(topology.Sources);

        var description = topology.Describe();
        Assert.Contains("GLOBALJOIN", description);
    }
}
