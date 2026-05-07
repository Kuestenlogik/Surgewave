using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class TopologyTestDriverTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly StreamsConfig _config;

    public TopologyTestDriverTests(ITestOutputHelper output)
    {
        _output = output;
        _config = new StreamsConfig
        {
            ApplicationId = "test-driver",
            BootstrapServers = "dummy:9092"
        };
    }

    public void Dispose() { }

    [Fact]
    public void PipeInput_MapValues_ReadOutput()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, int>("input")
            .MapValues(v => v * 2)
            .To("output");

        using var driver = new TopologyTestDriver(builder.Build(), _config);
        var input = driver.CreateInputTopic<string, int>("input");
        var output = driver.CreateOutputTopic<string, int>("output");

        input.PipeInput("key1", 21);

        var result = output.ReadKeyValue();
        Assert.Equal("key1", result.Key);
        Assert.Equal(42, result.Value);
        Assert.True(output.IsEmpty);
    }

    [Fact]
    public void PipeInput_Filter_OnlyMatchingRecords()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, int>("numbers")
            .Filter((k, v) => v > 10)
            .To("big-numbers");

        using var driver = new TopologyTestDriver(builder.Build(), _config);
        var input = driver.CreateInputTopic<string, int>("numbers");
        var output = driver.CreateOutputTopic<string, int>("big-numbers");

        input.PipeInput("a", 5);
        input.PipeInput("b", 15);
        input.PipeInput("c", 3);
        input.PipeInput("d", 20);

        var results = output.ReadKeyValuesToList();
        Assert.Equal(2, results.Count);
        Assert.Equal("b", results[0].Key);
        Assert.Equal(15, results[0].Value);
        Assert.Equal("d", results[1].Key);
        Assert.Equal(20, results[1].Value);
    }

    [Fact]
    public void PipeInput_SelectKey_ChangesKey()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input")
            .SelectKey((k, v) => v.ToUpperInvariant())
            .To("output");

        using var driver = new TopologyTestDriver(builder.Build(), _config);
        var input = driver.CreateInputTopic<string, string>("input");
        var output = driver.CreateOutputTopic<string, string>("output");

        input.PipeInput("ignored", "hello");

        var result = output.ReadKeyValue();
        Assert.Equal("HELLO", result.Key);
        Assert.Equal("hello", result.Value);
    }

    [Fact]
    public void ReadRecord_IncludesTimestamp()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").To("output");

        using var driver = new TopologyTestDriver(builder.Build(), _config);
        var input = driver.CreateInputTopic<string, string>("input");
        var output = driver.CreateOutputTopic<string, string>("output");

        input.PipeInput("k", "v", 42_000);

        var record = output.ReadRecord();
        Assert.Equal("k", record.Key);
        Assert.Equal("v", record.Value);
        Assert.Equal(42_000, record.Timestamp);
    }

    [Fact]
    public void ReadValue_IgnoresKey()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, int>("input")
            .MapValues(v => v + 1)
            .To("output");

        using var driver = new TopologyTestDriver(builder.Build(), _config);
        var input = driver.CreateInputTopic<string, int>("input");
        var output = driver.CreateOutputTopic<string, int>("output");

        input.PipeInput("any-key", 99);

        Assert.Equal(100, output.ReadValue());
    }

    [Fact]
    public void OutputTopic_IsEmpty_BeforeAndAfterReads()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").To("output");

        using var driver = new TopologyTestDriver(builder.Build(), _config);
        var input = driver.CreateInputTopic<string, string>("input");
        var output = driver.CreateOutputTopic<string, string>("output");

        Assert.True(output.IsEmpty);
        Assert.Equal(0, output.QueueSize);

        input.PipeInput("k1", "v1");
        input.PipeInput("k2", "v2");

        Assert.False(output.IsEmpty);
        Assert.Equal(2, output.QueueSize);

        output.ReadKeyValue();
        Assert.Equal(1, output.QueueSize);

        output.ReadKeyValue();
        Assert.True(output.IsEmpty);
    }

    [Fact]
    public void OutputTopic_ReadEmpty_Throws()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").To("output");

        using var driver = new TopologyTestDriver(builder.Build(), _config);
        var output = driver.CreateOutputTopic<string, string>("output");

        Assert.Throws<InvalidOperationException>(() => output.ReadKeyValue());
    }

    [Fact]
    public void InputTopic_InvalidTopic_Throws()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").To("output");

        using var driver = new TopologyTestDriver(builder.Build(), _config);
        var input = driver.CreateInputTopic<string, string>("nonexistent");

        Assert.Throws<ArgumentException>(() => input.PipeInput("k", "v"));
    }

    [Fact]
    public void PipeInputList_ProcessesAll()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, int>("input")
            .MapValues(v => v * 10)
            .To("output");

        using var driver = new TopologyTestDriver(builder.Build(), _config);
        var input = driver.CreateInputTopic<string, int>("input");
        var output = driver.CreateOutputTopic<string, int>("output");

        input.PipeInputList([
            new("a", 1),
            new("b", 2),
            new("c", 3)
        ]);

        var results = output.ReadValuesToList();
        Assert.Equal([10, 20, 30], results);
    }

    [Fact]
    public void StateStore_AccessibleFromDriver()
    {
        var builder = new StreamsBuilder();
        builder.AddStateStore(Stores.KeyValueStore<string, int>("my-store"));
        builder.Stream<string, int>("input").ForEach((k, v) => { });

        using var driver = new TopologyTestDriver(builder.Build(), _config);

        var store = driver.GetStateStore<IKeyValueStore<string, int>>("my-store");
        Assert.NotNull(store);

        store.Put("counter", 42);
        Assert.Equal(42, store.Get("counter"));
    }

    [Fact]
    public void Metrics_TracksProcessedRecords()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").To("output");

        using var driver = new TopologyTestDriver(builder.Build(), _config);
        var input = driver.CreateInputTopic<string, string>("input");

        Assert.Equal(0, driver.Metrics.ProcessedRecords);

        input.PipeInput("k1", "v1");
        input.PipeInput("k2", "v2");

        Assert.Equal(2, driver.Metrics.ProcessedRecords);
    }

    [Fact]
    public void DefaultConfig_WorksWithoutExplicitConfig()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").To("output");

        using var driver = new TopologyTestDriver(builder.Build());
        var input = driver.CreateInputTopic<string, string>("input");
        var output = driver.CreateOutputTopic<string, string>("output");

        input.PipeInput("k", "v");

        var result = output.ReadKeyValue();
        Assert.Equal("k", result.Key);
        Assert.Equal("v", result.Value);
    }
}
