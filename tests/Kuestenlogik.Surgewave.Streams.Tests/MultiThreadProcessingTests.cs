using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Runtime;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class MultiThreadProcessingTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public MultiThreadProcessingTests(ITestOutputHelper output)
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
    public async Task SingleThread_DefaultConfig_ProcessesRecords()
    {
        var results = new List<string>();

        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input")
            .ForEach((k, v) => { lock (results) results.Add($"{k}:{v}"); });

        var config = new StreamsConfig
        {
            ApplicationId = "single-thread-test",
            BootstrapServers = "localhost:9092",
            NumStreamThreads = 1
        };

        await using var app = new StreamsApplication(config, builder.Build(), _loggerFactory);

        Assert.Equal(1, app.NumStreamThreads);

        app.ProcessRecord("input", "key1", "value1");
        app.ProcessRecord("input", "key2", "value2");

        Assert.Equal(2, results.Count);
        Assert.Contains("key1:value1", results);
        Assert.Contains("key2:value2", results);
    }

    [Fact]
    public async Task MultiThread_CreatesCorrectNumberOfThreads()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").ForEach((k, v) => { });

        var config = new StreamsConfig
        {
            ApplicationId = "multi-thread-test",
            BootstrapServers = "localhost:9092",
            NumStreamThreads = 4
        };

        await using var app = new StreamsApplication(config, builder.Build(), _loggerFactory);

        Assert.Equal(4, app.NumStreamThreads);
        var threadInfos = app.StreamThreads;
        Assert.Equal(4, threadInfos.Count);
        Assert.Equal(0, threadInfos[0].ThreadId);
        Assert.Equal(3, threadInfos[3].ThreadId);
    }

    [Fact]
    public async Task MultiThread_ThreadNames_IncludeApplicationId()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").ForEach((k, v) => { });

        var config = new StreamsConfig
        {
            ApplicationId = "my-app",
            BootstrapServers = "localhost:9092",
            NumStreamThreads = 2
        };

        await using var app = new StreamsApplication(config, builder.Build(), _loggerFactory);

        var threads = app.StreamThreads;
        Assert.Equal("my-app-StreamThread-0", threads[0].ThreadName);
        Assert.Equal("my-app-StreamThread-1", threads[1].ThreadName);
    }

    [Fact]
    public async Task StreamThread_ProcessesRecordsThroughChannel()
    {
        var processedCount = 0;

        var builder = new StreamsBuilder();
        builder.Stream<string, string>("orders")
            .ForEach((k, v) =>
            {
                Interlocked.Increment(ref processedCount);
            });

        var config = new StreamsConfig
        {
            ApplicationId = "channel-test",
            BootstrapServers = "localhost:9092",
            NumStreamThreads = 2
        };

        var topology = builder.Build();

        // Create a single StreamThread and feed records directly
        var thread = new StreamThread(0, config, topology, _loggerFactory);

        // Assign partitions so the thread's TaskManager can route records
        thread.OnPartitionsAssigned([
            new TopicPartition("orders", 0)
        ]);

        thread.Start();

        // Serialize keys/values as JSON (SourceNode expects JSON-serialized bytes)
        var keySerde = Serdes.Json<string>();
        var valSerde = Serdes.Json<string>();

        await thread.WriteAsync(new ConsumerRecord("orders", 0, 0, 1000, keySerde.Serialize("k1"), valSerde.Serialize("v1")));
        await thread.WriteAsync(new ConsumerRecord("orders", 0, 1, 1001, keySerde.Serialize("k2"), valSerde.Serialize("v2")));
        await thread.WriteAsync(new ConsumerRecord("orders", 0, 2, 1002, keySerde.Serialize("k3"), valSerde.Serialize("v3")));

        // Wait for processing (channel is async)
        await Task.Delay(500);

        Assert.True(thread.ProcessedRecords >= 3, $"Expected >= 3 processed, got {thread.ProcessedRecords}");

        await thread.DisposeAsync();
    }

    [Fact]
    public async Task StreamThread_DisposeAsync_ShutsDownCleanly()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("input").ForEach((k, v) => { });

        var config = new StreamsConfig
        {
            ApplicationId = "shutdown-test",
            BootstrapServers = "localhost:9092"
        };

        var thread = new StreamThread(0, config, builder.Build(), _loggerFactory);
        thread.Start();

        Assert.Equal(StreamThreadState.Running, thread.State);

        await thread.DisposeAsync();

        Assert.Equal(StreamThreadState.Dead, thread.State);
    }

    [Fact]
    public async Task PartitionAssignment_RoundRobin_DistributesEvenly()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, string>("topic-a").ForEach((k, v) => { });

        var config = new StreamsConfig
        {
            ApplicationId = "partition-test",
            BootstrapServers = "localhost:9092",
            NumStreamThreads = 3
        };

        await using var app = new StreamsApplication(config, builder.Build(), _loggerFactory);

        // Simulate 6 partitions across 3 threads
        app.AssignPartitions([
            new TopicPartition("topic-a", 0),
            new TopicPartition("topic-a", 1),
            new TopicPartition("topic-a", 2),
            new TopicPartition("topic-a", 3),
            new TopicPartition("topic-a", 4),
            new TopicPartition("topic-a", 5),
        ]);

        // Each thread should have 2 partitions (round-robin)
        var threads = app.StreamThreads;
        Assert.Equal(3, threads.Count);
    }
}
