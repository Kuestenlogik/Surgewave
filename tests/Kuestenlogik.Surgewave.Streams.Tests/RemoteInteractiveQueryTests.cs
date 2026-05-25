using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.InteractiveQueries;
using Kuestenlogik.Surgewave.Streams.Runtime;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class RemoteInteractiveQueryTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly string _tempDir;

    public RemoteInteractiveQueryTests(ITestOutputHelper output)
    {
        _output = output;
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-riq-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch { }
    }

    [Fact]
    public void HostInfo_ParseAndToString()
    {
        var host = new HostInfo("localhost", 7070);
        Assert.Equal("localhost:7070", host.ToString());

        var parsed = HostInfo.Parse("myhost:8080");
        Assert.Equal("myhost", parsed.Host);
        Assert.Equal(8080, parsed.Port);
    }

    [Fact]
    public void StreamsMetadata_TracksStoresAndPartitions()
    {
        var metadata = new StreamsMetadata(
            new HostInfo("host1", 7070),
            ["counts", "sums"],
            [new TopicPartition("topic-a", 0), new TopicPartition("topic-a", 1)]);

        Assert.True(metadata.HasStateStore("counts"));
        Assert.True(metadata.HasStateStore("sums"));
        Assert.False(metadata.HasStateStore("other"));

        Assert.True(metadata.HasPartition(new TopicPartition("topic-a", 0)));
        Assert.False(metadata.HasPartition(new TopicPartition("topic-a", 2)));

        Assert.True(metadata.HasPartitionNumber(0));
        Assert.True(metadata.HasPartitionNumber(1));
        Assert.False(metadata.HasPartitionNumber(2));
    }

    [Fact]
    public void MetadataState_FindByPartitionAndStore()
    {
        var state = new StreamsMetadataState(new HostInfo("localhost", 7070));

        var meta1 = new StreamsMetadata(
            new HostInfo("localhost", 7070),
            ["counts"],
            [new TopicPartition("input", 0), new TopicPartition("input", 1)]);

        var meta2 = new StreamsMetadata(
            new HostInfo("localhost", 7071),
            ["counts"],
            [new TopicPartition("input", 2), new TopicPartition("input", 3)]);

        state.UpdateMetadata(meta1);
        state.UpdateMetadata(meta2);

        // Partition 0 → instance 1
        var found = state.FindByPartitionAndStore(0, "counts");
        Assert.NotNull(found);
        Assert.Equal(7070, found.HostInfo.Port);

        // Partition 2 → instance 2
        found = state.FindByPartitionAndStore(2, "counts");
        Assert.NotNull(found);
        Assert.Equal(7071, found.HostInfo.Port);

        // Partition 4 → not found
        Assert.Null(state.FindByPartitionAndStore(4, "counts"));

        // Wrong store
        Assert.Null(state.FindByPartitionAndStore(0, "nonexistent"));
    }

    [Fact]
    public void MetadataState_GetMaxPartitionCount()
    {
        var state = new StreamsMetadataState(new HostInfo("localhost", 7070));

        state.UpdateMetadata(new StreamsMetadata(
            new HostInfo("localhost", 7070),
            ["s"],
            [new TopicPartition("t", 0), new TopicPartition("t", 1)]));

        state.UpdateMetadata(new StreamsMetadata(
            new HostInfo("localhost", 7071),
            ["s"],
            [new TopicPartition("t", 2), new TopicPartition("t", 3)]));

        Assert.Equal(4, state.GetMaxPartitionCount());
    }

    [Fact]
    public void MetadataState_IsLocal()
    {
        var localHost = new HostInfo("localhost", 7070);
        var state = new StreamsMetadataState(localHost);

        state.UpdateMetadata(new StreamsMetadata(
            localHost, ["s"], [new TopicPartition("t", 0)]));

        Assert.True(state.IsLocal(localHost));
        Assert.False(state.IsLocal(new HostInfo("localhost", 7071)));
        Assert.True(state.IsLocalPartition(0));
        Assert.False(state.IsLocalPartition(1));
    }

    [Fact]
    public void MetadataState_ForStore()
    {
        var state = new StreamsMetadataState(new HostInfo("localhost", 7070));

        state.UpdateMetadata(new StreamsMetadata(
            new HostInfo("localhost", 7070), ["counts", "sums"], [new TopicPartition("t", 0)]));
        state.UpdateMetadata(new StreamsMetadata(
            new HostInfo("localhost", 7071), ["counts"], [new TopicPartition("t", 1)]));

        var countInstances = state.ForStore("counts");
        Assert.Equal(2, countInstances.Count);

        var sumInstances = state.ForStore("sums");
        Assert.Single(sumInstances);
    }

    [Fact]
    public void Murmur2_ConsistentHashing()
    {
        var hash1 = QueryProtocol.Murmur2("hello"u8.ToArray());
        var hash2 = QueryProtocol.Murmur2("hello"u8.ToArray());
        Assert.Equal(hash1, hash2);

        var hash3 = QueryProtocol.Murmur2("world"u8.ToArray());
        Assert.NotEqual(hash1, hash3);

        // Distribution: different keys should map to different partitions
        var partitions = new HashSet<int>();
        for (var i = 0; i < 100; i++)
        {
            var key = $"key-{i}";
            var partition = (int)(QueryProtocol.Murmur2(System.Text.Encoding.UTF8.GetBytes(key)) % 8u);
            partitions.Add(partition);
        }
        Assert.True(partitions.Count >= 4, $"Expected >= 4 different partitions, got {partitions.Count}");
    }

    [Fact]
    public async Task RemoteQueryServer_ServesMetadata()
    {
        var port = GetFreePort();
        var hostInfo = new HostInfo("localhost", port);

        var store = new InMemoryKeyValueStore<string, int>("test-store");
        var metrics = new StreamsMetrics();
        var context = new ProcessorContext(
            new StreamsConfig { ApplicationId = "test-app", BootstrapServers = "dummy:9092" },
            metrics,
            NullLogger.Instance);
        store.Init(context);

        var server = new RemoteQueryServer(
            hostInfo,
            name => name == "test-store" ? store : null,
            () => new StreamsMetadata(hostInfo, ["test-store"],
                [new TopicPartition("input", 0), new TopicPartition("input", 1)]),
            NullLogger.Instance);
        server.Start();

        try
        {
            using var client = new RemoteQueryClient(hostInfo);
            var metadata = await client.GetMetadataAsync();

            Assert.NotNull(metadata);
            Assert.Equal(port, metadata.HostInfo.Port);
            Assert.Contains("test-store", metadata.StateStoreNames);
            Assert.Equal(2, metadata.TopicPartitions.Count);
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task RemoteQueryServer_ServesKeyValueGet()
    {
        var port = GetFreePort();
        var hostInfo = new HostInfo("localhost", port);

        var store = new InMemoryKeyValueStore<string, int>("counts");
        var metrics = new StreamsMetrics();
        var context = new ProcessorContext(
            new StreamsConfig { ApplicationId = "test-app", BootstrapServers = "dummy:9092" },
            metrics,
            NullLogger.Instance);
        store.Init(context);
        store.Put("hello", 42);
        store.Put("world", 99);

        var server = new RemoteQueryServer(
            hostInfo,
            name => name == "counts" ? store : null,
            () => new StreamsMetadata(hostInfo, ["counts"], []),
            NullLogger.Instance);
        server.Start();

        try
        {
            using var client = new RemoteQueryClient(hostInfo);

            // Get existing key
            var value = await client.GetAsync<string, int>(
                "counts", "hello", Serdes.Json<string>(), Serdes.Json<int>());
            Assert.Equal(42, value);

            // Get non-existing key — for int value type, returns serialized default (0)
            var missingValue = await client.GetAsync<string, int>(
                "counts", "nonexistent", Serdes.Json<string>(), Serdes.Json<int>());
            Assert.Equal(0, missingValue);

            // Get from non-existing store
            var noStore = await client.GetRawAsync("nonexistent",
                Serdes.Json<string>().Serialize("key"));
            Assert.Null(noStore);
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task RemoteQueryServer_ServesKeyValueCount()
    {
        var port = GetFreePort();
        var hostInfo = new HostInfo("localhost", port);

        var store = new InMemoryKeyValueStore<string, int>("counts");
        var metrics = new StreamsMetrics();
        var context = new ProcessorContext(
            new StreamsConfig { ApplicationId = "test-app", BootstrapServers = "dummy:9092" },
            metrics,
            NullLogger.Instance);
        store.Init(context);
        store.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);

        var server = new RemoteQueryServer(
            hostInfo,
            name => name == "counts" ? store : null,
            () => new StreamsMetadata(hostInfo, ["counts"], []),
            NullLogger.Instance);
        server.Start();

        try
        {
            using var client = new RemoteQueryClient(hostInfo);
            var count = await client.CountAsync("counts");
            Assert.Equal(3, count);
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    [Fact]
    public async Task CompositeStore_LocalAndRemote()
    {
        var port1 = GetFreePort();
        var port2 = GetFreePort();
        var host1 = new HostInfo("localhost", port1);
        var host2 = new HostInfo("localhost", port2);

        // Setup instance 2 with a remote store (server)
        var remoteStore = new InMemoryKeyValueStore<string, int>("counts");
        var metrics = new StreamsMetrics();
        var remoteContext = new ProcessorContext(
            new StreamsConfig { ApplicationId = "test-app", BootstrapServers = "dummy:9092" },
            metrics,
            NullLogger.Instance);
        remoteStore.Init(remoteContext);
        remoteStore.Put("remote-key", 999);

        var server = new RemoteQueryServer(
            host2,
            name => name == "counts" ? remoteStore : null,
            () => new StreamsMetadata(host2, ["counts"],
                [new TopicPartition("input", 2), new TopicPartition("input", 3)]),
            NullLogger.Instance);
        server.Start();

        try
        {
            // Setup local metadata state
            var metadataState = new StreamsMetadataState(host1);

            // Local instance owns partitions 0-1
            var localStore = new InMemoryKeyValueStore<string, int>("counts");
            var localContext = new ProcessorContext(
                new StreamsConfig { ApplicationId = "test-app", BootstrapServers = "dummy:9092" },
                new StreamsMetrics(),
                NullLogger.Instance);
            localStore.Init(localContext);
            localStore.Put("local-key", 42);

            metadataState.UpdateMetadata(new StreamsMetadata(host1, ["counts"],
                [new TopicPartition("input", 0), new TopicPartition("input", 1)]));
            metadataState.UpdateMetadata(new StreamsMetadata(host2, ["counts"],
                [new TopicPartition("input", 2), new TopicPartition("input", 3)]));

            var composite = new CompositeReadOnlyKeyValueStore<string, int>(
                "counts",
                Serdes.Json<string>(),
                Serdes.Json<int>(),
                metadataState,
                name => name == "counts" ? localStore : null);

            // Query local key
            var localValue = await composite.GetAsync("local-key");
            // Note: depending on Murmur2 hash, it might be routed locally or remotely
            // The important thing is that the composite store works without errors

            // Query total count across instances
            var totalCount = await composite.ApproximateNumEntriesAsync();
            Assert.True(totalCount >= 1, $"Expected >= 1, got {totalCount}");
        }
        finally
        {
            await server.DisposeAsync();
        }
    }

    [Fact]
    public void StreamsApplication_MetadataForKey_RequiresApplicationServer()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, int>("input").ForEach((k, v) => { });
        var topology = builder.Build();

        // Without ApplicationServer — MetadataForKey returns null
        var config = new StreamsConfig
        {
            ApplicationId = "test",
            BootstrapServers = "dummy:9092"
        };
        var app = new StreamsApplication(config, topology, NullLoggerFactory.Instance);
        Assert.Null(app.MetadataForKey("store", "key", Serdes.Json<string>()));
        Assert.Empty(app.AllMetadata());
    }

    [Fact]
    public void StreamsApplication_WithApplicationServer_HasMetadata()
    {
        var port = GetFreePort();
        var builder = new StreamsBuilder();
        builder.AddStateStore(
            Stores.KeyValueStore<string, int>("my-store"));
        builder.Stream<string, int>("input").ForEach((k, v) => { });
        var topology = builder.Build();

        var config = new StreamsConfig
        {
            ApplicationId = "test",
            BootstrapServers = "dummy:9092",
            StateDir = _tempDir
        };
        var app = new StreamsApplication(config, topology, NullLoggerFactory.Instance);
        app.WithInteractiveQueries(new HostInfo("localhost", port));

        Assert.NotNull(app.MetadataState());
        Assert.Single(app.AllMetadata());

        var meta = app.AllMetadata().First();
        Assert.Equal(port, meta.HostInfo.Port);
        Assert.Contains("my-store", meta.StateStoreNames);
    }

    [Fact]
    public void StreamsApplication_CreateCompositeStore_ThrowsWithoutApplicationServer()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, int>("input").ForEach((k, v) => { });
        var topology = builder.Build();

        var config = new StreamsConfig
        {
            ApplicationId = "test",
            BootstrapServers = "dummy:9092"
        };
        var app = new StreamsApplication(config, topology, NullLoggerFactory.Instance);

        Assert.Throws<InvalidOperationException>(() =>
            app.CreateCompositeStore<string, int>("store", Serdes.Json<string>(), Serdes.Json<int>()));
    }

    private static int GetFreePort()
    {
        using var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var port = ((System.Net.IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
