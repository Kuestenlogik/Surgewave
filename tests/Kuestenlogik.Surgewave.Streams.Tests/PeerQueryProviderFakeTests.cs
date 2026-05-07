using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

/// <summary>
/// Verifies that <see cref="IPeerQueryProvider"/> works as an abstraction — an alternative
/// implementation (here a TCP-less in-memory fake) can be registered on <see cref="StreamsApplication"/>
/// via <see cref="StreamsApplication.WithPeerQueries"/> without needing any of the types from
/// <c>Kuestenlogik.Surgewave.Streams.InteractiveQueries</c>. This is the architectural justification for
/// introducing the interface: tests don't need real sockets, and alternative transports
/// (e.g. gRPC, in-memory, mesh networks) can plug in without touching the Streams core.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class PeerQueryProviderFakeTests
{
    [Fact]
    public void Fake_CanBeRegistered_OnStreamsApplication()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, int>("input").ForEach((_, _) => { });
        var topology = builder.Build();

        var config = new StreamsConfig
        {
            ApplicationId = "fake-test",
            BootstrapServers = "dummy:9092"
        };

        var app = new StreamsApplication(config, topology, NullLoggerFactory.Instance);
        var fake = new FakePeerQueryProvider(new HostInfo("fake-host", 1234));

        app.WithPeerQueries(fake);

        Assert.Same(fake, app.PeerQueries);
    }

    [Fact]
    public void WithPeerQueries_PushesInitialLocalMetadata()
    {
        var builder = new StreamsBuilder();
        builder.AddStateStore(Stores.KeyValueStore<string, int>("counts"));
        builder.Stream<string, int>("input").ForEach((_, _) => { });
        var topology = builder.Build();

        var config = new StreamsConfig
        {
            ApplicationId = "fake-test",
            BootstrapServers = "dummy:9092"
        };

        var app = new StreamsApplication(config, topology, NullLoggerFactory.Instance);
        var fake = new FakePeerQueryProvider(new HostInfo("fake-host", 1234));

        app.WithPeerQueries(fake);

        // Registration should have pushed an initial metadata snapshot into the provider
        // so consumers see the local instance immediately, even before Start() runs.
        Assert.Single(fake.UpdateCallLog);
        var pushed = fake.UpdateCallLog[0];
        Assert.Equal("fake-host", pushed.HostInfo.Host);
        Assert.Equal(1234, pushed.HostInfo.Port);
        Assert.Contains("counts", pushed.StateStoreNames);
    }

    [Fact]
    public void AllMetadata_FlowsThroughFakeProvider()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, int>("input").ForEach((_, _) => { });
        var topology = builder.Build();

        var config = new StreamsConfig
        {
            ApplicationId = "fake-test",
            BootstrapServers = "dummy:9092"
        };

        var app = new StreamsApplication(config, topology, NullLoggerFactory.Instance);
        var fake = new FakePeerQueryProvider(new HostInfo("fake-host", 1234));
        app.WithPeerQueries(fake);

        // Manually inject a second peer
        fake.InjectPeer(new StreamsMetadata(
            new HostInfo("peer-host", 5678),
            ["shared-store"],
            []));

        Assert.Equal(2, fake.AllMetadata.Count);
        Assert.Contains(fake.AllMetadata, m => m.HostInfo.Host == "fake-host");
        Assert.Contains(fake.AllMetadata, m => m.HostInfo.Host == "peer-host");
    }

    [Fact]
    public void FindByKey_DelegatesToFakeProvider()
    {
        var fake = new FakePeerQueryProvider(new HostInfo("fake-host", 1234));
        fake.PlantedKeyOwner = new StreamsMetadata(
            new HostInfo("owner", 5678),
            ["counts"],
            []);

        var result = fake.FindByKey("counts", [1, 2, 3]);

        Assert.NotNull(result);
        Assert.Equal("owner", result.HostInfo.Host);
        Assert.Single(fake.FindByKeyCalls);
        Assert.Equal("counts", fake.FindByKeyCalls[0].storeName);
    }

    [Fact]
    public async Task DisposeAsync_IsInvokedByStreamsApplicationShutdown()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, int>("input").ForEach((_, _) => { });
        var topology = builder.Build();

        var config = new StreamsConfig
        {
            ApplicationId = "fake-test",
            BootstrapServers = "dummy:9092"
        };

        var app = new StreamsApplication(config, topology, NullLoggerFactory.Instance);
        var fake = new FakePeerQueryProvider(new HostInfo("fake-host", 1234));
        app.WithPeerQueries(fake);

        await app.DisposeAsync();

        Assert.True(fake.Disposed);
    }

    // ────────────────────────────────────────────────────────────────────────
    // FakePeerQueryProvider — an IPeerQueryProvider with no sockets, no TCP
    // server, no deserialization. Drop-in replacement proving the interface
    // is the sole coupling point between StreamsApplication and the peer
    // query infrastructure.
    // ────────────────────────────────────────────────────────────────────────
    private sealed class FakePeerQueryProvider : IPeerQueryProvider
    {
        private readonly HostInfo _localHost;
        private readonly ConcurrentDictionary<HostInfo, StreamsMetadata> _metadata = new();

        public FakePeerQueryProvider(HostInfo localHost)
        {
            _localHost = localHost;
        }

        public HostInfo LocalHost => _localHost;

        public bool Started { get; private set; }
        public bool Disposed { get; private set; }

        public List<StreamsMetadata> UpdateCallLog { get; } = [];
        public List<(string storeName, byte[] keyBytes)> FindByKeyCalls { get; } = [];

        /// <summary>Planted result returned by <see cref="FindByKey"/> when set.</summary>
        public StreamsMetadata? PlantedKeyOwner { get; set; }

        public IReadOnlyCollection<StreamsMetadata> AllMetadata => _metadata.Values.ToList();

        public IReadOnlyCollection<StreamsMetadata> AllMetadataForStore(string storeName)
            => _metadata.Values.Where(m => m.StateStoreNames.Contains(storeName)).ToList();

        public StreamsMetadata? FindByKey(string storeName, byte[] keyBytes)
        {
            FindByKeyCalls.Add((storeName, keyBytes));
            return PlantedKeyOwner;
        }

        public Task RegisterPeerAsync(HostInfo peer, CancellationToken cancellationToken = default)
        {
            _metadata[peer] = new StreamsMetadata(peer, [], []);
            return Task.CompletedTask;
        }

        public void UpdateLocalMetadata(StreamsMetadata metadata)
        {
            _metadata[metadata.HostInfo] = metadata;
            UpdateCallLog.Add(metadata);
        }

        public void InjectPeer(StreamsMetadata metadata)
        {
            _metadata[metadata.HostInfo] = metadata;
        }

        public void Start(PeerQueryContext context)
        {
            Started = true;
        }

        public ValueTask DisposeAsync()
        {
            Disposed = true;
            return ValueTask.CompletedTask;
        }
    }
}
