using System.Net;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// The shipped broker in a cluster of three controller voters under the readiness rule (#250): a broker reports ready,
/// and answers clients, only once it has caught up with the metadata log, and catching up needs a leader, hence a
/// majority of the voters.
/// </summary>
/// <remarks>
/// <para>
/// The multi-broker tests elsewhere run the embedded runtime, which its hosts start one broker after the other and
/// which does not hold a broker of a larger quorum back. Here it is the process a deployment starts: one broker alone
/// waits and says so, the second one makes the quorum and both become ready, and a broker that joins later, or comes
/// back to its data directory, is ready only when it can answer from what the cluster decided without it.
/// </para>
/// <para>
/// What a broker itself knows is asked on its own port with a request written by hand: a Kafka client would be told
/// the other brokers by the first answer and ask whichever it likes.
/// </para>
/// <para>
/// Two brokers started together with the third voter down are the case that found the election livelock: 1 run of
/// 20 under load ended with "Broker did not reach 'Surgewave broker ready to accept connections' within 90s", each
/// broker's Raft node winning the pre-vote 2/2 and then losing the election, term after term (ten terms in 55 s).
/// <c>RaftSplitVoteTests</c> holds the node to it.
/// </para>
/// <para>
/// Not in the BrokerBoot collection: each broker is on ports reserved for it and in a directory of its own, so the
/// class needs no turn.
/// </para>
/// </remarks>
[Trait("Category", "Integration")]
public sealed class ClusterReadinessTests
{
    private const short NoError = 0;

    [Fact(Timeout = 240_000)]
    public async Task ABrokerAloneWaitsForTheQuorumAndIsReadyOnceTheSecondOneIsUp()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var cluster = new Cluster();
        using var http = new HttpClient();

        var first = cluster.Launch(1);

        // What the broker says when it starts to wait, and the HTTP port it serves its probes on while it does.
        await first.WaitForLineAsync(BrokerProcess.CatchingUpLine, ct);
        await first.WaitForLineAsync(BrokerProcess.AppStartedLine, ct);
        var ready = await StatusAsync(http, first, "/health/ready", ct);
        var live = await StatusAsync(http, first, "/health/live", ct);
        Assert.True(ready == HttpStatusCode.ServiceUnavailable,
            $"broker 1, alone of three voters, answered /health/ready with {(int)ready}\n{first.Dump()}");
        Assert.True(live == HttpStatusCode.OK,
            $"broker 1, waiting for a quorum, answered /health/live with {(int)live}\n{first.Dump()}");
        Assert.False(first.HasReportedReady, $"broker 1 reported ready alone of three voters\n{first.Dump()}");

        var second = cluster.Launch(2);
        await cluster.ReadyAsync(first, ct);
        await cluster.ReadyAsync(second, ct);

        foreach (var broker in new[] { first, second })
        {
            ready = await StatusAsync(http, broker, "/health/ready", ct);
            Assert.True(ready == HttpStatusCode.OK,
                $"a broker that reported ready answered /health/ready with {(int)ready}\n{broker.Dump()}");
        }
    }

    [Fact(Timeout = 240_000)]
    public async Task ABrokerThatJoinsLaterIsReadyWithTheTopicsCreatedBeforeIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var cluster = new Cluster();
        var topic = $"orders-{Guid.NewGuid():N}";

        var first = cluster.Launch(1);
        var second = cluster.Launch(2);
        await cluster.ReadyAsync(first, ct);
        await cluster.ReadyAsync(second, ct);

        // Three partitions, which no default gives a topic.
        using (var admin = Admin(first))
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 3, ReplicationFactor = 1 }]);

        var third = cluster.Launch(3);
        await cluster.ReadyAsync(third, ct);

        var known = await KafkaWire.MetadataAsync(third.KafkaPort, topic, ct);
        Assert.True(known.Error == NoError && known.Leaders.Count == 3,
            $"broker 3, ready, answered error {known.Error} and {known.Leaders.Count} partitions for {topic}, created with three before it started\n{third.Dump()}");
    }

    [Fact(Timeout = 240_000)]
    public async Task ABrokerThatComesBackIsReadyWithWhatChangedWhileItWasDown()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var cluster = new Cluster();
        var topic = $"payments-{Guid.NewGuid():N}";
        var resource = new ConfigResource { Type = ResourceType.Topic, Name = topic };

        var brokers = new[] { cluster.Launch(1), cluster.Launch(2), cluster.Launch(3) };
        foreach (var broker in brokers)
            await cluster.ReadyAsync(broker, ct);

        // A replica on every broker: the one that leaves has the topic in its own logs too, with the old config.
        using var admin = Admin(brokers[0]);
        await admin.CreateTopicsAsync([new TopicSpecification
        {
            Name = topic,
            NumPartitions = 1,
            ReplicationFactor = 3,
            Configs = new Dictionary<string, string> { ["retention.ms"] = "111111" },
        }]);

        // A follower leaves, not the controller: the cluster keeps its leader, and what is asked is how a broker
        // comes back, not how a controller is replaced.
        var controller = (await KafkaWire.MetadataAsync(brokers[0].KafkaPort, topic, ct)).ControllerId;
        Assert.True(controller is >= 1 and <= 3, $"broker 1, ready, names controller {controller}\n{brokers[0].Dump()}");
        var leaving = controller == 3 ? 2 : 3;
        await cluster.StopAsync(leaving);

        await admin.IncrementalAlterConfigsAsync(new Dictionary<ConfigResource, List<ConfigEntry>>
        {
            [resource] = [new ConfigEntry { Name = "retention.ms", Value = "987654321", IncrementalOperation = AlterConfigOpType.Set }],
        });

        var back = cluster.Launch(leaving);
        await cluster.ReadyAsync(back, ct);

        var described = await KafkaWire.TopicConfigAsync(back.KafkaPort, topic, "retention.ms", ct);
        Assert.True(described == "987654321",
            $"broker {leaving}, back and ready, describes retention.ms of {topic} as '{described}'; it was changed to 987654321 while the broker was down\n{back.Dump()}");
    }

    private static async Task<HttpStatusCode> StatusAsync(HttpClient http, BrokerProcess broker, string path, CancellationToken ct)
    {
        using var response = await http.GetAsync(new Uri($"http://127.0.0.1:{broker.RestPort}{path}"), ct);
        return response.StatusCode;
    }

    private static IAdminClient Admin(BrokerProcess broker) =>
        new AdminClientBuilder(new AdminClientConfig { BootstrapServers = broker.BootstrapServers }).Build();

    /// <summary>
    /// Three brokers that name each other as cluster nodes and controller voters, each with ports and a data directory
    /// that stay its own across a restart. Disposing stops the ones running and removes the directories.
    /// </summary>
    private sealed class Cluster : IAsyncDisposable
    {
        private const int Size = 3;

        private readonly BrokerPorts[] _ports = [.. Enumerable.Range(0, Size).Select(_ => BrokerPorts.Reserve())];
        private readonly string[] _dataDirs = [.. Enumerable.Range(0, Size).Select(_ => NewDataDir())];
        private readonly BrokerProcess?[] _running = new BrokerProcess?[Size];
        private readonly string _nodes;
        private readonly string _voters;

        public Cluster()
        {
            _nodes = string.Join(",", _ports.Select((p, i) => $"{i + 1}:127.0.0.1:{p.Kafka}:{p.Replication}"));
            _voters = string.Join(",", _ports.Select((p, i) => $"{i + 1}@127.0.0.1:{p.Replication}"));
        }

        /// <summary>Starts the process of broker <paramref name="brokerId"/> without waiting for it.</summary>
        public BrokerProcess Launch(int brokerId)
        {
            Assert.Null(_running[brokerId - 1]);
            var broker = BrokerProcess.Launch(_dataDirs[brokerId - 1], _ports[brokerId - 1],
                $"--Surgewave:BrokerId={brokerId}",
                $"--Surgewave:ClusterNodes={_nodes}",
                $"--Surgewave:ControllerQuorumVoters={_voters}",
                // A topic a broker knows is one from the metadata log, never one it made up for the question.
                "--Surgewave:AutoCreateTopics=false",
                "--Surgewave:Connect:Enabled=false");
            _running[brokerId - 1] = broker;
            return broker;
        }

        /// <summary>
        /// Waits until <paramref name="broker"/> is ready. A failure carries what the other brokers said too: a
        /// broker that does not get ready is waiting for them.
        /// </summary>
        public async Task ReadyAsync(BrokerProcess broker, CancellationToken ct)
        {
            try
            {
                await broker.WaitUntilReadyAsync(ct);
            }
            catch (Xunit.Sdk.XunitException ex)
            {
                var others = Enumerable.Range(1, Size)
                    .Where(id => _running[id - 1] is { } other && other != broker)
                    .Select(id => $"--- output of broker {id}, running beside it ---\n{_running[id - 1]!.Log}");
                Assert.Fail(ex.Message + "\n" + string.Join("\n", others));
            }
        }

        /// <summary>Stops broker <paramref name="brokerId"/>; returns when its process has exited.</summary>
        public async Task StopAsync(int brokerId)
        {
            var broker = _running[brokerId - 1];
            Assert.NotNull(broker);
            _running[brokerId - 1] = null;
            await broker.DisposeAsync();
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var broker in _running)
            {
                if (broker is not null)
                    await broker.DisposeAsync();
            }
            foreach (var dir in _dataDirs)
                BrokerProcess.TryDelete(dir);
        }

        private static string NewDataDir()
        {
            var dir = Path.Combine(Path.GetTempPath(), "surgewave-boottest-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            return dir;
        }
    }
}
