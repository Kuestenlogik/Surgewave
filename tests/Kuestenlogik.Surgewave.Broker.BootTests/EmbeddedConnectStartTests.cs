using System.Net;
using System.Net.Http.Json;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// The shipped broker with its embedded Connect worker enabled, which is the image's default (#268): the worker is a
/// client of the broker it runs in and starts once that broker accepts clients, beside the broker's start and not as a
/// step of it.
/// </summary>
/// <remarks>
/// <para>
/// In a cluster of three such brokers, two of them never became ready. The worker's start was a step of the host's
/// start, and on a broker that hosts no replica of the worker's config topic it never ended: asked for the topic's
/// offsets, the broker took the topic for a missing one, went on to auto-create it, was refused by the metadata log
/// and answered nothing. Now and then a start hung earlier: each worker asks for its topics as soon as its broker
/// lets it in, which is while the first elections are still under way, and a controller that lost its leadership
/// with a topic's entry not yet on a majority answered "created" for a topic the next leader's log did not have.
/// And a broker that waited for a quorum waited inside the worker's start, which needs the listener the broker opens
/// only once it has caught up: its HTTP port was not bound and no liveness probe was answered meanwhile.
/// </para>
/// <para>
/// Each broker is asked on its own HTTP port. Not in the BrokerBoot collection: every broker is on ports reserved for
/// it and in a directory of its own.
/// </para>
/// </remarks>
[Trait("Category", "Integration")]
public sealed class EmbeddedConnectStartTests
{
    private const short NoError = 0;

    // The host's word that the worker failed to start and that the broker stops for it.
    private const string ConnectDidNotStartLine = "The Connect worker did not start; stopping the broker";
    private static readonly string[] ConnectTopics = ["surgewave-connect-configs", "surgewave-connect-offsets", "surgewave-connect-status"];

    [Fact(Timeout = 300_000)]
    public async Task ThreeBrokersWithConnectAreReadyAndEachAnswersForItsConnectorsAndPipelines()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var cluster = new ConnectCluster();
        using var http = new HttpClient();

        var brokers = new[] { cluster.Launch(1), cluster.Launch(2), cluster.Launch(3) };
        foreach (var broker in brokers)
            await cluster.ReadyAsync(broker, ct);
        foreach (var broker in brokers)
            await cluster.SaidAsync(broker, BrokerProcess.ConnectStartedLine, ct);

        var controller = (await KafkaWire.MetadataAsync(brokers[0].KafkaPort, ConnectTopics[0], ct)).ControllerId;
        Assert.True(controller is >= 1 and <= 3, $"broker 1, ready, names controller {controller}\n{brokers[0].Dump()}");

        for (var id = 1; id <= brokers.Length; id++)
        {
            var broker = brokers[id - 1];
            var role = id == controller ? "the controller" : "not the controller";

            // The worker's topics are in the metadata log, which every broker has: none of them is one broker's own.
            foreach (var topic in ConnectTopics)
            {
                var known = await KafkaWire.MetadataAsync(broker.KafkaPort, topic, ct);
                Assert.True(known.Error == NoError && known.Leaders.Count > 0,
                    $"broker {id}, {role}, answered error {known.Error} and {known.Leaders.Count} partitions for {topic}\n{broker.Dump()}");
            }

            using var connectors = await http.GetAsync(new Uri($"http://127.0.0.1:{broker.RestPort}/v3/clusters/local/connectors"), ct);
            Assert.True(connectors.StatusCode == HttpStatusCode.OK,
                $"broker {id}, {role}, answered GET /v3/clusters/local/connectors with {(int)connectors.StatusCode}\n{broker.Dump()}");

            using var pipelines = await http.GetAsync(new Uri($"http://127.0.0.1:{broker.RestPort}/api/pipelines"), ct);
            Assert.True(pipelines.StatusCode == HttpStatusCode.OK,
                $"broker {id}, {role}, answered GET /api/pipelines with {(int)pipelines.StatusCode}\n{broker.Dump()}");
        }
    }

    [Fact(Timeout = 300_000)]
    public async Task ABrokerAloneWithConnectAnswersItsLivenessProbeAndStartsTheWorkerOnceTheQuorumIsThere()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var cluster = new ConnectCluster();
        using var http = new HttpClient();

        var first = cluster.Launch(1);

        // Alone of three voters it catches up with nothing, and says so; its HTTP port is up all the same.
        await first.WaitForLineAsync(BrokerProcess.CatchingUpLine, ct);
        await first.WaitForLineAsync(BrokerProcess.AppStartedLine, ct);
        var live = await StatusAsync(http, first, "/health/live", ct);
        var ready = await StatusAsync(http, first, "/health/ready", ct);
        var pipelines = await StatusAsync(http, first, "/api/pipelines", ct);
        Assert.True(live == HttpStatusCode.OK,
            $"broker 1, waiting for a quorum with Connect enabled, answered /health/live with {(int)live}\n{first.Dump()}");
        Assert.True(ready == HttpStatusCode.ServiceUnavailable,
            $"broker 1, alone of three voters, answered /health/ready with {(int)ready}\n{first.Dump()}");
        Assert.True(pipelines == HttpStatusCode.ServiceUnavailable,
            $"broker 1, alone of three voters, answered /api/pipelines with {(int)pipelines}\n{first.Dump()}");
        Assert.False(first.HasReportedReady, $"broker 1 reported ready alone of three voters\n{first.Dump()}");
        Assert.DoesNotContain(BrokerProcess.ConnectStartedLine, first.Log, StringComparison.Ordinal);

        // Both of the others: two brokers started together with the third voter down sometimes elect no leader, which
        // is the Raft node's and not what is asked here.
        var others = new[] { cluster.Launch(2), cluster.Launch(3) };
        await cluster.ReadyAsync(first, ct);
        await cluster.SaidAsync(first, BrokerProcess.ConnectStartedLine, ct);
        foreach (var other in others)
            await cluster.ReadyAsync(other, ct);

        pipelines = await StatusAsync(http, first, "/api/pipelines", ct);
        Assert.True(pipelines == HttpStatusCode.OK,
            $"broker 1, ready and with its worker started, answered /api/pipelines with {(int)pipelines}\n{first.Dump()}");
    }

    [Fact(Timeout = 300_000)]
    public async Task APipelineStoredBeforeARestartIsListedOnceTheWorkerHasStartedAgain()
    {
        var ct = TestContext.Current.CancellationToken;
        var dataDir = ConnectCluster.NewDataDir();
        var name = $"orders-to-archive-{Guid.NewGuid():N}";
        using var http = new HttpClient();

        try
        {
            await using (var first = await BrokerProcess.StartAsync(dataDir, ct, "--Surgewave:Connect:Enabled=true"))
            {
                await first.WaitForLineAsync(BrokerProcess.ConnectStartedLine, ct);
                using var created = await http.PostAsJsonAsync(
                    new Uri($"http://127.0.0.1:{first.RestPort}/api/pipelines"), new { name }, ct);
                Assert.True(created.StatusCode == HttpStatusCode.Created,
                    $"POST /api/pipelines answered {(int)created.StatusCode}: {await created.Content.ReadAsStringAsync(ct)}\n{first.Dump()}");
            }

            await using var second = await BrokerProcess.StartAsync(dataDir, ct, "--Surgewave:Connect:Enabled=true");
            await second.WaitForLineAsync(BrokerProcess.ConnectStartedLine, ct);

            var listed = await http.GetStringAsync(new Uri($"http://127.0.0.1:{second.RestPort}/api/pipelines"), ct);
            Assert.True(listed.Contains(name, StringComparison.Ordinal),
                $"the pipeline {name}, stored before the restart, is not listed after it: {listed}\n{second.Dump()}");
        }
        finally
        {
            BrokerProcess.TryDelete(dataDir);
        }
    }

    [Fact(Timeout = 300_000)]
    public async Task ABrokerWhoseWorkerCannotStartStops()
    {
        var ct = TestContext.Current.CancellationToken;
        var dataDir = ConnectCluster.NewDataDir();

        try
        {
            // A config topic no broker creates: its name is not a topic name.
            await using var broker = BrokerProcess.Launch(dataDir, BrokerPorts.Reserve(),
                "--Surgewave:Connect:Enabled=true",
                "--Surgewave:Connect:ConfigTopic=not a/topic name");

            // The worker starts beside the host, so its failure no longer ends the host's start by itself: it stops
            // the broker, which would otherwise answer for connectors it does not run.
            await broker.WaitForLineAsync(ConnectDidNotStartLine, ct);
            var exitCode = await broker.WaitForExitAsync(ct);

            Assert.True(exitCode != 0, $"a broker whose Connect worker did not start exited with code 0\n{broker.Dump()}");
            Assert.DoesNotContain(BrokerProcess.ConnectStartedLine, broker.Log, StringComparison.Ordinal);
        }
        finally
        {
            BrokerProcess.TryDelete(dataDir);
        }
    }

    private static async Task<HttpStatusCode> StatusAsync(HttpClient http, BrokerProcess broker, string path, CancellationToken ct)
    {
        using var response = await http.GetAsync(new Uri($"http://127.0.0.1:{broker.RestPort}{path}"), ct);
        return response.StatusCode;
    }

    /// <summary>
    /// Three brokers that name each other as cluster nodes and controller voters, each with the embedded Connect
    /// worker enabled, on ports and in a data directory of its own. Disposing stops them and removes the directories.
    /// </summary>
    private sealed class ConnectCluster : IAsyncDisposable
    {
        private const int Size = 3;

        private readonly BrokerPorts[] _ports = [.. Enumerable.Range(0, Size).Select(_ => BrokerPorts.Reserve())];
        private readonly string[] _dataDirs = [.. Enumerable.Range(0, Size).Select(_ => NewDataDir())];
        private readonly BrokerProcess?[] _running = new BrokerProcess?[Size];
        private readonly string _nodes;
        private readonly string _voters;

        public ConnectCluster()
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
                // Topic auto-creation stays as the image has it, on: with it, a broker that hosts no replica of the
                // worker's config topic took the topic for a missing one when its worker asked for it.
                "--Surgewave:Connect:Enabled=true");
            _running[brokerId - 1] = broker;
            return broker;
        }

        /// <summary>Waits until <paramref name="broker"/> is ready; a failure carries what the other brokers said too.</summary>
        public Task ReadyAsync(BrokerProcess broker, CancellationToken ct) =>
            WithTheOthersAsync(broker, () => broker.WaitUntilReadyAsync(ct));

        /// <summary>
        /// Waits until <paramref name="broker"/> has written <paramref name="line"/>; a failure carries what the other
        /// brokers said too.
        /// </summary>
        public Task SaidAsync(BrokerProcess broker, string line, CancellationToken ct) =>
            WithTheOthersAsync(broker, () => broker.WaitForLineAsync(line, ct));

        // A broker that does not get there is waiting for the others.
        private async Task WithTheOthersAsync(BrokerProcess broker, Func<Task> wait)
        {
            try
            {
                await wait();
            }
            catch (Xunit.Sdk.XunitException ex)
            {
                var others = Enumerable.Range(1, Size)
                    .Where(id => _running[id - 1] is { } other && other != broker)
                    .Select(id => $"--- output of broker {id}, running beside it ---\n{_running[id - 1]!.Log}");
                Assert.Fail(ex.Message + "\n" + string.Join("\n", others));
            }
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

        public static string NewDataDir()
        {
            var dir = Path.Combine(Path.GetTempPath(), "surgewave-boottest-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(dir);
            return dir;
        }
    }
}
