using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.Client.Native;
using Xunit;

using static Kuestenlogik.Surgewave.Broker.BootTests.KafkaWire;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// What a broker of a cluster answers about the cluster is the cluster's, whichever broker is asked (#272).
/// </summary>
/// <remarks>
/// The native cluster info, broker list and topic description — what <c>surgewave cluster status</c>,
/// <c>cluster nodes</c> and <c>cluster partitions</c> show — and the gRPC metadata behind <c>/v3/metadata</c> named
/// the broker asked as the controller, the only node, and the leader and only replica of every partition. Two
/// brokers of the shipped binary in one cluster, each asked the same questions; the answers have to agree, and to
/// name both brokers.
/// </remarks>
[Trait("Category", "Integration")]
[Collection("BrokerBoot")]
public sealed class ClusterAnswersDescribeTheClusterTests
{
    [Fact(Timeout = 240_000)]
    public async Task EveryBrokerNamesTheSameControllerBrokersAndPartitionLeaders()
    {
        var ct = TestContext.Current.CancellationToken;
        var dirs = new[] { NewDataDir(), NewDataDir() };
        var ports = new[] { BrokerPorts.Reserve(), BrokerPorts.Reserve() };
        var nodes = string.Join(",", Enumerable.Range(0, 2).Select(i => $"{i + 1}:127.0.0.1:{ports[i].Kafka}:{ports[i].Replication}"));
        var voters = string.Join(",", Enumerable.Range(0, 2).Select(i => $"{i + 1}@127.0.0.1:{ports[i].Replication}"));
        var topic = $"orders-{Guid.NewGuid():N}";

        try
        {
            var starting = Enumerable.Range(0, 2).Select(i => BrokerProcess.StartAsync(dirs[i], ports[i], ct,
                $"--Surgewave:BrokerId={i + 1}",
                $"--Surgewave:ClusterNodes={nodes}",
                $"--Surgewave:ControllerQuorumVoters={voters}")).ToArray();
            await using var first = await starting[0];
            await using var second = await starting[1];
            var brokers = new[] { first, second };

            // Two partitions, one replica each: the assignment puts one on each broker.
            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = first.BootstrapServers }).Build();
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 2, ReplicationFactor = 1 }]);

            // What the Kafka Metadata of each broker says, once both name a leader for both partitions: the answer
            // the others are held against.
            var leaders = await LeadersKnownToAsync(first.KafkaPort, topic, ct);
            Assert.Equal(leaders, await LeadersKnownToAsync(second.KafkaPort, topic, ct));
            Assert.Equal([1, 2], leaders.Values.Order());

            using var http = new HttpClient();
            int? controller = null;
            foreach (var broker in brokers)
            {
                var id = Array.IndexOf(brokers, broker) + 1;
                await using var client = new SurgewaveNativeClient("127.0.0.1", broker.KafkaPort);
                await client.ConnectAsync(ct);

                var info = await client.Cluster.GetClusterInfoAsync(ct);
                controller ??= info.ControllerId;
                Assert.True(info.ControllerId is 1 or 2, $"broker {id} names controller {info.ControllerId}\n{broker.Dump()}");
                Assert.True(info.ControllerId == controller, $"broker {id} names controller {info.ControllerId}, broker 1 names {controller}");
                Assert.Equal(id, info.BrokerId);
                Assert.Equal(info.ControllerId == id, info.IsController);

                var listed = await client.Cluster.ListBrokersAsync(ct);
                Assert.Equal([1, 2], listed.Select(b => b.BrokerId).Order());
                Assert.Equal([controller.Value], listed.Where(b => b.IsController).Select(b => b.BrokerId));
                Assert.All(listed, b => Assert.True(b.IsAlive, $"broker {id} holds broker {b.BrokerId} to be dead"));

                var described = await client.Topics.DescribeAsync(topic, ct);
                Assert.Equal(leaders, described.Partitions.ToDictionary(p => p.PartitionId, p => p.Leader));
                Assert.All(described.Partitions, p =>
                {
                    Assert.Equal([p.Leader], p.Replicas);
                    Assert.Equal([p.Leader], p.Isr);
                });

                using var answer = await http.GetAsync(new Uri($"http://127.0.0.1:{broker.RestPort}/v3/metadata?topics={topic}"), ct);
                var body = await answer.Content.ReadAsStringAsync(ct);
                Assert.True(answer.IsSuccessStatusCode, $"GET /v3/metadata on broker {id} answered {(int)answer.StatusCode}: {body}");
                using var metadata = JsonDocument.Parse(body);
                Assert.True(
                    metadata.RootElement.GetProperty("controllerId").GetInt32() == controller,
                    $"GET /v3/metadata on broker {id} names another controller than {controller}: {body}");
                var partitions = metadata.RootElement.GetProperty("topics").EnumerateArray()
                    .Single(t => t.GetProperty("name").GetString() == topic)
                    .GetProperty("partitions").EnumerateArray()
                    .ToDictionary(p => PartitionOf(p), p => p.GetProperty("leader").GetInt32());
                Assert.True(leaders.SequenceEqual(partitions.OrderBy(p => p.Key)), $"GET /v3/metadata on broker {id}: {body}");
            }
        }
        finally
        {
            foreach (var dir in dirs)
                BrokerProcess.TryDelete(dir);
        }
    }

    // JSON of a proto message leaves out a field that has its default, and partition 0 has.
    private static int PartitionOf(JsonElement partition)
        => partition.TryGetProperty("partitionId", out var index) ? index.GetInt32() : 0;

    private static string NewDataDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "surgewave-boottest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    // The leader of each partition as the broker's Kafka Metadata names it, once it names one for both.
    private static async Task<SortedDictionary<int, int>> LeadersKnownToAsync(int port, string topic, CancellationToken ct)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(TimeSpan.FromSeconds(60));
        while (true)
        {
            var leaders = (await MetadataAsync(port, topic, timeout.Token)).Leaders;
            if (leaders.Count == 2 && leaders.Values.All(l => l > 0))
                return new SortedDictionary<int, int>(leaders.ToDictionary(l => l.Key, l => l.Value));
            // The metadata log reaches the broker in its own time, and nothing tells a client when; the next request
            // asks again.
            await Task.Delay(TimeSpan.FromMilliseconds(200), timeout.Token);
        }
    }
}
