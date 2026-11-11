using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// A broker that reports ready has caught up with its metadata log (#250): a client that comes at once is answered
/// from the metadata as it was before the restart.
/// </summary>
/// <remarks>
/// A restarted broker holds its metadata log and has applied none of it until its Raft node is elected, an election
/// timeout after the node was created. The broker reported ready and accepted clients without waiting for that, so
/// whether a client was answered from the applied log depended on the rest of the startup taking longer than the
/// election — which it did, except on a fast start: "retention.ms of payments-… did not survive the restart" on CI,
/// from a DescribeConfigs that arrived while the log was being applied, after the topic's creation and before its
/// config change. Here the election timeout is seconds, so readiness before the election is certain instead of rare:
/// without the handshake the topic "does not exist" for the first request, every time.
/// </remarks>
[Trait("Category", "Integration")]
[Collection("BrokerBoot")]
public sealed class ReadyMeansCaughtUpTests
{
    // Far longer than the rest of a broker's startup; the election that precedes catching up comes after it.
    private const int ElectionTimeoutMs = 3000;

    [Fact(Timeout = 240_000)]
    public async Task ABrokerThatReportsReadyAnswersFromTheMetadataLogItStartedWith()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var dataDir = Path.Combine(Path.GetTempPath(), "surgewave-boottest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);
        var topic = $"payments-{Guid.NewGuid():N}";
        var resource = new ConfigResource { Type = ResourceType.Topic, Name = topic };

        try
        {
            await using (var first = await BrokerProcess.StartAsync(dataDir, cancellationToken, "--Surgewave:Connect:Enabled=false"))
            {
                using var admin = Admin(first);
                await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
                await admin.IncrementalAlterConfigsAsync(new Dictionary<ConfigResource, List<ConfigEntry>>
                {
                    [resource] = [new ConfigEntry { Name = "retention.ms", Value = "987654321", IncrementalOperation = AlterConfigOpType.Set }],
                });
            }

            await using var second = await BrokerProcess.StartAsync(dataDir, cancellationToken,
                "--Surgewave:Connect:Enabled=false",
                $"--Surgewave:RaftElectionTimeoutMinMs={ElectionTimeoutMs}",
                $"--Surgewave:RaftElectionTimeoutMaxMs={ElectionTimeoutMs + 1}");

            using var again = Admin(second);
            var described = await DescribeAsync(again, resource);
            Assert.True(described == "987654321",
                $"retention.ms of {topic} is '{described}' for the first request after the restart\n{second.Dump()}");

            // And it said so in that order: caught up, then ready.
            var log = second.Log;
            var ready = log.IndexOf(BrokerProcess.ReadyLine, StringComparison.Ordinal);
            var caughtUp = log.IndexOf(BrokerProcess.CaughtUpLine, StringComparison.Ordinal);
            Assert.True(caughtUp >= 0 && caughtUp < ready,
                $"the broker reported ready before it had caught up with the metadata log\n{second.Dump()}");
        }
        finally
        {
            BrokerProcess.TryDelete(dataDir);
        }
    }

    // The topic's retention.ms as the broker describes it, or the broker's error.
    private static async Task<string> DescribeAsync(IAdminClient admin, ConfigResource resource)
    {
        try
        {
            return (await admin.DescribeConfigsAsync([resource])).Single().Entries["retention.ms"].Value;
        }
        catch (DescribeConfigsException ex)
        {
            return ex.Results[0].Error.Reason;
        }
    }

    private static IAdminClient Admin(BrokerProcess broker) =>
        new AdminClientBuilder(new AdminClientConfig { BootstrapServers = broker.BootstrapServers }).Build();
}
