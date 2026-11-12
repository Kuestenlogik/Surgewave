using Confluent.Kafka;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// A broker with topic auto-creation off and Connect on (#215) starts: the Connect worker creates its internal topics
/// itself, as Kafka Connect does, independent of auto-creation. The start hung in the worker, which waited for a config
/// topic nobody created, and the broker never became ready. The worker now starts beside the broker (#268), and its
/// own line says when it has.
/// </summary>
[Trait("Category", "Integration")]
[Collection("BrokerBoot")]
public sealed class ConnectWithoutAutoCreationTests
{
    [Fact(Timeout = 240_000)]
    public async Task ABrokerWithoutAutoCreationStartsAndConnectHasItsTopics()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var dataDir = Path.Combine(Path.GetTempPath(), "surgewave-boottest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);

        try
        {
            await using var broker = await BrokerProcess.StartAsync(dataDir, cancellationToken, "--Surgewave:AutoCreateTopics=false");
            await broker.WaitForLineAsync(BrokerProcess.ConnectStartedLine, cancellationToken);

            using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = broker.BootstrapServers }).Build();
            var topics = admin.GetMetadata(TimeSpan.FromSeconds(10)).Topics.Select(t => t.Topic).ToHashSet(StringComparer.Ordinal);
            foreach (var topic in new[] { "surgewave-connect-configs", "surgewave-connect-offsets", "surgewave-connect-status" })
                Assert.True(topics.Contains(topic), $"Connect's internal topic {topic} is missing\n{broker.Dump()}");
        }
        finally
        {
            BrokerProcess.TryDelete(dataDir);
        }
    }
}
