using System.Net.Http.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.BootTests;

/// <summary>
/// A topic created over Kafka is still there after the broker restarts (#212). The metadata log is the only store of
/// topics (#163), and the shipped host never gave the Kafka plugin's CreateTopics handler the cluster controller: the
/// topic went into the local log manager only, past the metadata log, and was gone on the next start while its
/// partition directories stayed on disk. The embedded runtime wired it, so only the real host shows it.
/// </summary>
[Trait("Category", "Integration")]
[Collection("BrokerBoot")]
public sealed class TopicSurvivesRestartTests
{
    [Fact(Timeout = 240_000)]
    public async Task ATopicCreatedOverKafkaIsListedAfterARestart()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var dataDir = Path.Combine(Path.GetTempPath(), "surgewave-boottest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);
        var topic = $"orders-{Guid.NewGuid():N}";

        try
        {
            await using (var first = await BrokerProcess.StartAsync(dataDir, cancellationToken, "--Surgewave:Connect:Enabled=false"))
            {
                using var admin = Admin(first);
                await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 2, ReplicationFactor = 1 }]);
                Assert.Equal(2, PartitionsOf(admin, topic));
            }

            await using var second = await BrokerProcess.StartAsync(dataDir, cancellationToken, "--Surgewave:Connect:Enabled=false");
            using var again = Admin(second);
            Assert.True(PartitionsOf(again, topic) == 2, $"topic {topic} did not survive the restart\n{second.Dump()}");
        }
        finally
        {
            BrokerProcess.TryDelete(dataDir);
        }
    }

    [Fact(Timeout = 240_000)]
    public async Task ATopicCreatedOverRestIsThereWithItsConfigAfterARestart()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var dataDir = Path.Combine(Path.GetTempPath(), "surgewave-boottest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);
        var topic = $"invoices-{Guid.NewGuid():N}";

        try
        {
            // The transcoded gRPC TopicService created the topic in the log manager alone, past the metadata log, and
            // it was gone after a restart (#214); its config never reached the log manager from the metadata log.
            await using (var first = await BrokerProcess.StartAsync(dataDir, cancellationToken, "--Surgewave:Connect:Enabled=false"))
            {
                using var http = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{first.RestPort}") };
                using var created = await http.PostAsJsonAsync("/v3/topics", new
                {
                    topic,
                    numPartitions = 2,
                    replicationFactor = 1,
                    config = new Dictionary<string, string> { ["retention.ms"] = "123456789" },
                }, cancellationToken);
                var body = await created.Content.ReadAsStringAsync(cancellationToken);
                Assert.True(created.IsSuccessStatusCode && ErrorOf(body) is null or "NONE",
                    $"POST /v3/topics answered {(int)created.StatusCode}: {body}\n{first.Dump()}");
            }

            await using var second = await BrokerProcess.StartAsync(dataDir, cancellationToken, "--Surgewave:Connect:Enabled=false");
            using var admin = Admin(second);
            Assert.True(PartitionsOf(admin, topic) == 2, $"topic {topic} did not survive the restart\n{second.Dump()}");
            var described = await admin.DescribeConfigsAsync([new ConfigResource { Type = ResourceType.Topic, Name = topic }]);
            Assert.Equal("123456789", described.Single().Entries["retention.ms"].Value);
        }
        finally
        {
            BrokerProcess.TryDelete(dataDir);
        }
    }

    [Fact(Timeout = 240_000)]
    public async Task AConfigChangedOverKafkaIsThereAfterARestart()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var dataDir = Path.Combine(Path.GetTempPath(), "surgewave-boottest-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);
        var topic = $"payments-{Guid.NewGuid():N}";
        var resource = new ConfigResource { Type = ResourceType.Topic, Name = topic };

        try
        {
            // A config change was made in the log manager of the broker asked alone, past the metadata log, and was
            // gone after a restart (#119); the topic's other replicas never had it.
            await using (var first = await BrokerProcess.StartAsync(dataDir, cancellationToken, "--Surgewave:Connect:Enabled=false"))
            {
                using var admin = Admin(first);
                await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
                await admin.IncrementalAlterConfigsAsync(new Dictionary<ConfigResource, List<ConfigEntry>>
                {
                    [resource] = [new ConfigEntry { Name = "retention.ms", Value = "987654321", IncrementalOperation = AlterConfigOpType.Set }],
                });
                Assert.Equal("987654321", (await admin.DescribeConfigsAsync([resource])).Single().Entries["retention.ms"].Value);
            }

            await using var second = await BrokerProcess.StartAsync(dataDir, cancellationToken, "--Surgewave:Connect:Enabled=false");
            using var again = Admin(second);
            var described = await again.DescribeConfigsAsync([resource]);
            Assert.True(described.Single().Entries["retention.ms"].Value == "987654321",
                $"retention.ms of {topic} did not survive the restart\n{second.Dump()}");
        }
        finally
        {
            BrokerProcess.TryDelete(dataDir);
        }
    }

    // The response's error code, null when it has none (proto3 JSON leaves NONE out).
    private static string? ErrorOf(string body) =>
        System.Text.Json.JsonDocument.Parse(body).RootElement.TryGetProperty("status", out var status)
        && status.TryGetProperty("errorCode", out var code) ? code.ToString() : null;

    private static IAdminClient Admin(BrokerProcess broker) =>
        new AdminClientBuilder(new AdminClientConfig { BootstrapServers = broker.BootstrapServers }).Build();

    // The partition count the broker reports for the topic, 0 when it does not know it.
    private static int PartitionsOf(IAdminClient admin, string topic)
    {
        var metadata = admin.GetMetadata(TimeSpan.FromSeconds(10));
        return metadata.Topics.SingleOrDefault(t => t.Topic == topic) is { Error.IsError: false } t ? t.Partitions.Count : 0;
    }
}
