using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Diagnostic harness for the KIP-848 e2e stall against librdkafka 2.14. Captures
/// every librdkafka log line plus Surgewave's debug output so we can identify which
/// RPC hangs. Marked [Trait] not [Fact(Skip)] so it can be run on demand from
/// the IDE without polluting the regular CI run.
/// </summary>
[Collection("Broker")]
[Trait("Category", TestCategories.Diagnostic)]
public sealed class Kip848DiagnosticTests
{
    private readonly ITestOutputHelper _output;
    private readonly BrokerFixture _fixture;

    public Kip848DiagnosticTests(BrokerFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
    }

    [Fact(Skip = "Diagnostic — run on demand via dotnet test --filter Category=Diagnostic")]
    public async Task NextGenConsumer_LogsEveryStep()
    {
        var topic = $"diag-{Guid.NewGuid().ToString("N")[..8]}";
        var groupId = $"diag-grp-{Guid.NewGuid().ToString("N")[..8]}";

        await CreateTopicAsync(topic, 3);
        await ProduceAsync(topic, 3);

        var config = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = groupId,
            ClientId = "diag-consumer",
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            Debug = "consumer,cgrp,protocol,broker,topic,fetch", // librdkafka verbose
        };
        config.Set("group.protocol", "consumer");

        using var consumer = new ConsumerBuilder<string, string>(config)
            .SetLogHandler((_, msg) => _output.WriteLine($"[rd-{msg.Level}] {msg.Facility}: {msg.Message}"))
            .SetErrorHandler((_, err) => _output.WriteLine($"[rd-ERR] {err.Code}: {err.Reason}"))
            .Build();

        consumer.Subscribe(topic);

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(15);
        int polls = 0;
        while (DateTime.UtcNow < deadline)
        {
            var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
            polls++;
            _output.WriteLine($"poll #{polls}: result={(result?.Message != null ? "msg" : "null")}, assignment=[{string.Join(",", consumer.Assignment.Select(tp => tp.Partition.Value))}]");
            if (result?.Message != null) break;
        }

        consumer.Close();
    }

    private static async Task CreateTopicAsync(string topic, int partitions)
    {
        var adminConfig = new AdminClientConfig { BootstrapServers = BrokerFixture.BootstrapServers };
        using var admin = new AdminClientBuilder(adminConfig).Build();
        try
        {
            await admin.CreateTopicsAsync([new TopicSpecification { Name = topic, NumPartitions = partitions, ReplicationFactor = 1 }]);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists)) { }
    }

    private static async Task ProduceAsync(string topic, int count)
    {
        var pc = new ProducerConfig { BootstrapServers = BrokerFixture.BootstrapServers, EnableIdempotence = true, Acks = Acks.All };
        using var producer = new ProducerBuilder<string, string>(pc).Build();
        for (int i = 0; i < count; i++)
        {
            await producer.ProduceAsync(topic, new Message<string, string> { Key = $"k{i}", Value = $"v{i}" });
        }
        producer.Flush(TimeSpan.FromSeconds(5));
    }
}
