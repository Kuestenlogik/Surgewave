using System.Text;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Testing.Chaos;
using Kuestenlogik.Surgewave.Testing.Chaos.Linearizability;
using Xunit;

namespace Kuestenlogik.Surgewave.Testing.Chaos.Tests.Linearizability;

/// <summary>
/// End-to-end checks: a live Surgewave broker receives produce / consume calls that are
/// recorded into a <see cref="History"/>, then <see cref="LinearizabilityChecker"/> is
/// run against the recording. A clean history must pass; the same history with an
/// injected tampering must fail. Chaos-scenario variants extend this with latency
/// injection to demonstrate that the invariants still hold under perturbation.
/// </summary>
[Trait("Category", TestCategories.Integration)]
public class LinearizabilityIntegrationTests
{
    private const string Topic = "linearizability-smoke";

    [Fact]
    public async Task CleanRun_PassesChecker()
    {
        await using var cluster = await ChaosCluster.CreateAsync(brokerCount: 1);
        var history = new History();

        await ProduceAndConsumeAsync(cluster, history, recordCount: 25);

        var result = new LinearizabilityChecker().Check(history);
        Assert.True(result.IsValid,
            $"Clean run failed linearizability check:\n{string.Join("\n", result.Violations.Select(v => v.Description))}");
    }

    [Fact]
    public async Task LatencyInjection_StillPassesChecker()
    {
        await using var cluster = await ChaosCluster.CreateAsync(brokerCount: 1);
        var history = new History();

        // Start produce+consume under injected broker latency. Linearizability is
        // about correctness, not performance — slow must still be correct.
        cluster.InjectLatency(brokerId: 0, TimeSpan.FromMilliseconds(5));
        try
        {
            await ProduceAndConsumeAsync(cluster, history, recordCount: 25);
        }
        finally
        {
            cluster.GetEngine(0).DeactivateAll();
        }

        var result = new LinearizabilityChecker().Check(history);
        Assert.True(result.IsValid,
            $"Latency-injected run failed linearizability check:\n{string.Join("\n", result.Violations.Select(v => v.Description))}");
    }

    [Fact]
    public async Task TamperedHistory_FailsChecker()
    {
        // Regression guard for the checker itself: if we deliberately inject a
        // consume with a value that never matches the corresponding produce, the
        // checker must flag it. Proves the end-to-end wiring actually evaluates the
        // recorded events rather than silently passing everything.
        await using var cluster = await ChaosCluster.CreateAsync(brokerCount: 1);
        var history = new History();

        await ProduceAndConsumeAsync(cluster, history, recordCount: 3);

        // Tamper: append a fabricated ConsumeOk at an already-acknowledged offset.
        history.Record(new ConsumeOk
        {
            ClientId = 999,
            Timestamp = DateTimeOffset.UtcNow,
            Topic = Topic,
            Partition = 0,
            Offset = 0,
            Value = Encoding.UTF8.GetBytes("this-was-not-produced"),
        });

        var result = new LinearizabilityChecker().Check(history);
        Assert.False(result.IsValid);
        // Two ConsumeOk records at the same offset with different values trigger
        // InconsistentReadsViolation; DivergentReadViolation only fires when the
        // first consumed value itself diverges from the acknowledged produce.
        // Either outcome means the checker caught the tampering — exactly what
        // this regression guard verifies.
        Assert.Contains(result.Violations,
            v => v is InconsistentReadsViolation or DivergentReadViolation);
    }

    private static async Task ProduceAndConsumeAsync(ChaosCluster cluster, History history, int recordCount)
    {
        var bootstrap = cluster.GetBootstrapServers(0);

        // --- Produce all records, recording each call + ack. ---
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageTimeoutMs = 10_000,
        };

        using (var producer = new ProducerBuilder<Null, string>(producerConfig).Build())
        {
            for (var i = 0; i < recordCount; i++)
            {
                var value = $"msg-{i}";
                var bytes = Encoding.UTF8.GetBytes(value);
                history.Record(new ProduceInvoke
                {
                    ClientId = 1,
                    Timestamp = DateTimeOffset.UtcNow,
                    Topic = Topic,
                    Partition = 0,
                    Value = bytes,
                });

                try
                {
                    var delivery = await producer.ProduceAsync(
                        new TopicPartition(Topic, new Partition(0)),
                        new Message<Null, string> { Value = value });

                    history.Record(new ProduceOk
                    {
                        ClientId = 1,
                        Timestamp = DateTimeOffset.UtcNow,
                        Topic = Topic,
                        Partition = delivery.Partition.Value,
                        Offset = delivery.Offset.Value,
                        Value = bytes,
                    });
                }
                catch (ProduceException<Null, string> ex)
                {
                    history.Record(new ProduceFail
                    {
                        ClientId = 1,
                        Timestamp = DateTimeOffset.UtcNow,
                        Topic = Topic,
                        Partition = 0,
                        Value = bytes,
                        Reason = ex.Error.Reason,
                    });
                }
            }
            producer.Flush(TimeSpan.FromSeconds(5));
        }

        // --- Consume back and record what we read. ---
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = "lin-checker-" + Guid.NewGuid().ToString("N")[..8],
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        using var consumer = new ConsumerBuilder<Null, string>(consumerConfig).Build();
        consumer.Assign(new TopicPartitionOffset(Topic, new Partition(0), new Offset(0)));

        var consumed = 0;
        var deadline = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(15);
        while (consumed < recordCount && DateTimeOffset.UtcNow < deadline)
        {
            history.Record(new ConsumeInvoke
            {
                ClientId = 2,
                Timestamp = DateTimeOffset.UtcNow,
                Topic = Topic,
                Partition = 0,
                FromOffset = consumed,
            });

            try
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result is null) continue;

                history.Record(new ConsumeOk
                {
                    ClientId = 2,
                    Timestamp = DateTimeOffset.UtcNow,
                    Topic = result.Topic,
                    Partition = result.Partition.Value,
                    Offset = result.Offset.Value,
                    Value = Encoding.UTF8.GetBytes(result.Message.Value),
                });
                consumed++;
            }
            catch (ConsumeException ex)
            {
                history.Record(new ConsumeFail
                {
                    ClientId = 2,
                    Timestamp = DateTimeOffset.UtcNow,
                    Topic = Topic,
                    Partition = 0,
                    FromOffset = consumed,
                    Reason = ex.Error.Reason,
                });
            }
        }

        consumer.Close();
    }
}
