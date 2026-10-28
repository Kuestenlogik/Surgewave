using DotNet.Testcontainers.Containers;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// The broker against a Java 4.x client — Apache Kafka's own tools (#195). Hand-written codecs and a schema
/// check still left defects only a real Java client hit: a classic group that could not sync, KIP-848 groups
/// missing from the group list, `--describe` failing on a classic group and showing no offsets. Each test is
/// one thing an operator does with the Kafka tools, checked by what the tools print.
/// </summary>
[Collection("JavaClient")]
[Trait("Category", TestCategories.Integration)]
public sealed class JavaClientEndToEndTests(JavaClientFixture fixture)
{
    private const int Records = 500;

    [Fact]
    public async Task AClassicGroupConsumesEveryRecordAndCommitsItsOffsets()
    {
        fixture.SkipUnlessAvailable();
        var (topic, group) = Names("classic");
        await CreateTopicWithRecordsAsync(topic);

        var consumed = await ConsumeAsync(topic, group, "--consumer-property group.protocol=classic");

        Assert.Equal(Records, consumed);
        AssertCommittedEverything(await DescribeGroupAsync(group), topic);
    }

    [Fact]
    public async Task AConsumerProtocolGroupConsumesEveryRecordAndCommitsItsOffsets()
    {
        fixture.SkipUnlessAvailable();
        var (topic, group) = Names("kip848");
        await CreateTopicWithRecordsAsync(topic);

        var consumed = await ConsumeAsync(topic, group, "--consumer-property group.protocol=consumer");

        Assert.Equal(Records, consumed);
        AssertCommittedEverything(await DescribeGroupAsync(group), topic);
    }

    [Fact]
    public async Task ATransactionalProducerCommitsAndAReadCommittedConsumerSeesEveryRecord()
    {
        fixture.SkipUnlessAvailable();
        var (topic, group) = Names("txn");
        var transactionalId = $"{topic}-tx";
        await RunOkAsync($"kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic {topic} --partitions 3 --replication-factor 1");

        await RunOkAsync(
            $"kafka-producer-perf-test.sh --topic {topic} --num-records {Records} --record-size 100 --throughput -1 " +
            $"--transaction-duration-ms 200 --producer-props bootstrap.servers=$BOOTSTRAP transactional.id={transactionalId}");
        var consumed = await ConsumeAsync(topic, group, "--isolation-level read_committed");

        Assert.Equal(Records, consumed);
        var transactions = await RunOkAsync("kafka-transactions.sh --bootstrap-server $BOOTSTRAP list");
        // TransactionalId, Coordinator, ProducerId, TransactionState
        var row = Assert.Single(Columns(transactions.Stdout), c => c[0] == transactionalId);
        Assert.Equal("CompleteCommit", row[3]);
    }

    [Fact]
    public async Task TheGroupListNamesClassicAndConsumerProtocolGroupsWithTheirType()
    {
        fixture.SkipUnlessAvailable();
        var (topic, classic) = Names("list");
        var consumer = $"{classic}-848";
        await CreateTopicWithRecordsAsync(topic);
        await ConsumeAsync(topic, classic, "--consumer-property group.protocol=classic");
        await ConsumeAsync(topic, consumer, "--consumer-property group.protocol=consumer");

        // GROUP, TYPE, PROTOCOL
        var listed = Columns((await RunOkAsync("kafka-groups.sh --bootstrap-server $BOOTSTRAP --list")).Stdout)
            .Where(c => c.Length >= 2)
            .ToDictionary(c => c[0], c => c[1]);

        Assert.Equal("Classic", listed[classic]);
        Assert.Equal("Consumer", listed[consumer]);
    }

    private static (string Topic, string Group) Names(string scenario)
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        return ($"java-{scenario}-{suffix}", $"g-{scenario}-{suffix}");
    }

    private async Task CreateTopicWithRecordsAsync(string topic)
    {
        await RunOkAsync($"kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic {topic} --partitions 3 --replication-factor 1");
        await RunOkAsync($"seq 1 {Records} | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic {topic}");
    }

    /// <summary>
    /// Consumes until <see cref="Records"/> records arrived: the consumer stops on the last one. The timeout only
    /// bounds a consumer that never gets them all; it then exits with what it got.
    /// </summary>
    private async Task<int> ConsumeAsync(string topic, string group, string options)
    {
        var result = await fixture.RunAsync(
            $"kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic {topic} --group {group} --from-beginning " +
            $"--max-messages {Records} --timeout-ms 60000 {options}",
            TestContext.Current.CancellationToken);
        return result.Stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries).Length;
    }

    private async Task<string[][]> DescribeGroupAsync(string group)
    {
        var result = await RunOkAsync($"kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group {group}");
        // GROUP, TOPIC, PARTITION, CURRENT-OFFSET, LOG-END-OFFSET, LAG, ...
        return [.. Columns(result.Stdout).Where(c => c.Length >= 6 && c[0] == group)];
    }

    /// <summary>Every partition of the topic shows a committed offset, the offsets add up to all records, no lag.</summary>
    private static void AssertCommittedEverything(string[][] rows, string topic)
    {
        Assert.Equal(3, rows.Length);
        Assert.All(rows, r =>
        {
            Assert.Equal(topic, r[1]);
            Assert.Equal("0", r[5]); // LAG
        });
        Assert.Equal(Records, rows.Sum(r => long.Parse(r[3], System.Globalization.CultureInfo.InvariantCulture))); // CURRENT-OFFSET
    }

    /// <summary>The tools' tables, one row per line; columns are padded with spaces, tabs or both.</summary>
    private static IEnumerable<string[]> Columns(string output) =>
        output.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => l.Split([' ', '\t', '\r'], StringSplitOptions.RemoveEmptyEntries))
            .Where(c => c.Length > 0);

    private async Task<ExecResult> RunOkAsync(string command)
    {
        var result = await fixture.RunAsync(command, TestContext.Current.CancellationToken);
        Assert.True(result.ExitCode == 0, $"`{command}` exited with {result.ExitCode}:\n{result.Stderr}");
        return result;
    }
}
