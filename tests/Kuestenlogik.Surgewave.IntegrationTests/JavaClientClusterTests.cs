using DotNet.Testcontainers.Containers;
using Kuestenlogik.Surgewave.Coordination.Coordinators;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// The coordinator slices across three brokers with a Java 4.x client — Apache Kafka's own tools (#220): one group
/// coordinator for consumers bootstrapped at different brokers (#204), offsets that outlive their coordinator (#205),
/// and a transaction across partitions led by different brokers (#197). The .NET client shows the same in
/// <c>GroupCoordinatorClusterTests</c> and <c>TransactionCoordinatorClusterTests</c>. A test that stops a broker is in
/// <see cref="JavaClientClusterFailoverTests"/>, with a cluster of its own.
/// </summary>
[Collection(nameof(BrokerSpawningCollection))]
[Trait("Category", TestCategories.Integration)]
public sealed class JavaClientClusterTests(JavaClusterFixture fixture, ITestOutputHelper output) : IClassFixture<JavaClusterFixture>
{
    private const int Records = 300;

    [Fact(Timeout = 240_000)]
    public async Task TwoConsumersBootstrappedAtDifferentBrokersShareOneGroup()
    {
        var cluster = fixture.ClusterOrSkip();
        var (topic, group) = Names("share");
        var running = cluster.Running.ToList();
        var (first, second) = (running[0], running[1]);
        await RunOkAsync($"kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic {topic} --partitions 3 --replication-factor 3", first);

        // Both join before anything is produced, so neither takes every record before the other arrives.
        var consumers = new[] { first, second }.Select(b => fixture.RunAsync(
            $"kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic {topic} --group {group} --timeout-ms 20000",
            b, TestContext.Current.CancellationToken)).ToArray();
        Assert.True(await TestWaitHelpers.WaitForConditionAsync(
                async () => (await MembersAsync(group, first)) == 2, timeout: TimeSpan.FromSeconds(90), output: output),
            "the two consumers did not meet in one group");

        // Keyed, so the records spread over the partitions: without keys the producer puts them all into one.
        await RunOkAsync(
            $"seq 1 {Records} | sed 's/.*/&:&/' | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic {topic} " +
            "--property parse.key=true --property key.separator=:", second);
        var consumed = (await Task.WhenAll(consumers)).Select(r => Lines(r.Stdout)).ToArray();

        output.WriteLine($"consumed {consumed[0]} and {consumed[1]}");
        Assert.All(consumed, c => Assert.True(c > 0, "a consumer of the group got no partition"));
        Assert.Equal(Records, consumed.Sum());
    }

    [Fact(Timeout = 240_000)]
    public async Task ATransactionAcrossPartitionsLedByDifferentBrokersCommits()
    {
        var cluster = fixture.ClusterOrSkip();
        var (topic, group) = Names("txn");
        var transactionalId = $"{topic}-tx";
        var bootstrap = cluster.Running.First();
        await RunOkAsync($"kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic {topic} --partitions 3 --replication-factor 3", bootstrap);
        Assert.True(await TestWaitHelpers.WaitForConditionAsync(
            () => Enumerable.Range(0, 3).All(p => CoordinatorCluster.LeaderOf(bootstrap, topic, p) > 0), timeout: TimeSpan.FromSeconds(30), output: output));
        var leaders = Enumerable.Range(0, 3).Select(p => CoordinatorCluster.LeaderOf(bootstrap, topic, p)!.Value).Distinct().ToList();
        output.WriteLine($"{transactionalId} is coordinated by broker {cluster.CoordinatorOf(CoordinatorKind.Transaction, transactionalId).BrokerId}; {topic} is led by {string.Join(",", leaders)}");
        Assert.True(leaders.Count > 1, "the topic's partitions are all led by one broker");

        await RunOkAsync(
            $"kafka-producer-perf-test.sh --topic {topic} --num-records {Records} --record-size 100 --throughput -1 " +
            $"--transaction-duration-ms 200 --producer-props bootstrap.servers=$BOOTSTRAP transactional.id={transactionalId}",
            bootstrap);
        var consumed = await fixture.RunAsync(
            $"kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic {topic} --group {group} --from-beginning " +
            $"--max-messages {Records} --timeout-ms 60000 --isolation-level read_committed",
            cluster.Running.Last(), TestContext.Current.CancellationToken);

        Assert.Equal(Records, Lines(consumed.Stdout));
        var transactions = await RunOkAsync("kafka-transactions.sh --bootstrap-server $BOOTSTRAP list", bootstrap);
        // TransactionalId, Coordinator, ProducerId, TransactionState
        var row = Assert.Single(Columns(transactions.Stdout), c => c[0] == transactionalId);
        Assert.Equal("CompleteCommit", row[3]);
    }

    private static (string Topic, string Group) Names(string scenario)
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        return ($"java-cluster-{scenario}-{suffix}", $"g-{scenario}-{suffix}");
    }

    private async Task<int> MembersAsync(string group, SurgewaveRuntime bootstrap)
    {
        var result = await fixture.RunAsync(
            $"kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group {group} --members", bootstrap, TestContext.Current.CancellationToken);
        // GROUP, CONSUMER-ID, HOST, CLIENT-ID, #PARTITIONS
        return Columns(result.Stdout).Count(c => c.Length >= 5 && c[0] == group);
    }

    // The sum of the group's committed offsets; a partition without one counts 0.
    private async Task<long> CommittedAsync(string group, SurgewaveRuntime bootstrap)
    {
        var result = await fixture.RunAsync(
            $"kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group {group}", bootstrap, TestContext.Current.CancellationToken);
        // GROUP, TOPIC, PARTITION, CURRENT-OFFSET, LOG-END-OFFSET, LAG, ...
        return Columns(result.Stdout)
            .Where(c => c.Length >= 6 && c[0] == group)
            .Sum(c => long.TryParse(c[3], System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var o) ? o : 0);
    }

    private static int Lines(string output) => output.Split('\n', StringSplitOptions.RemoveEmptyEntries).Length;

    /// <summary>The tools' tables, one row per line; columns are padded with spaces, tabs or both.</summary>
    private static IEnumerable<string[]> Columns(string output) =>
        output.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => l.Split([' ', '\t', '\r'], StringSplitOptions.RemoveEmptyEntries))
            .Where(c => c.Length > 0);

    private async Task<ExecResult> RunOkAsync(string command, SurgewaveRuntime bootstrap)
    {
        var result = await fixture.RunAsync(command, bootstrap, TestContext.Current.CancellationToken);
        Assert.True(result.ExitCode == 0, $"`{command}` exited with {result.ExitCode}:\n{result.Stderr}");
        return result;
    }
}

/// <summary>
/// The group slice of <see cref="JavaClientClusterTests"/> that stops the group's coordinator (#205, #224): a cluster of
/// its own, so no other test meets a stopped broker.
/// </summary>
[Collection(nameof(BrokerSpawningCollection))]
[Trait("Category", TestCategories.Integration)]
public sealed class JavaClientClusterFailoverTests(JavaClusterFixture fixture, ITestOutputHelper output) : IClassFixture<JavaClusterFixture>
{
    private const int Records = 300;

    [Fact(Timeout = 300_000)]
    public async Task AGroupResumesFromItsCommittedOffsetsAfterItsCoordinatorIsStopped()
    {
        var cluster = fixture.ClusterOrSkip();
        var (topic, group) = Names("resume");
        var coordinator = cluster.CoordinatorOf(CoordinatorKind.Group, group);
        var survivor = cluster.Running.First(b => b != coordinator);
        output.WriteLine($"{group} is coordinated by broker {coordinator.BrokerId}; the client uses broker {survivor.BrokerId}");
        await RunOkAsync($"kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic {topic} --partitions 3 --replication-factor 3", survivor);
        await RunOkAsync($"seq 1 {Records} | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic {topic}", survivor);

        await RunOkAsync(
            $"kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic {topic} --group {group} --from-beginning --max-messages {Records / 2} --timeout-ms 60000",
            survivor);
        var committed = await CommittedAsync(group, survivor);
        Assert.InRange(committed, 1, Records);

        await cluster.StopAsync(coordinator);

        // Another broker coordinates the group now, read from __consumer_offsets: the same offsets, and the rest of the
        // records after them.
        Assert.True(await TestWaitHelpers.WaitForConditionAsync(
                async () => await CommittedAsync(group, survivor) == committed, timeout: TimeSpan.FromSeconds(120), output: output),
            "the group's committed offsets did not survive its coordinator");
        var rest = await fixture.RunAsync(
            $"kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic {topic} --group {group} --timeout-ms 20000",
            survivor, TestContext.Current.CancellationToken);
        Assert.Equal(Records - committed, Lines(rest.Stdout));
    }

    private static (string Topic, string Group) Names(string scenario)
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        return ($"java-cluster-{scenario}-{suffix}", $"g-{scenario}-{suffix}");
    }

    private async Task<int> MembersAsync(string group, SurgewaveRuntime bootstrap)
    {
        var result = await fixture.RunAsync(
            $"kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group {group} --members", bootstrap, TestContext.Current.CancellationToken);
        // GROUP, CONSUMER-ID, HOST, CLIENT-ID, #PARTITIONS
        return Columns(result.Stdout).Count(c => c.Length >= 5 && c[0] == group);
    }

    // The sum of the group's committed offsets; a partition without one counts 0.
    private async Task<long> CommittedAsync(string group, SurgewaveRuntime bootstrap)
    {
        var result = await fixture.RunAsync(
            $"kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group {group}", bootstrap, TestContext.Current.CancellationToken);
        // GROUP, TOPIC, PARTITION, CURRENT-OFFSET, LOG-END-OFFSET, LAG, ...
        return Columns(result.Stdout)
            .Where(c => c.Length >= 6 && c[0] == group)
            .Sum(c => long.TryParse(c[3], System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var o) ? o : 0);
    }

    private static int Lines(string output) => output.Split('\n', StringSplitOptions.RemoveEmptyEntries).Length;

    /// <summary>The tools' tables, one row per line; columns are padded with spaces, tabs or both.</summary>
    private static IEnumerable<string[]> Columns(string output) =>
        output.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => l.Split([' ', '\t', '\r'], StringSplitOptions.RemoveEmptyEntries))
            .Where(c => c.Length > 0);

    private async Task<ExecResult> RunOkAsync(string command, SurgewaveRuntime bootstrap)
    {
        var result = await fixture.RunAsync(command, bootstrap, TestContext.Current.CancellationToken);
        Assert.True(result.ExitCode == 0, $"`{command}` exited with {result.ExitCode}:\n{result.Stderr}");
        return result;
    }
}
