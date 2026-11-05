using DotNet.Testcontainers.Containers;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Share groups across three brokers with a Java 4.x share consumer — Apache Kafka's own console share consumer (#228):
/// a share partition is held by its partition's leader, so members bootstrapped at different brokers get each record
/// once between them, and its state is in <c>__share_group_state</c>, so a leader that takes the partition over goes on
/// where the last one stopped. The test that stops a broker is in <see cref="JavaShareConsumerFailoverTests"/>, with a
/// cluster of its own.
/// </summary>
[Collection(nameof(BrokerSpawningCollection))]
[Trait("Category", TestCategories.Integration)]
public sealed class JavaShareConsumerClusterTests(JavaClusterFixture fixture, ITestOutputHelper output) : IClassFixture<JavaClusterFixture>
{
    private const int Records = 300;

    [Fact(Timeout = 240_000)]
    public async Task ShareConsumersBootstrappedAtDifferentBrokersGetEachRecordOnce()
    {
        var cluster = fixture.ClusterOrSkip();
        var share = new ShareClient(fixture, output, TestContext.Current.CancellationToken);
        var (topic, group) = ShareClient.Names("spread");
        var running = cluster.Running.ToList();
        var (first, second) = (running[0], running[1]);
        await share.RunOkAsync($"kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic {topic} --partitions 3 --replication-factor 3", first);

        // Both join before anything is produced.
        var consumers = new[] { first, second }.Select(b => share.ConsumeAsync(topic, group, b)).ToArray();
        await share.WaitForMembersAsync(group, 2, first);

        // Keyed, so the records spread over the partitions and their leaders.
        await share.RunOkAsync(
            $"seq 1 {Records} | sed 's/.*/&:&/' | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic {topic} " +
            "--property parse.key=true --property key.separator=:", second);
        var results = await Task.WhenAll(consumers);
        foreach (var r in results)
            output.WriteLine($"share consumer exited with {r.ExitCode}: {r.Stderr}");
        var consumed = results.Select(ShareClient.Values).ToArray();

        output.WriteLine($"consumed {consumed[0].Count} and {consumed[1].Count}");
        var all = consumed.SelectMany(c => c).ToList();
        Assert.Equal(Records, all.Count);
        Assert.Equal(Enumerable.Range(1, Records), all.Order());
    }
}

/// <summary>
/// The share-group slice of <see cref="JavaShareConsumerClusterTests"/> that stops a partition's leader (#228): a
/// cluster of its own, so no other test meets a stopped broker.
/// </summary>
[Collection(nameof(BrokerSpawningCollection))]
[Trait("Category", TestCategories.Integration)]
public sealed class JavaShareConsumerFailoverTests(JavaClusterFixture fixture, ITestOutputHelper output) : IClassFixture<JavaClusterFixture>
{
    private const int Records = 150;

    [Fact(Timeout = 300_000)]
    public async Task AShareGroupGoesOnWhereItStoppedWhenItsPartitionsLeaderIsStopped()
    {
        var cluster = fixture.ClusterOrSkip();
        var share = new ShareClient(fixture, output, TestContext.Current.CancellationToken);
        var (topic, group) = ShareClient.Names("failover");
        var bootstrap = cluster.Running.First();
        await share.RunOkAsync($"kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic {topic} --partitions 1 --replication-factor 3", bootstrap);
        Assert.True(await TestWaitHelpers.WaitForConditionAsync(
            () => CoordinatorCluster.LeaderOf(bootstrap, topic, 0) > 0, timeout: TimeSpan.FromSeconds(30), output: output));
        var leader = cluster.Brokers.Single(b => b.BrokerId == CoordinatorCluster.LeaderOf(bootstrap, topic, 0));
        var survivor = cluster.Running.First(b => b != leader);
        output.WriteLine($"{topic} is led by broker {leader.BrokerId}; the client uses broker {survivor.BrokerId}");

        // The group acknowledges the first records with the partition's first leader.
        var before = share.ConsumeAsync(topic, group, survivor);
        await share.WaitForMembersAsync(group, 1, survivor);
        await share.RunOkAsync($"seq 1 {Records} | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic {topic}", survivor);
        Assert.Equal(Enumerable.Range(1, Records), ShareClient.Values(await before).Order());

        await cluster.StopAsync(leader);
        Assert.True(await TestWaitHelpers.WaitForConditionAsync(
                () => CoordinatorCluster.LeaderOf(survivor, topic, 0) is { } now && now > 0 && now != leader.BrokerId,
                timeout: TimeSpan.FromSeconds(120), output: output),
            "the partition got no new leader");

        // The new leader reads the share partition from __share_group_state: the acknowledged records are done, and only
        // the new ones are delivered. Started afresh, it would deliver all of them again.
        var after = share.ConsumeAsync(topic, group, survivor);
        await share.WaitForMembersAsync(group, 1, survivor);
        await share.RunOkAsync($"seq {Records + 1} {2 * Records} | kafka-console-producer.sh --bootstrap-server $BOOTSTRAP --topic {topic}", survivor);
        var afterResult = await after;
        output.WriteLine($"led by broker {CoordinatorCluster.LeaderOf(survivor, topic, 0)} now; the share consumer said: {afterResult.Stderr}");
        Assert.Equal(Enumerable.Range(Records + 1, Records), ShareClient.Values(afterResult).Order());
    }
}

// The Kafka tools a share-group test uses.
internal sealed class ShareClient(JavaClusterFixture fixture, ITestOutputHelper output, CancellationToken cancellationToken)
{
    public static (string Topic, string Group) Names(string scenario)
    {
        var suffix = Guid.NewGuid().ToString("N")[..8];
        return ($"java-share-{scenario}-{suffix}", $"sg-{scenario}-{suffix}");
    }

    // A share consumer that prints each record's value and stops once nothing came for a while; it accepts what it got.
    public Task<ExecResult> ConsumeAsync(string topic, string group, SurgewaveRuntime bootstrap) => fixture.RunAsync(
        $"kafka-console-share-consumer.sh --bootstrap-server $BOOTSTRAP --topic {topic} --group {group} --timeout-ms 20000",
        bootstrap, cancellationToken);

    public async Task WaitForMembersAsync(string group, int members, SurgewaveRuntime bootstrap) =>
        Assert.True(await TestWaitHelpers.WaitForConditionAsync(
                async () => (await MembersAsync(group, bootstrap)) == members, timeout: TimeSpan.FromSeconds(90), output: output, ct: cancellationToken),
            $"the share group did not get {members} member(s)");

    public async Task<ExecResult> RunOkAsync(string command, SurgewaveRuntime bootstrap)
    {
        var result = await fixture.RunAsync(command, bootstrap, cancellationToken);
        Assert.True(result.ExitCode == 0, $"`{command}` exited with {result.ExitCode}:\n{result.Stderr}");
        return result;
    }

    // The values a consumer printed, as numbers.
    public static List<int> Values(ExecResult consumed) =>
        [.. consumed.Stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(l => int.TryParse(l, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var v) ? v : -1)
            .Where(v => v > 0)];

    private async Task<int> MembersAsync(string group, SurgewaveRuntime bootstrap)
    {
        var result = await fixture.RunAsync(
            $"kafka-share-groups.sh --bootstrap-server $BOOTSTRAP --describe --group {group} --members", bootstrap, cancellationToken);
        // GROUP, CONSUMER-ID, HOST, CLIENT-ID, ASSIGNMENT
        return result.Stdout.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(l => l.Split([' ', '\t', '\r'], StringSplitOptions.RemoveEmptyEntries))
            .Count(c => c.Length >= 4 && c[0] == group);
    }
}
