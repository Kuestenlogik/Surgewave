using System.Diagnostics;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Kuestenlogik.Surgewave.Testing.Chaos;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Benchmarks.RealWorld.Scenarios;

/// <summary>
/// Measures performance impact during broker failure and recovery.
/// Uses a 3-broker cluster with chaos engine: continuously produces/consumes,
/// crashes one broker mid-flight, and measures throughput during and after failure.
/// </summary>
public sealed class FailoverScenario
{
    private readonly BenchmarkConfig _config;

    public FailoverScenario(BenchmarkConfig config)
    {
        _config = config;
    }

    /// <summary>
    /// Runs a continuous produce workload, injects a broker crash after 30% of messages,
    /// then recovers the broker after 60% of messages. Measures throughput in each phase
    /// and reports recovery timeline.
    /// </summary>
    public async Task<BenchmarkResult> RunAsync()
    {
        AnsiConsole.MarkupLine("[bold cyan]Failover Scenario[/]");
        AnsiConsole.MarkupLine($"  Brokers: {_config.BrokerCount}, Messages: {_config.MessageCount:N0}, Size: {_config.MessageSizeBytes}B");

        var overallSw = Stopwatch.StartNew();
        var metrics = new Dictionary<string, double>();

        // Must have at least 2 brokers for failover to make sense
        var brokerCount = Math.Max(2, _config.BrokerCount);

        await using var cluster = await ClusterSetup.CreateAsync(
            brokerCount,
            StorageEngines.Memory,
            partitions: 1,
            enableChaos: true);

        var topicName = "bench-failover";
        await cluster.CreateTopicAsync(topicName, 1);

        var messageValue = new byte[_config.MessageSizeBytes];
        Random.Shared.NextBytes(messageValue);

        // We produce to broker 0 (which does not get crashed)
        await using var client = await cluster.GetClientAsync(0);
        await using var producer = new SurgewaveBatchingProducer(
            client,
            topicName,
            partition: 0,
            maxBatchSize: _config.BatchSize,
            lingerTime: TimeSpan.FromMilliseconds(5));

        var crashBrokerId = brokerCount - 1; // Crash the last broker
        var crashPoint = _config.MessageCount * 30 / 100;  // 30%
        var recoverPoint = _config.MessageCount * 60 / 100; // 60%

        var phaseBeforeSw = Stopwatch.StartNew();
        var phaseDuringSw = new Stopwatch();
        var phaseAfterSw = new Stopwatch();

        var crashTimestamp = DateTimeOffset.MinValue;
        var recoverTimestamp = DateTimeOffset.MinValue;
        var producedBefore = 0;
        var producedDuring = 0;
        var producedAfter = 0;
        var errors = 0;

        var chaosEngine = cluster.GetChaosEngine(crashBrokerId);
        using var crashScenario = BrokerCrashScenario.Create(chaosEngine, crashBrokerId);
        // Immediately recover - we'll re-crash at the designated point
        crashScenario.Recover();

        for (int i = 0; i < _config.MessageCount; i++)
        {
            // Phase transitions
            if (i == crashPoint)
            {
                phaseBeforeSw.Stop();
                phaseDuringSw.Start();
                crashTimestamp = DateTimeOffset.UtcNow;

                AnsiConsole.MarkupLine($"    [yellow]Crashing broker {crashBrokerId} at message {i:N0}...[/]");
                chaosEngine.ActivateFault(FaultType.NodeCrash, new FaultScope { BrokerId = crashBrokerId });
            }

            if (i == recoverPoint && crashTimestamp != DateTimeOffset.MinValue)
            {
                phaseDuringSw.Stop();
                phaseAfterSw.Start();
                recoverTimestamp = DateTimeOffset.UtcNow;

                AnsiConsole.MarkupLine($"    [green]Recovering broker {crashBrokerId} at message {i:N0}...[/]");
                chaosEngine.DeactivateAll();
            }

            try
            {
                await producer.ProduceAsync(null, messageValue);

                if (i < crashPoint)
                    producedBefore++;
                else if (i < recoverPoint)
                    producedDuring++;
                else
                    producedAfter++;
            }
            catch
            {
                errors++;
            }
        }

        await producer.FlushAsync();

        // Stop any still-running timers
        phaseBeforeSw.Stop();
        phaseDuringSw.Stop();
        phaseAfterSw.Stop();

        // Compute throughput per phase
        var beforeMs = Math.Max(1, phaseBeforeSw.ElapsedMilliseconds);
        var duringMs = Math.Max(1, phaseDuringSw.ElapsedMilliseconds);
        var afterMs = Math.Max(1, phaseAfterSw.ElapsedMilliseconds);

        var beforeThroughput = producedBefore * 1000.0 / beforeMs;
        var duringThroughput = producedDuring * 1000.0 / duringMs;
        var afterThroughput = producedAfter * 1000.0 / afterMs;

        metrics["before_crash_throughput_msg_sec"] = beforeThroughput;
        metrics["during_crash_throughput_msg_sec"] = duringThroughput;
        metrics["after_recovery_throughput_msg_sec"] = afterThroughput;
        metrics["throughput_impact_pct"] = beforeThroughput > 0 ? (1.0 - duringThroughput / beforeThroughput) * 100 : 0;
        metrics["recovery_time_ms"] = recoverTimestamp != DateTimeOffset.MinValue && crashTimestamp != DateTimeOffset.MinValue
            ? (recoverTimestamp - crashTimestamp).TotalMilliseconds
            : 0;
        metrics["errors_during_failover"] = errors;
        metrics["total_messages_produced"] = producedBefore + producedDuring + producedAfter;

        AnsiConsole.MarkupLine($"  [green]Before crash:   {beforeThroughput:N0} msg/sec ({producedBefore:N0} messages)[/]");
        AnsiConsole.MarkupLine($"  [yellow]During crash:   {duringThroughput:N0} msg/sec ({producedDuring:N0} messages)[/]");
        AnsiConsole.MarkupLine($"  [green]After recovery: {afterThroughput:N0} msg/sec ({producedAfter:N0} messages)[/]");
        AnsiConsole.MarkupLine($"  [dim]Errors: {errors}, Impact: {metrics["throughput_impact_pct"]:F1}%[/]");

        overallSw.Stop();

        return new BenchmarkResult
        {
            Scenario = "failover",
            Description = $"Failover impact with {brokerCount} broker(s), crash broker {crashBrokerId}, {_config.MessageCount:N0} messages x {_config.MessageSizeBytes}B",
            Metrics = metrics,
            Duration = overallSw.Elapsed
        };
    }
}
