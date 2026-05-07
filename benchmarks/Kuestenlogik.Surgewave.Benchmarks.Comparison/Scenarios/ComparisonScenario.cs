using System.Diagnostics;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Benchmarks.Comparison.Infrastructure;
using Kuestenlogik.Surgewave.Benchmarks.Comparison.Models;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;
using Kuestenlogik.Surgewave.Runtime;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison.Scenarios;

/// <summary>
/// Base class for all comparison benchmark scenarios.
/// Provides common infrastructure for running workloads against all 8 platform configurations.
/// </summary>
public abstract class ComparisonScenario
{
    /// <summary>Scenario display name.</summary>
    public abstract string Name { get; }

    /// <summary>Short description of what this scenario measures.</summary>
    public abstract string Description { get; }

    /// <summary>Runs the Surgewave workload with the given parameters.</summary>
    public abstract Task<ComparisonResult> RunSurgewaveAsync(BenchmarkParams p, CancellationToken ct);

    /// <summary>Runs the Kafka workload with the given parameters.</summary>
    public abstract Task<ComparisonResult> RunKafkaAsync(BenchmarkParams p, CancellationToken ct);

    /// <summary>
    /// Runs all enabled platforms, returning a combined multi-platform report.
    /// </summary>
    public virtual async Task<ComparisonReport> RunAsync(BenchmarkParams p, CancellationToken ct)
    {
        AnsiConsole.Write(new Rule($"[bold cyan]{Name}[/]"));
        AnsiConsole.MarkupLine($"  [dim]{Description}[/]");
        AnsiConsole.WriteLine();

        var results = new List<ComparisonResult>();

        foreach (var platform in Enum.GetValues<BenchmarkPlatform>())
        {
            if (!p.IsPlatformEnabled(platform))
                continue;

            try
            {
                AnsiConsole.MarkupLine($"  [bold {platform.Color()}]{platform.DisplayName()}[/] ...");
                var result = await RunPlatformAsync(platform, p, ct);
                results.Add(result);

                AnsiConsole.MarkupLine($"    Produce: [{platform.Color()}]{result.ProduceThroughputMsgPerSec:N0}[/] msg/s" +
                    (result.ConsumeThroughputMsgPerSec > 0
                        ? $", Consume: [{platform.Color()}]{result.ConsumeThroughputMsgPerSec:N0}[/] msg/s"
                        : ""));
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"    [dim]{platform.DisplayName()} skipped: {ex.Message}[/]");
            }
        }

        AnsiConsole.WriteLine();

        return new ComparisonReport
        {
            ScenarioName = Name,
            Description = Description,
            Results = results
        };
    }

    /// <summary>
    /// Dispatches a benchmark run to the appropriate platform implementation.
    /// </summary>
    protected virtual async Task<ComparisonResult> RunPlatformAsync(
        BenchmarkPlatform platform, BenchmarkParams p, CancellationToken ct)
    {
        return platform switch
        {
            BenchmarkPlatform.SurgewaveEmbeddedNative => await RunSurgewaveEmbeddedNativeAsync(p, ct),
            BenchmarkPlatform.SurgewaveEmbeddedKafka => await RunSurgewaveEmbeddedKafkaAsync(p, ct),
            BenchmarkPlatform.SurgewaveStandaloneNative => await RunSurgewaveStandaloneNativeAsync(p, ct),
            BenchmarkPlatform.SurgewaveStandaloneKafka => await RunSurgewaveStandaloneKafkaAsync(p, ct),
            BenchmarkPlatform.SurgewaveContainerNative => await RunSurgewaveContainerNativeAsync(p, ct),
            BenchmarkPlatform.SurgewaveContainerKafka => await RunSurgewaveContainerKafkaAsync(p, ct),
            BenchmarkPlatform.ApacheKafkaContainer => await RunKafkaContainerAsync(p, ct),
            BenchmarkPlatform.RedpandaContainer => await RunRedpandaContainerAsync(p, ct),
            _ => throw new ArgumentOutOfRangeException(nameof(platform), platform, "Unknown platform")
        };
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Platform implementations
    // ─────────────────────────────────────────────────────────────────────

    /// <summary>1. In-process embedded Surgewave broker with native client.</summary>
    protected virtual Task<ComparisonResult> RunSurgewaveEmbeddedNativeAsync(BenchmarkParams p, CancellationToken ct)
    {
        return RunSurgewaveThroughputAsync(
            p.MessageCount, p.MessageSizeBytes, p.BatchSize, BenchmarkPlatform.SurgewaveEmbeddedNative, ct);
    }

    /// <summary>2. In-process embedded Surgewave broker with Confluent.Kafka client.</summary>
    protected virtual async Task<ComparisonResult> RunSurgewaveEmbeddedKafkaAsync(BenchmarkParams p, CancellationToken ct)
    {
        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .Build()
            .StartAsync();

        var bootstrap = $"{surgewave.Host}:{surgewave.Port}";
        return await RunKafkaThroughputAsync(
            bootstrap, p.MessageCount, p.MessageSizeBytes, p.BatchSize, BenchmarkPlatform.SurgewaveEmbeddedKafka, ct);
    }

    /// <summary>3. External Surgewave process with native client.</summary>
    protected virtual async Task<ComparisonResult> RunSurgewaveStandaloneNativeAsync(BenchmarkParams p, CancellationToken ct)
    {
        if (!await ContainerManager.IsKafkaAvailableAsync(p.SurgewaveStandaloneAddress, TimeSpan.FromSeconds(3)))
            throw new InvalidOperationException($"Surgewave standalone not available at {p.SurgewaveStandaloneAddress}");

        var parts = p.SurgewaveStandaloneAddress.Split(':');
        var host = parts[0];
        var port = int.Parse(parts[1]);

        return await RunNativeClientThroughputAsync(
            host, port, p.MessageCount, p.MessageSizeBytes, p.BatchSize,
            BenchmarkPlatform.SurgewaveStandaloneNative, ct);
    }

    /// <summary>4. External Surgewave process with Confluent.Kafka client.</summary>
    protected virtual async Task<ComparisonResult> RunSurgewaveStandaloneKafkaAsync(BenchmarkParams p, CancellationToken ct)
    {
        if (!await ContainerManager.IsKafkaAvailableAsync(p.SurgewaveStandaloneAddress, TimeSpan.FromSeconds(3)))
            throw new InvalidOperationException($"Surgewave standalone not available at {p.SurgewaveStandaloneAddress}");

        return await RunKafkaThroughputAsync(
            p.SurgewaveStandaloneAddress, p.MessageCount, p.MessageSizeBytes, p.BatchSize,
            BenchmarkPlatform.SurgewaveStandaloneKafka, ct);
    }

    /// <summary>5. Surgewave Docker container with native client.</summary>
    protected virtual async Task<ComparisonResult> RunSurgewaveContainerNativeAsync(BenchmarkParams p, CancellationToken ct)
    {
        var bootstrap = await ContainerManager.GetSurgewaveContainerBootstrapAsync(p.SurgewaveContainerImage);
        var parts = bootstrap.Split(':');
        var host = parts[0];
        var port = int.Parse(parts[1]);

        return await RunNativeClientThroughputAsync(
            host, port, p.MessageCount, p.MessageSizeBytes, p.BatchSize,
            BenchmarkPlatform.SurgewaveContainerNative, ct);
    }

    /// <summary>6. Surgewave Docker container with Confluent.Kafka client.</summary>
    protected virtual async Task<ComparisonResult> RunSurgewaveContainerKafkaAsync(BenchmarkParams p, CancellationToken ct)
    {
        var bootstrap = await ContainerManager.GetSurgewaveContainerBootstrapAsync(p.SurgewaveContainerImage);
        return await RunKafkaThroughputAsync(
            bootstrap, p.MessageCount, p.MessageSizeBytes, p.BatchSize,
            BenchmarkPlatform.SurgewaveContainerKafka, ct);
    }

    /// <summary>7. Apache Kafka Docker container with Confluent.Kafka client.</summary>
    protected virtual async Task<ComparisonResult> RunKafkaContainerAsync(BenchmarkParams p, CancellationToken ct)
    {
        var bootstrap = await ContainerManager.GetOrStartKafkaAsync(p.KafkaBootstrap, p.KafkaContainerImage);
        return await RunKafkaThroughputAsync(
            bootstrap, p.MessageCount, p.MessageSizeBytes, p.BatchSize,
            BenchmarkPlatform.ApacheKafkaContainer, ct);
    }

    /// <summary>8. Redpanda Docker container with Confluent.Kafka client.</summary>
    protected virtual async Task<ComparisonResult> RunRedpandaContainerAsync(BenchmarkParams p, CancellationToken ct)
    {
        var bootstrap = await ContainerManager.GetOrStartRedpandaAsync(image: p.RedpandaContainerImage);
        return await RunKafkaThroughputAsync(
            bootstrap, p.MessageCount, p.MessageSizeBytes, p.BatchSize,
            BenchmarkPlatform.RedpandaContainer, ct);
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Shared helpers for Surgewave benchmarks (embedded, in-process)
    // ─────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Runs a throughput benchmark against an embedded Surgewave broker using the native protocol.
    /// </summary>
    protected static async Task<ComparisonResult> RunSurgewaveThroughputAsync(
        int messageCount, int messageSizeBytes, int batchSize,
        BenchmarkPlatform platformType = BenchmarkPlatform.SurgewaveEmbeddedNative,
        CancellationToken ct = default)
    {
        var payload = new byte[messageSizeBytes];
        Random.Shared.NextBytes(payload);

        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .Build()
            .StartAsync();

        var topicName = $"surgewave-bench-{Guid.NewGuid():N}";

        await using var client = new SurgewaveNativeClient(surgewave.Host, surgewave.Port);
        await client.ConnectAsync();
        await client.Topics.CreateAsync(topicName, 1);

        // Producer throughput
        await using var producer = new SurgewaveBatchingProducer(
            client, topicName, 0,
            maxBatchSize: batchSize,
            lingerTime: TimeSpan.FromMilliseconds(5));

        var produceSw = Stopwatch.StartNew();
        for (int i = 0; i < messageCount; i++)
        {
            await producer.ProduceAsync(null, payload);
        }
        await producer.FlushAsync();
        produceSw.Stop();

        var produceMs = Math.Max(1, produceSw.ElapsedMilliseconds);
        var produceMsgPerSec = messageCount * 1000.0 / produceMs;
        var produceMBPerSec = (long)messageCount * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / produceMs;

        // Consumer throughput
        var consumeSw = Stopwatch.StartNew();
        var consumed = 0;
        long offset = 0;

        while (consumed < messageCount)
        {
            ct.ThrowIfCancellationRequested();
            var result = await client.Messaging.ReceiveAsync(topicName, 0, offset, 1024 * 1024);
            if (result.Messages.Count == 0)
            {
                await Task.Delay(10, ct);
                continue;
            }
            consumed += result.Messages.Count;
            offset = result.Messages[^1].Offset + 1;
        }
        consumeSw.Stop();

        var consumeMs = Math.Max(1, consumeSw.ElapsedMilliseconds);
        var consumeMsgPerSec = consumed * 1000.0 / consumeMs;
        var consumeMBPerSec = (long)consumed * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / consumeMs;

        return new ComparisonResult
        {
            Platform = platformType.DisplayName(),
            PlatformType = platformType,
            ProduceThroughputMsgPerSec = produceMsgPerSec,
            ProduceThroughputMbPerSec = produceMBPerSec,
            ConsumeThroughputMsgPerSec = consumeMsgPerSec,
            ConsumeThroughputMbPerSec = consumeMBPerSec,
            TotalBytesProduced = (long)messageCount * messageSizeBytes,
            Duration = produceSw.Elapsed + consumeSw.Elapsed
        };
    }

    /// <summary>
    /// Runs a throughput benchmark against a remote Surgewave broker using the native protocol.
    /// </summary>
    protected static async Task<ComparisonResult> RunNativeClientThroughputAsync(
        string host, int port, int messageCount, int messageSizeBytes, int batchSize,
        BenchmarkPlatform platformType, CancellationToken ct)
    {
        var payload = new byte[messageSizeBytes];
        Random.Shared.NextBytes(payload);
        var topicName = $"surgewave-bench-{Guid.NewGuid():N}";

        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync();
        await client.Topics.CreateAsync(topicName, 1);

        // Producer throughput
        await using var producer = new SurgewaveBatchingProducer(
            client, topicName, 0,
            maxBatchSize: batchSize,
            lingerTime: TimeSpan.FromMilliseconds(5));

        var produceSw = Stopwatch.StartNew();
        for (int i = 0; i < messageCount; i++)
        {
            await producer.ProduceAsync(null, payload);
        }
        await producer.FlushAsync();
        produceSw.Stop();

        var produceMs = Math.Max(1, produceSw.ElapsedMilliseconds);
        var produceMsgPerSec = messageCount * 1000.0 / produceMs;
        var produceMBPerSec = (long)messageCount * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / produceMs;

        // Consumer throughput
        var consumeSw = Stopwatch.StartNew();
        var consumed = 0;
        long offset = 0;

        while (consumed < messageCount)
        {
            ct.ThrowIfCancellationRequested();
            var result = await client.Messaging.ReceiveAsync(topicName, 0, offset, 1024 * 1024);
            if (result.Messages.Count == 0)
            {
                await Task.Delay(10, ct);
                continue;
            }
            consumed += result.Messages.Count;
            offset = result.Messages[^1].Offset + 1;
        }
        consumeSw.Stop();

        var consumeMs = Math.Max(1, consumeSw.ElapsedMilliseconds);
        var consumeMsgPerSec = consumed * 1000.0 / consumeMs;
        var consumeMBPerSec = (long)consumed * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / consumeMs;

        return new ComparisonResult
        {
            Platform = platformType.DisplayName(),
            PlatformType = platformType,
            ProduceThroughputMsgPerSec = produceMsgPerSec,
            ProduceThroughputMbPerSec = produceMBPerSec,
            ConsumeThroughputMsgPerSec = consumeMsgPerSec,
            ConsumeThroughputMbPerSec = consumeMBPerSec,
            TotalBytesProduced = (long)messageCount * messageSizeBytes,
            Duration = produceSw.Elapsed + consumeSw.Elapsed
        };
    }

    /// <summary>
    /// Runs a latency benchmark against an embedded Surgewave broker using the native protocol.
    /// </summary>
    protected static async Task<ComparisonResult> RunSurgewaveLatencyAsync(
        int messageCount, int messageSizeBytes,
        BenchmarkPlatform platformType = BenchmarkPlatform.SurgewaveEmbeddedNative,
        CancellationToken ct = default)
    {
        var payload = new byte[messageSizeBytes];
        Random.Shared.NextBytes(payload);
        var tpm = Stopwatch.Frequency / 1_000.0; // ticks per millisecond

        await using var surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithAutoCreateTopics(true)
            .WithPartitions(1)
            .Build()
            .StartAsync();

        var topicName = $"surgewave-lat-{Guid.NewGuid():N}";

        await using var client = new SurgewaveNativeClient(surgewave.Host, surgewave.Port);
        await client.ConnectAsync();
        await client.Topics.CreateAsync(topicName, 1);

        // Warmup
        await using (var warmupProducer = new SurgewaveBatchingProducer(client, topicName, 0, maxBatchSize: 1, lingerTime: TimeSpan.Zero))
        {
            for (int i = 0; i < Math.Min(500, messageCount / 10); i++)
            {
                await warmupProducer.ProduceAsync(null, payload);
                await warmupProducer.FlushAsync();
            }
        }

        // Produce latency (single message, no batching)
        var produceLatencies = new double[messageCount];
        await using (var producer = new SurgewaveBatchingProducer(client, topicName, 0, maxBatchSize: 1, lingerTime: TimeSpan.Zero))
        {
            var sw = new Stopwatch();
            for (int i = 0; i < messageCount; i++)
            {
                ct.ThrowIfCancellationRequested();
                sw.Restart();
                await producer.ProduceAsync(null, payload);
                await producer.FlushAsync();
                sw.Stop();
                produceLatencies[i] = sw.ElapsedTicks / tpm;
            }
        }

        // Consume latency
        var consumeLatencies = new double[messageCount];
        await using var consumer = new SurgewavePrefetchingConsumer(
            client, topicName, 0, 0, messageCount + 1000, 1024 * 1024);
        await Task.Delay(200, ct);

        var sw2 = new Stopwatch();
        for (int i = 0; i < messageCount; i++)
        {
            ct.ThrowIfCancellationRequested();
            sw2.Restart();
            var msg = consumer.Consume() ?? await consumer.ConsumeAsync();
            sw2.Stop();
            consumeLatencies[i] = sw2.ElapsedTicks / tpm;
        }

        Array.Sort(produceLatencies);
        Array.Sort(consumeLatencies);

        // Also measure throughput for complete result
        var totalProduceMs = produceLatencies.Sum();
        var totalConsumeMs = consumeLatencies.Sum();

        return new ComparisonResult
        {
            Platform = platformType.DisplayName(),
            PlatformType = platformType,
            ProduceThroughputMsgPerSec = messageCount / (totalProduceMs / 1000.0),
            ProduceThroughputMbPerSec = (long)messageCount * messageSizeBytes / 1024.0 / 1024.0 / (totalProduceMs / 1000.0),
            ConsumeThroughputMsgPerSec = messageCount / (totalConsumeMs / 1000.0),
            ConsumeThroughputMbPerSec = (long)messageCount * messageSizeBytes / 1024.0 / 1024.0 / (totalConsumeMs / 1000.0),
            ProduceLatencyP50Ms = GetPercentile(produceLatencies, 50),
            ProduceLatencyP90Ms = GetPercentile(produceLatencies, 90),
            ProduceLatencyP99Ms = GetPercentile(produceLatencies, 99),
            ConsumeLatencyP50Ms = GetPercentile(consumeLatencies, 50),
            ConsumeLatencyP90Ms = GetPercentile(consumeLatencies, 90),
            ConsumeLatencyP99Ms = GetPercentile(consumeLatencies, 99),
            TotalBytesProduced = (long)messageCount * messageSizeBytes,
            Duration = TimeSpan.FromMilliseconds(totalProduceMs + totalConsumeMs)
        };
    }

    /// <summary>
    /// Runs a latency benchmark against a remote Surgewave broker using the native protocol.
    /// </summary>
    protected static async Task<ComparisonResult> RunNativeClientLatencyAsync(
        string host, int port, int messageCount, int messageSizeBytes,
        BenchmarkPlatform platformType, CancellationToken ct)
    {
        var payload = new byte[messageSizeBytes];
        Random.Shared.NextBytes(payload);
        var tpm = Stopwatch.Frequency / 1_000.0;
        var topicName = $"surgewave-lat-{Guid.NewGuid():N}";

        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync();
        await client.Topics.CreateAsync(topicName, 1);

        // Warmup
        await using (var warmupProducer = new SurgewaveBatchingProducer(client, topicName, 0, maxBatchSize: 1, lingerTime: TimeSpan.Zero))
        {
            for (int i = 0; i < Math.Min(500, messageCount / 10); i++)
            {
                await warmupProducer.ProduceAsync(null, payload);
                await warmupProducer.FlushAsync();
            }
        }

        var produceLatencies = new double[messageCount];
        await using (var producer = new SurgewaveBatchingProducer(client, topicName, 0, maxBatchSize: 1, lingerTime: TimeSpan.Zero))
        {
            var sw = new Stopwatch();
            for (int i = 0; i < messageCount; i++)
            {
                ct.ThrowIfCancellationRequested();
                sw.Restart();
                await producer.ProduceAsync(null, payload);
                await producer.FlushAsync();
                sw.Stop();
                produceLatencies[i] = sw.ElapsedTicks / tpm;
            }
        }

        var consumeLatencies = new double[messageCount];
        await using var consumer = new SurgewavePrefetchingConsumer(
            client, topicName, 0, 0, messageCount + 1000, 1024 * 1024);
        await Task.Delay(200, ct);

        var sw2 = new Stopwatch();
        for (int i = 0; i < messageCount; i++)
        {
            ct.ThrowIfCancellationRequested();
            sw2.Restart();
            var msg = consumer.Consume() ?? await consumer.ConsumeAsync();
            sw2.Stop();
            consumeLatencies[i] = sw2.ElapsedTicks / tpm;
        }

        Array.Sort(produceLatencies);
        Array.Sort(consumeLatencies);

        var totalProduceMs = produceLatencies.Sum();
        var totalConsumeMs = consumeLatencies.Sum();

        return new ComparisonResult
        {
            Platform = platformType.DisplayName(),
            PlatformType = platformType,
            ProduceThroughputMsgPerSec = messageCount / (totalProduceMs / 1000.0),
            ProduceThroughputMbPerSec = (long)messageCount * messageSizeBytes / 1024.0 / 1024.0 / (totalProduceMs / 1000.0),
            ConsumeThroughputMsgPerSec = messageCount / (totalConsumeMs / 1000.0),
            ConsumeThroughputMbPerSec = (long)messageCount * messageSizeBytes / 1024.0 / 1024.0 / (totalConsumeMs / 1000.0),
            ProduceLatencyP50Ms = GetPercentile(produceLatencies, 50),
            ProduceLatencyP90Ms = GetPercentile(produceLatencies, 90),
            ProduceLatencyP99Ms = GetPercentile(produceLatencies, 99),
            ConsumeLatencyP50Ms = GetPercentile(consumeLatencies, 50),
            ConsumeLatencyP90Ms = GetPercentile(consumeLatencies, 90),
            ConsumeLatencyP99Ms = GetPercentile(consumeLatencies, 99),
            TotalBytesProduced = (long)messageCount * messageSizeBytes,
            Duration = TimeSpan.FromMilliseconds(totalProduceMs + totalConsumeMs)
        };
    }

    // ─────────────────────────────────────────────────────────────────────
    //  Shared helpers for Kafka-protocol benchmarks
    // ─────────────────────────────────────────────────────────────────────

    /// <summary>
    /// Runs a throughput benchmark using the Confluent.Kafka client against any Kafka-compatible broker.
    /// </summary>
    protected static async Task<ComparisonResult> RunKafkaThroughputAsync(
        string bootstrapServers, int messageCount, int messageSizeBytes, int batchSize,
        BenchmarkPlatform platformType = BenchmarkPlatform.ApacheKafkaContainer,
        CancellationToken ct = default)
    {
        var payload = new byte[messageSizeBytes];
        Random.Shared.NextBytes(payload);
        var topicName = $"kafka-bench-{Guid.NewGuid():N}";

        // Producer throughput
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 5,
            BatchSize = 65536,
            QueueBufferingMaxMessages = 2000000,
            QueueBufferingMaxKbytes = 2097152,
        };

        var produceSw = Stopwatch.StartNew();
        using (var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build())
        {
            for (int i = 0; i < messageCount; i++)
            {
                producer.Produce(topicName, new Message<Null, byte[]> { Value = payload });
            }
            producer.Flush(TimeSpan.FromSeconds(60));
        }
        produceSw.Stop();

        var produceMs = Math.Max(1, produceSw.ElapsedMilliseconds);
        var produceMsgPerSec = messageCount * 1000.0 / produceMs;
        var produceMBPerSec = (long)messageCount * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / produceMs;

        // Consumer throughput
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"bench-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        var consumeSw = Stopwatch.StartNew();
        var consumed = 0;
        using (var consumer = new ConsumerBuilder<Null, byte[]>(consumerConfig).Build())
        {
            consumer.Assign([new TopicPartitionOffset(topicName, 0, Offset.Beginning)]);
            var noMessageCount = 0;

            while (consumed < messageCount && noMessageCount < 3)
            {
                ct.ThrowIfCancellationRequested();
                var result = consumer.Consume(TimeSpan.FromSeconds(5));
                if (result == null)
                {
                    noMessageCount++;
                    continue;
                }
                noMessageCount = 0;
                consumed++;
            }
        }
        consumeSw.Stop();

        var consumeMs = Math.Max(1, consumeSw.ElapsedMilliseconds);
        var consumeMsgPerSec = consumed * 1000.0 / consumeMs;
        var consumeMBPerSec = (long)consumed * messageSizeBytes / 1024.0 / 1024.0 * 1000.0 / consumeMs;

        return new ComparisonResult
        {
            Platform = platformType.DisplayName(),
            PlatformType = platformType,
            ProduceThroughputMsgPerSec = produceMsgPerSec,
            ProduceThroughputMbPerSec = produceMBPerSec,
            ConsumeThroughputMsgPerSec = consumeMsgPerSec,
            ConsumeThroughputMbPerSec = consumeMBPerSec,
            TotalBytesProduced = (long)messageCount * messageSizeBytes,
            Duration = produceSw.Elapsed + consumeSw.Elapsed
        };
    }

    /// <summary>
    /// Runs a latency benchmark using the Confluent.Kafka client against any Kafka-compatible broker.
    /// </summary>
    protected static async Task<ComparisonResult> RunKafkaLatencyAsync(
        string bootstrapServers, int messageCount, int messageSizeBytes,
        BenchmarkPlatform platformType = BenchmarkPlatform.ApacheKafkaContainer,
        CancellationToken ct = default)
    {
        var payload = new byte[messageSizeBytes];
        Random.Shared.NextBytes(payload);
        var topicName = $"kafka-lat-{Guid.NewGuid():N}";
        var tpm = Stopwatch.Frequency / 1_000.0;

        // Warmup
        var warmupConfig = new ProducerConfig { BootstrapServers = bootstrapServers };
        using (var warmup = new ProducerBuilder<Null, byte[]>(warmupConfig).Build())
        {
            for (int i = 0; i < Math.Min(500, messageCount / 10); i++)
            {
                await warmup.ProduceAsync(topicName, new Message<Null, byte[]> { Value = payload });
            }
            warmup.Flush(TimeSpan.FromSeconds(10));
        }

        // Produce latency (single message, sync ack)
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.All,
            LingerMs = 0,
            BatchSize = 1
        };
        var produceLatencies = new double[messageCount];

        using (var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build())
        {
            var sw = new Stopwatch();
            for (int i = 0; i < messageCount; i++)
            {
                ct.ThrowIfCancellationRequested();
                sw.Restart();
                await producer.ProduceAsync(topicName, new Message<Null, byte[]> { Value = payload });
                sw.Stop();
                produceLatencies[i] = sw.ElapsedTicks / tpm;
            }
            producer.Flush(TimeSpan.FromSeconds(10));
        }

        // Consume latency
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"bench-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var consumeLatencies = new List<double>(messageCount);
        using (var consumer = new ConsumerBuilder<Ignore, byte[]>(consumerConfig).Build())
        {
            consumer.Subscribe(topicName);
            var sw = new Stopwatch();
            var consumed = 0;

            while (consumed < messageCount)
            {
                ct.ThrowIfCancellationRequested();
                sw.Restart();
                var result = consumer.Consume(TimeSpan.FromSeconds(5));
                sw.Stop();
                if (result != null)
                {
                    consumeLatencies.Add(sw.ElapsedTicks / tpm);
                    consumed++;
                }
            }
        }

        Array.Sort(produceLatencies);
        consumeLatencies.Sort();

        var totalProduceMs = produceLatencies.Sum();
        var consumeArr = consumeLatencies.ToArray();
        var totalConsumeMs = consumeArr.Sum();

        return new ComparisonResult
        {
            Platform = platformType.DisplayName(),
            PlatformType = platformType,
            ProduceThroughputMsgPerSec = messageCount / (totalProduceMs / 1000.0),
            ProduceThroughputMbPerSec = (long)messageCount * messageSizeBytes / 1024.0 / 1024.0 / (totalProduceMs / 1000.0),
            ConsumeThroughputMsgPerSec = messageCount / (totalConsumeMs / 1000.0),
            ConsumeThroughputMbPerSec = (long)messageCount * messageSizeBytes / 1024.0 / 1024.0 / (totalConsumeMs / 1000.0),
            ProduceLatencyP50Ms = GetPercentile(produceLatencies, 50),
            ProduceLatencyP90Ms = GetPercentile(produceLatencies, 90),
            ProduceLatencyP99Ms = GetPercentile(produceLatencies, 99),
            ConsumeLatencyP50Ms = GetPercentile(consumeArr, 50),
            ConsumeLatencyP90Ms = GetPercentile(consumeArr, 90),
            ConsumeLatencyP99Ms = GetPercentile(consumeArr, 99),
            TotalBytesProduced = (long)messageCount * messageSizeBytes,
            Duration = TimeSpan.FromMilliseconds(totalProduceMs + totalConsumeMs)
        };
    }

    /// <summary>
    /// Gets a percentile value from a sorted array.
    /// </summary>
    protected static double GetPercentile(double[] sortedData, double percentile)
    {
        if (sortedData.Length == 0) return 0;
        var index = (int)Math.Ceiling(percentile / 100.0 * sortedData.Length) - 1;
        return sortedData[Math.Max(0, Math.Min(index, sortedData.Length - 1))];
    }

    /// <summary>
    /// Gets or starts a Kafka broker for benchmarking, either from Testcontainers or an external address.
    /// </summary>
    protected static async Task<string> GetKafkaBootstrapAsync(BenchmarkParams p)
    {
        return await ContainerManager.GetOrStartKafkaAsync(p.KafkaBootstrap, p.KafkaContainerImage);
    }

    /// <summary>
    /// Gets a bootstrap address for the given platform.
    /// </summary>
    protected static async Task<string> GetBootstrapForPlatformAsync(BenchmarkPlatform platform, BenchmarkParams p)
    {
        return platform switch
        {
            BenchmarkPlatform.SurgewaveStandaloneNative or BenchmarkPlatform.SurgewaveStandaloneKafka =>
                p.SurgewaveStandaloneAddress,
            BenchmarkPlatform.SurgewaveContainerNative or BenchmarkPlatform.SurgewaveContainerKafka =>
                await ContainerManager.GetSurgewaveContainerBootstrapAsync(p.SurgewaveContainerImage),
            BenchmarkPlatform.ApacheKafkaContainer =>
                await ContainerManager.GetOrStartKafkaAsync(p.KafkaBootstrap, p.KafkaContainerImage),
            BenchmarkPlatform.RedpandaContainer =>
                await ContainerManager.GetOrStartRedpandaAsync(image: p.RedpandaContainerImage),
            _ => throw new InvalidOperationException($"Platform {platform} does not use a bootstrap address")
        };
    }
}
