using System.Diagnostics;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Benchmarks.Public.Scenarios;
using Kuestenlogik.Surgewave.Benchmarks.Public.Sut;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;

namespace Kuestenlogik.Surgewave.Benchmarks.Public.Runners;

/// <summary>
/// Producer-then-consumer throughput run. Times producer-side
/// (until Flush returns) and consumer-side (until last message
/// drained) separately, picks the slower of the two as the
/// scenario's reported throughput — that is the actual end-to-end
/// rate a downstream system would see.
///
/// Compression + batch + acks come from <see cref="PublicBenchmarkOptions"/>.
/// </summary>
internal static class ThroughputRunner
{
    public static async Task<ScenarioResult> RunAsync(
        IBrokerSut sut,
        PublicBenchmarkOptions options,
        CancellationToken ct)
    {
        var payload = new byte[options.PayloadBytes];
        Random.Shared.NextBytes(payload);

        if (sut.SupportsNative && sut.NativeEndpoint is { } endpoint)
        {
            return await RunNativeAsync(sut.DisplayName, endpoint.Host, endpoint.Port, payload, options, ct).ConfigureAwait(false);
        }

        return await RunKafkaWireAsync(sut.DisplayName, sut.BootstrapServers, payload, options, ct).ConfigureAwait(false);
    }

    private static async Task<ScenarioResult> RunKafkaWireAsync(
        string displayName, string bootstrap, byte[] payload, PublicBenchmarkOptions options, CancellationToken ct)
    {
        var topic = $"bench-{Guid.NewGuid():N}";
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            Acks = options.Acks == -1 ? Acks.All : (options.Acks == 1 ? Acks.Leader : Acks.None),
            LingerMs = 5,
            BatchSize = options.BatchSize,
            CompressionType = ParseCompression(options.CompressionCodec),
            QueueBufferingMaxMessages = 2_000_000,
            QueueBufferingMaxKbytes = 2_097_152,
        };

        double producerMsgPerSec, producerMBPerSec;
        var wall = Stopwatch.StartNew();
        using (var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build())
        {
            var sw = Stopwatch.StartNew();
            for (var i = 0; i < options.MessageCount; i++)
            {
                ct.ThrowIfCancellationRequested();
                producer.Produce(topic, new Message<Null, byte[]> { Value = payload });
            }
            producer.Flush(TimeSpan.FromMinutes(2));
            sw.Stop();

            (producerMsgPerSec, producerMBPerSec) = Rates(options.MessageCount, options.PayloadBytes, sw.ElapsedMilliseconds);
        }

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = $"bench-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        double consumerMsgPerSec, consumerMBPerSec;
        long consumedBytes;
        using (var consumer = new ConsumerBuilder<Null, byte[]>(consumerConfig).Build())
        {
            consumer.Assign([new TopicPartitionOffset(topic, 0, Offset.Beginning)]);

            var sw = Stopwatch.StartNew();
            var consumed = 0;
            consumedBytes = 0;
            var idle = 0;
            while (consumed < options.MessageCount && idle < 3)
            {
                ct.ThrowIfCancellationRequested();
                var result = consumer.Consume(TimeSpan.FromSeconds(5));
                if (result == null)
                {
                    idle++;
                    continue;
                }
                idle = 0;
                consumed++;
                consumedBytes += result.Message.Value?.LongLength ?? 0;
            }
            sw.Stop();
            (consumerMsgPerSec, consumerMBPerSec) = Rates(consumed, options.PayloadBytes, sw.ElapsedMilliseconds);
        }
        wall.Stop();

        var slowerMsg = Math.Min(producerMsgPerSec, consumerMsgPerSec);
        var slowerMB = Math.Min(producerMBPerSec, consumerMBPerSec);

        return new ScenarioResult(
            System: displayName,
            ThroughputMessagesPerSec: slowerMsg,
            ThroughputMegabytesPerSec: slowerMB,
            P50LatencyMs: null,
            P90LatencyMs: null,
            P99LatencyMs: null,
            P999LatencyMs: null,
            P9999LatencyMs: null,
            MessagesSent: options.MessageCount,
            PayloadBytes: consumedBytes,
            WallClock: wall.Elapsed);
    }

    private static async Task<ScenarioResult> RunNativeAsync(
        string displayName, string host, int port, byte[] payload, PublicBenchmarkOptions options, CancellationToken ct)
    {
        var topic = $"bench-{Guid.NewGuid():N}";
        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync().ConfigureAwait(false);
        await client.Topics.CreateAsync(topic, 1).ConfigureAwait(false);

        var wall = Stopwatch.StartNew();
        double producerMsgPerSec, producerMBPerSec;
        await using (var producer = new SurgewaveBatchingProducer(
            client, topic, 0,
            maxBatchSize: Math.Max(1, options.BatchSize / Math.Max(1, options.PayloadBytes)),
            lingerTime: TimeSpan.FromMilliseconds(5)))
        {
            var sw = Stopwatch.StartNew();
            for (var i = 0; i < options.MessageCount; i++)
            {
                ct.ThrowIfCancellationRequested();
                await producer.ProduceAsync(null, payload, ct).ConfigureAwait(false);
            }
            await producer.FlushAsync().ConfigureAwait(false);
            sw.Stop();
            (producerMsgPerSec, producerMBPerSec) = Rates(options.MessageCount, options.PayloadBytes, sw.ElapsedMilliseconds);
        }

        var consumeSw = Stopwatch.StartNew();
        var consumed = 0;
        long consumedBytes = 0;
        long offset = 0;
        while (consumed < options.MessageCount)
        {
            ct.ThrowIfCancellationRequested();
            var result = await client.Messaging.ReceiveAsync(topic, 0, offset, 1024 * 1024).ConfigureAwait(false);
            if (result.Messages.Count == 0)
            {
                await Task.Delay(10, ct).ConfigureAwait(false);
                continue;
            }
            foreach (var msg in result.Messages)
            {
                consumedBytes += msg.Value?.Length ?? 0;
            }
            consumed += result.Messages.Count;
            offset = result.Messages[^1].Offset + 1;
        }
        consumeSw.Stop();
        var (consumerMsgPerSec, consumerMBPerSec) = Rates(consumed, options.PayloadBytes, consumeSw.ElapsedMilliseconds);
        wall.Stop();

        var slowerMsg = Math.Min(producerMsgPerSec, consumerMsgPerSec);
        var slowerMB = Math.Min(producerMBPerSec, consumerMBPerSec);

        return new ScenarioResult(
            System: displayName,
            ThroughputMessagesPerSec: slowerMsg,
            ThroughputMegabytesPerSec: slowerMB,
            P50LatencyMs: null,
            P90LatencyMs: null,
            P99LatencyMs: null,
            P999LatencyMs: null,
            P9999LatencyMs: null,
            MessagesSent: options.MessageCount,
            PayloadBytes: consumedBytes,
            WallClock: wall.Elapsed);
    }

    private static (double msgPerSec, double mbPerSec) Rates(long count, int payloadBytes, long ms)
    {
        if (ms <= 0) return (0, 0);
        var msg = count * 1000.0 / ms;
        var mb = count * (double)payloadBytes / 1024.0 / 1024.0 * 1000.0 / ms;
        return (msg, mb);
    }

    private static Confluent.Kafka.CompressionType ParseCompression(string codec) => codec.ToLowerInvariant() switch
    {
        "none" => Confluent.Kafka.CompressionType.None,
        "gzip" => Confluent.Kafka.CompressionType.Gzip,
        "snappy" => Confluent.Kafka.CompressionType.Snappy,
        "lz4" => Confluent.Kafka.CompressionType.Lz4,
        "zstd" => Confluent.Kafka.CompressionType.Zstd,
        _ => Confluent.Kafka.CompressionType.Lz4,
    };
}
