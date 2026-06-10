using System.Diagnostics;
using Confluent.Kafka;
using HdrHistogram;
using Kuestenlogik.Surgewave.Benchmarks.Public.Scenarios;
using Kuestenlogik.Surgewave.Benchmarks.Public.Sut;
using Kuestenlogik.Surgewave.Client.Native;

namespace Kuestenlogik.Surgewave.Benchmarks.Public.Runners;

/// <summary>
/// End-to-end produce-to-consume latency capture. One producer + one
/// consumer per SUT, single-record waves with a sub-millisecond pause
/// between sends so the broker is not throughput-saturated — the goal
/// is the latency distribution at moderate load, not the throughput
/// ceiling. Each round-trip is recorded into an HdrHistogram with
/// microsecond resolution; P50…P99.99 read off the histogram.
/// </summary>
internal static class LatencyRunner
{
    public static async Task<ScenarioResult> RunAsync(
        IBrokerSut sut,
        PublicBenchmarkOptions options,
        CancellationToken ct)
    {
        var payload = new byte[options.PayloadBytes];
        Random.Shared.NextBytes(payload);

        var histogram = new LongHistogram(
            highestTrackableValue: TimeStamp.Seconds(60),
            numberOfSignificantValueDigits: 3);

        var wall = Stopwatch.StartNew();
        if (sut.SupportsNative && sut.NativeEndpoint is { } endpoint)
        {
            await RunNativeAsync(endpoint.Host, endpoint.Port, payload, options, histogram, ct).ConfigureAwait(false);
        }
        else
        {
            await RunKafkaWireAsync(sut.BootstrapServers, payload, options, histogram, ct).ConfigureAwait(false);
        }
        wall.Stop();

        var totalMs = Math.Max(1, wall.ElapsedMilliseconds);
        var throughput = options.MessageCount * 1000.0 / totalMs;
        var mb = options.MessageCount * (double)options.PayloadBytes / 1024.0 / 1024.0 * 1000.0 / totalMs;

        return new ScenarioResult(
            System: sut.DisplayName,
            ThroughputMessagesPerSec: throughput,
            ThroughputMegabytesPerSec: mb,
            P50LatencyMs: TicksToMillis(histogram.GetValueAtPercentile(50)),
            P90LatencyMs: TicksToMillis(histogram.GetValueAtPercentile(90)),
            P99LatencyMs: TicksToMillis(histogram.GetValueAtPercentile(99)),
            P999LatencyMs: TicksToMillis(histogram.GetValueAtPercentile(99.9)),
            P9999LatencyMs: TicksToMillis(histogram.GetValueAtPercentile(99.99)),
            MessagesSent: options.MessageCount,
            PayloadBytes: (long)options.MessageCount * options.PayloadBytes,
            WallClock: wall.Elapsed);
    }

    private static async Task RunKafkaWireAsync(
        string bootstrap, byte[] payload, PublicBenchmarkOptions options, LongHistogram histogram, CancellationToken ct)
    {
        var topic = $"latency-{Guid.NewGuid():N}";
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrap,
            Acks = Acks.All,
            LingerMs = 0,
            EnableIdempotence = false,
        };
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrap,
            GroupId = $"latency-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            FetchWaitMaxMs = 10,
        };

        using var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build();
        using var consumer = new ConsumerBuilder<Null, byte[]>(consumerConfig).Build();
        consumer.Assign([new TopicPartitionOffset(topic, 0, Offset.Beginning)]);

        for (var i = 0; i < options.MessageCount; i++)
        {
            ct.ThrowIfCancellationRequested();
            var start = Stopwatch.GetTimestamp();
            await producer.ProduceAsync(topic, new Message<Null, byte[]> { Value = payload }, ct).ConfigureAwait(false);

            ConsumeResult<Null, byte[]>? rec = null;
            while (rec is null)
            {
                ct.ThrowIfCancellationRequested();
                rec = consumer.Consume(TimeSpan.FromSeconds(5));
            }
            var elapsedUs = (long)(Stopwatch.GetElapsedTime(start).TotalMicroseconds);
            histogram.RecordValue(Math.Max(1, elapsedUs));
        }
    }

    private static async Task RunNativeAsync(
        string host, int port, byte[] payload, PublicBenchmarkOptions options, LongHistogram histogram, CancellationToken ct)
    {
        var topic = $"latency-{Guid.NewGuid():N}";
        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync().ConfigureAwait(false);
        await client.Topics.CreateAsync(topic, 1).ConfigureAwait(false);

        long offset = 0;
        for (var i = 0; i < options.MessageCount; i++)
        {
            ct.ThrowIfCancellationRequested();
            var start = Stopwatch.GetTimestamp();
            await client.Messaging.Send(topic)
                .WithValue(payload)
                .ExecuteAsync(ct).ConfigureAwait(false);

            while (true)
            {
                ct.ThrowIfCancellationRequested();
                var result = await client.Messaging.ReceiveAsync(topic, 0, offset, 1024 * 1024).ConfigureAwait(false);
                if (result.Messages.Count > 0)
                {
                    offset = result.Messages[^1].Offset + 1;
                    break;
                }
                await Task.Delay(1, ct).ConfigureAwait(false);
            }
            var elapsedUs = (long)Stopwatch.GetElapsedTime(start).TotalMicroseconds;
            histogram.RecordValue(Math.Max(1, elapsedUs));
        }
    }

    /// <summary>Convert histogram value (microseconds) to milliseconds.</summary>
    private static double TicksToMillis(long microseconds) => microseconds / 1_000.0;
}
