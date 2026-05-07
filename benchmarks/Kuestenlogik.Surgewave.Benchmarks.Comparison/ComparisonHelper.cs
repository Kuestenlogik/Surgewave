using System.Diagnostics;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.Performance;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison;

/// <summary>
/// Shared benchmark execution helpers for all SurgewaveVs* comparison benchmarks.
/// </summary>
public static class ComparisonHelper
{
    public static async Task<BenchmarkResult> RunKafkaBenchmarkAsync(
        string bootstrapServers, int messageCount, int messageSize, byte[] messageValue, string prefix)
    {
        var topicName = $"{prefix}-bench-{Guid.NewGuid():N}";

        // Producer test
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            Acks = Acks.Leader,
            LingerMs = 5,
            BatchSize = 65536,
            QueueBufferingMaxMessages = 2000000,
            QueueBufferingMaxKbytes = 2097152,
        };

        double producerMsgPerSec, producerMBPerSec;
        using (var producer = new ProducerBuilder<Null, byte[]>(producerConfig).Build())
        {
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < messageCount; i++)
            {
                producer.Produce(topicName, new Message<Null, byte[]> { Value = messageValue });
            }
            producer.Flush(TimeSpan.FromSeconds(60));
            sw.Stop();

            var ms = sw.ElapsedMilliseconds;
            producerMsgPerSec = messageCount * 1000.0 / ms;
            producerMBPerSec = (long)messageCount * messageSize / 1024.0 / 1024.0 * 1000.0 / ms;
        }

        // Consumer test
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"bench-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
        };

        double consumerMsgPerSec, consumerMBPerSec;
        using (var consumer = new ConsumerBuilder<Null, byte[]>(consumerConfig).Build())
        {
            consumer.Assign([new TopicPartitionOffset(topicName, 0, Offset.Beginning)]);

            var sw = Stopwatch.StartNew();
            var consumed = 0;
            var noMessageCount = 0;

            while (consumed < messageCount && noMessageCount < 3)
            {
                var result = consumer.Consume(TimeSpan.FromSeconds(5));
                if (result == null)
                {
                    noMessageCount++;
                    continue;
                }
                noMessageCount = 0;
                consumed++;
            }
            sw.Stop();

            var ms = sw.ElapsedMilliseconds;
            consumerMsgPerSec = consumed * 1000.0 / ms;
            consumerMBPerSec = (long)consumed * messageSize / 1024.0 / 1024.0 * 1000.0 / ms;
        }

        return new BenchmarkResult(producerMsgPerSec, producerMBPerSec, consumerMsgPerSec, consumerMBPerSec);
    }

    public static async Task<BenchmarkResult> RunNativeBenchmarkAsync(
        string host, int port, int messageCount, int messageSize, byte[] messageValue)
    {
        var topicName = $"native-bench-{Guid.NewGuid():N}";

        await using var client = new SurgewaveNativeClient(host, port);
        await client.ConnectAsync();
        await client.Topics.CreateAsync(topicName, 1);

        // Producer test with batching
        await using var producer = new SurgewaveBatchingProducer(
            client, topicName, 0,
            maxBatchSize: 1000,
            lingerTime: TimeSpan.FromMilliseconds(5));

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < messageCount; i++)
        {
            await producer.ProduceAsync(null, messageValue);
        }
        await producer.FlushAsync();
        sw.Stop();

        var produceMs = sw.ElapsedMilliseconds;
        var producerMsgPerSec = messageCount * 1000.0 / produceMs;
        var producerMBPerSec = (long)messageCount * messageSize / 1024.0 / 1024.0 * 1000.0 / produceMs;

        // Consumer test
        sw.Restart();
        var consumed = 0;
        long offset = 0;

        while (consumed < messageCount)
        {
            var result = await client.Messaging.ReceiveAsync(topicName, 0, offset, 1024 * 1024);
            if (result.Messages.Count == 0)
            {
                await Task.Delay(10);
                continue;
            }
            consumed += result.Messages.Count;
            offset = result.Messages[^1].Offset + 1;
        }
        sw.Stop();

        var consumeMs = sw.ElapsedMilliseconds;
        var consumerMsgPerSec = consumed * 1000.0 / consumeMs;
        var consumerMBPerSec = (long)consumed * messageSize / 1024.0 / 1024.0 * 1000.0 / consumeMs;

        return new BenchmarkResult(producerMsgPerSec, producerMBPerSec, consumerMsgPerSec, consumerMBPerSec);
    }
}

/// <summary>Results from a single benchmark run.</summary>
public sealed record BenchmarkResult(
    double ProducerMsgPerSec,
    double ProducerMBPerSec,
    double ConsumerMsgPerSec,
    double ConsumerMBPerSec);
