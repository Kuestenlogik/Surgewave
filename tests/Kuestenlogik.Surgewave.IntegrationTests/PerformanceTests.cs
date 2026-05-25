using System.Diagnostics;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.IntegrationTests.Fixtures;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Performance and load tests for the Surgewave broker.
/// These tests measure throughput and latency under various conditions.
/// </summary>
[Collection("Broker")]
[Trait("Category", TestCategories.Integration)]
public class PerformanceTests
{
    private readonly ITestOutputHelper _output;

    public PerformanceTests(BrokerFixture fixture, ITestOutputHelper output)
    {
        _ = fixture;
        _output = output;
    }

    /// <summary>
    /// Measure producer throughput with different batch sizes.
    /// </summary>
    [Theory]
    [InlineData(100, 1024)]      // 100 messages, 1KB each
    [InlineData(1000, 1024)]     // 1000 messages, 1KB each
    [InlineData(1000, 10240)]    // 1000 messages, 10KB each
    public async Task ProducerThroughput_WithDifferentBatchSizes(int messageCount, int messageSize)
    {
        var topic = $"perf-producer-{messageCount}-{messageSize}-{Guid.NewGuid():N}";
        var messageValue = new string('X', messageSize);

        var config = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "perf-producer",
            LingerMs = 5,
            BatchSize = 65536,
            Acks = Acks.All
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        var sw = Stopwatch.StartNew();
        var tasks = new List<Task<DeliveryResult<string, string>>>();

        for (int i = 0; i < messageCount; i++)
        {
            tasks.Add(producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"key-{i}",
                Value = messageValue
            }));
        }

        await Task.WhenAll(tasks);
        producer.Flush(TimeSpan.FromSeconds(30));
        sw.Stop();

        var totalBytes = (long)messageCount * messageSize;
        var throughputMsgs = messageCount * 1000.0 / sw.ElapsedMilliseconds;
        var throughputMB = totalBytes / (1024.0 * 1024.0) / (sw.ElapsedMilliseconds / 1000.0);

        _output.WriteLine($"Produced {messageCount} messages ({messageSize} bytes each)");
        _output.WriteLine($"  Time: {sw.ElapsedMilliseconds}ms");
        _output.WriteLine($"  Throughput: {throughputMsgs:F0} msg/sec, {throughputMB:F2} MB/sec");

        Assert.True(tasks.All(t => t.Result.Status == PersistenceStatus.Persisted));
    }

    /// <summary>
    /// Measure consumer throughput.
    /// </summary>
    [Theory]
    [InlineData(500)]
    [InlineData(1000)]
    public async Task ConsumerThroughput(int messageCount)
    {
        var topic = $"perf-consumer-{messageCount}-{Guid.NewGuid():N}";
        var messageSize = 1024;
        var messageValue = new string('X', messageSize);

        // First produce messages
        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "perf-producer-setup"
        };

        using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
        {
            for (int i = 0; i < messageCount; i++)
            {
                await producer.ProduceAsync(topic, new Message<string, string>
                {
                    Key = $"key-{i}",
                    Value = messageValue
                });
            }
            producer.Flush(TimeSpan.FromSeconds(30));
        }

        _output.WriteLine($"Produced {messageCount} messages for consumption test");

        // Measure consumption
        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = $"perf-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
            FetchMinBytes = 1,
            FetchMaxBytes = 52428800 // 50MB
        };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        consumer.Subscribe(topic);

        var sw = Stopwatch.StartNew();
        var consumed = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        try
        {
            while (consumed < messageCount && !cts.Token.IsCancellationRequested)
            {
                var result = consumer.Consume(cts.Token);
                if (result != null && !result.IsPartitionEOF)
                {
                    consumed++;
                }
            }
        }
        catch (OperationCanceledException) { }

        sw.Stop();
        consumer.Close();

        var totalBytes = (long)consumed * messageSize;
        var throughputMsgs = consumed * 1000.0 / sw.ElapsedMilliseconds;
        var throughputMB = totalBytes / (1024.0 * 1024.0) / (sw.ElapsedMilliseconds / 1000.0);

        _output.WriteLine($"Consumed {consumed} messages");
        _output.WriteLine($"  Time: {sw.ElapsedMilliseconds}ms");
        _output.WriteLine($"  Throughput: {throughputMsgs:F0} msg/sec, {throughputMB:F2} MB/sec");

        Assert.Equal(messageCount, consumed);
    }

    /// <summary>
    /// Measure end-to-end latency (produce to consume).
    /// </summary>
    [Fact]
    public async Task EndToEndLatency()
    {
        var topic = $"perf-latency-{Guid.NewGuid():N}";
        var iterations = 100;
        var latencies = new List<long>();

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "latency-producer",
            Acks = Acks.All
        };

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = $"latency-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true
        };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();
        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

        consumer.Subscribe(topic);

        // Warm up
        await producer.ProduceAsync(topic, new Message<string, string> { Key = "warmup", Value = "warmup" });
        producer.Flush(TimeSpan.FromSeconds(5));
        consumer.Consume(TimeSpan.FromSeconds(5));

        // Measure latencies
        for (int i = 0; i < iterations; i++)
        {
            var sw = Stopwatch.StartNew();

            var produceResult = await producer.ProduceAsync(topic, new Message<string, string>
            {
                Key = $"latency-{i}",
                Value = $"Latency test message {i}"
            });

            producer.Flush(TimeSpan.FromSeconds(5));

            var result = consumer.Consume(TimeSpan.FromSeconds(5));

            sw.Stop();

            if (result != null && !result.IsPartitionEOF)
            {
                latencies.Add(sw.ElapsedMilliseconds);
            }
        }

        consumer.Close();

        if (latencies.Count > 0)
        {
            var avgLatency = latencies.Average();
            var minLatency = latencies.Min();
            var maxLatency = latencies.Max();
            var p50 = Percentile(latencies, 50);
            var p95 = Percentile(latencies, 95);
            var p99 = Percentile(latencies, 99);

            _output.WriteLine($"End-to-end latency ({latencies.Count} samples):");
            _output.WriteLine($"  Min: {minLatency}ms, Max: {maxLatency}ms, Avg: {avgLatency:F1}ms");
            _output.WriteLine($"  P50: {p50}ms, P95: {p95}ms, P99: {p99}ms");
        }

        Assert.True(latencies.Count > iterations * 0.9, "Should have measured at least 90% of iterations");
    }

    /// <summary>
    /// Test sustained high throughput over time.
    /// </summary>
    [Fact]
    public async Task SustainedThroughput()
    {
        var topic = $"perf-sustained-{Guid.NewGuid():N}";
        var durationSeconds = 10;
        var messageSize = 512;
        var messageValue = new string('X', messageSize);

        var config = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "sustained-producer",
            LingerMs = 5,
            BatchSize = 65536
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds));
        var sw = Stopwatch.StartNew();
        var messagesSent = 0;
        var errors = 0;

        var produceTasks = new List<Task>();

        while (!cts.Token.IsCancellationRequested)
        {
            try
            {
                var task = producer.ProduceAsync(topic, new Message<string, string>
                {
                    Key = $"key-{messagesSent}",
                    Value = messageValue
                });

                produceTasks.Add(task);
                messagesSent++;

                // Periodically await to avoid memory buildup
                if (messagesSent % 1000 == 0)
                {
                    await Task.WhenAll(produceTasks);
                    produceTasks.Clear();
                }
            }
            catch
            {
                errors++;
            }
        }

        // Wait for remaining messages
        try
        {
            await Task.WhenAll(produceTasks);
        }
        catch { }

        producer.Flush(TimeSpan.FromSeconds(10));
        sw.Stop();

        var totalBytes = (long)messagesSent * messageSize;
        var throughputMsgs = messagesSent * 1000.0 / sw.ElapsedMilliseconds;
        var throughputMB = totalBytes / (1024.0 * 1024.0) / (sw.ElapsedMilliseconds / 1000.0);

        _output.WriteLine($"Sustained throughput over {durationSeconds} seconds:");
        _output.WriteLine($"  Messages sent: {messagesSent:N0}");
        _output.WriteLine($"  Errors: {errors}");
        _output.WriteLine($"  Throughput: {throughputMsgs:F0} msg/sec, {throughputMB:F2} MB/sec");

        Assert.Equal(0, errors);
        Assert.True(messagesSent > 100, "Should have sent at least 100 messages");
    }

    // NOTE: ParallelProducers test moved to IsolatedPerformanceTests.cs for broker isolation

    /// <summary>
    /// Test mixed workload (produce and consume simultaneously).
    /// </summary>
    [Fact]
    public async Task MixedWorkload_ProduceAndConsume()
    {
        var topic = $"perf-mixed-{Guid.NewGuid():N}";
        var durationSeconds = 10;
        var messageValue = new string('X', 512);

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            ClientId = "mixed-producer",
            LingerMs = 5
        };

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = $"mixed-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds));
        var producedCount = 0;
        var consumedCount = 0;

        // Producer task
        var producerTask = Task.Run(async () =>
        {
            using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    await producer.ProduceAsync(topic, new Message<string, string>
                    {
                        Key = $"key-{producedCount}",
                        Value = messageValue
                    });
                    Interlocked.Increment(ref producedCount);
                }
                catch (OperationCanceledException) { break; }
                catch { }
            }

            producer.Flush(TimeSpan.FromSeconds(5));
        });

        // Consumer task
        var consumerTask = Task.Run(() =>
        {
            using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
            consumer.Subscribe(topic);

            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (result != null && !result.IsPartitionEOF)
                    {
                        Interlocked.Increment(ref consumedCount);
                    }
                }
                catch (OperationCanceledException) { break; }
                catch { }
            }

            consumer.Close();
        });

        await Task.WhenAll(producerTask, consumerTask);

        _output.WriteLine($"Mixed workload over {durationSeconds} seconds:");
        _output.WriteLine($"  Produced: {producedCount:N0} messages");
        _output.WriteLine($"  Consumed: {consumedCount:N0} messages");
        _output.WriteLine($"  Producer rate: {producedCount / durationSeconds:N0} msg/sec");
        _output.WriteLine($"  Consumer rate: {consumedCount / durationSeconds:N0} msg/sec");

        Assert.True(producedCount > 100, "Should have produced messages");
        Assert.True(consumedCount > 0, "Should have consumed messages");
    }

    private static long Percentile(List<long> values, int percentile)
    {
        var sorted = values.OrderBy(v => v).ToList();
        var index = (int)Math.Ceiling(percentile / 100.0 * sorted.Count) - 1;
        return sorted[Math.Max(0, Math.Min(index, sorted.Count - 1))];
    }

    private async Task<int> ConsumeAllMessages(string topic, int expectedCount, int timeoutSeconds)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BrokerFixture.BootstrapServers,
            GroupId = $"verify-consumer-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(topic);

        var count = 0;
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(timeoutSeconds));

        try
        {
            while (count < expectedCount && !cts.Token.IsCancellationRequested)
            {
                var result = consumer.Consume(cts.Token);
                if (result != null && !result.IsPartitionEOF)
                {
                    count++;
                }
            }
        }
        catch (OperationCanceledException) { }

        consumer.Close();
        return count;
    }
}
