using System.Diagnostics;
using Confluent.Kafka;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// Performance tests that require broker isolation to avoid flakiness.
/// Each test starts a fresh broker instance with dynamic ports.
/// </summary>
[Trait("Category", TestCategories.Integration)]
public class IsolatedPerformanceTests : IAsyncLifetime
{
    private readonly ITestOutputHelper _output;
    private SurgewaveRuntime? _surgewave;
    private ILoggerFactory? _loggerFactory;

    private string BootstrapServers => _surgewave?.BootstrapServers ?? throw new InvalidOperationException("Broker not started");

    public IsolatedPerformanceTests(ITestOutputHelper output)
    {
        _output = output;
    }

    public async ValueTask InitializeAsync()
    {
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Warning);
            builder.AddConsole();
        });

        _surgewave = await SurgewaveRuntime.CreateBuilder()
            .WithPort(0)  // Dynamic port to avoid conflicts
            .WithPartitions(3)
            .WithAutoCreateTopics(true)
            .WithShutdownTimeout(5)
            .WithLogging(_loggerFactory)
            .WithStorageEngine(StorageEngines.Memory)  // Use memory storage for performance tests
            .Build()
            .StartAsync();

        _output.WriteLine($"Broker started on {_surgewave.BootstrapServers}");
    }

    public async ValueTask DisposeAsync()
    {
        if (_surgewave != null)
        {
            await _surgewave.DisposeAsync();
        }
        _loggerFactory?.Dispose();
    }

    /// <summary>
    /// Test parallel producer performance.
    /// This test was flaky when sharing a broker with other tests due to timing issues.
    /// Fixed by increasing write workers and using aggressive flushing in LogManager.
    /// </summary>
    [Theory(Timeout = 120000)] // 2 minute timeout
    [InlineData(2)]
    [InlineData(4)]
    public async Task ParallelProducers(int producerCount)
    {
        var topic = $"perf-parallel-{producerCount}-{Guid.NewGuid():N}";
        var messagesPerProducer = 50;  // Reduced from 200 for faster test execution
        var messageSize = 512;  // Reduced from 1024
        var messageValue = new string('X', messageSize);

        // Wait for broker to be fully ready
        await WaitForBrokerReady();

        var sw = Stopwatch.StartNew();
        var successCount = 0;
        var failCount = 0;

        var tasks = Enumerable.Range(0, producerCount)
            .Select(async producerId =>
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = BootstrapServers,
                    ClientId = $"parallel-producer-{producerId}",
                    LingerMs = 5,
                    Acks = Acks.Leader,  // Leader ack is sufficient for single broker
                    MessageTimeoutMs = 10000,
                    RequestTimeoutMs = 5000,
                    RetryBackoffMs = 50,
                    MessageSendMaxRetries = 2
                };

                using var producer = new ProducerBuilder<string, string>(config).Build();

                for (int i = 0; i < messagesPerProducer; i++)
                {
                    try
                    {
                        await producer.ProduceAsync(topic, new Message<string, string>
                        {
                            Key = $"p{producerId}-{i}",
                            Value = messageValue
                        });
                        Interlocked.Increment(ref successCount);
                    }
                    catch (ProduceException<string, string>)
                    {
                        Interlocked.Increment(ref failCount);
                    }
                }

                producer.Flush(TimeSpan.FromSeconds(10));
            })
            .ToList();

        await Task.WhenAll(tasks);
        sw.Stop();

        var totalMessages = producerCount * messagesPerProducer;
        var totalBytes = (long)successCount * messageSize;
        var throughputMsgs = successCount * 1000.0 / sw.ElapsedMilliseconds;
        var throughputMB = totalBytes / (1024.0 * 1024.0) / (sw.ElapsedMilliseconds / 1000.0);

        _output.WriteLine($"Parallel producers ({producerCount}):");
        _output.WriteLine($"  Total attempted: {totalMessages:N0}");
        _output.WriteLine($"  Successful: {successCount:N0}");
        _output.WriteLine($"  Failed: {failCount:N0}");
        _output.WriteLine($"  Time: {sw.ElapsedMilliseconds}ms");
        _output.WriteLine($"  Throughput: {throughputMsgs:F0} msg/sec, {throughputMB:F2} MB/sec");

        // Verify all messages were produced and can be consumed
        var consumed = await ConsumeAllMessages(topic, successCount, timeoutSeconds: 30);
        _output.WriteLine($"  Consumed: {consumed:N0}");

        Assert.Equal(totalMessages, successCount);
        Assert.Equal(0, failCount);
        Assert.Equal(successCount, consumed);
    }

    private async Task WaitForBrokerReady()
    {
        var config = new ProducerConfig
        {
            BootstrapServers = BootstrapServers,
            MessageTimeoutMs = 5000
        };

        using var producer = new ProducerBuilder<string, string>(config).Build();

        // Try to produce a test message with retries
        for (int attempt = 0; attempt < 5; attempt++)
        {
            try
            {
                await producer.ProduceAsync("__broker_ready_check", new Message<string, string>
                {
                    Key = "ready",
                    Value = "check"
                });
                return; // Broker is ready
            }
            catch
            {
                await Task.Delay(500);
            }
        }
    }

    private async Task<int> ConsumeAllMessages(string topic, int expectedCount, int timeoutSeconds)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = BootstrapServers,
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
