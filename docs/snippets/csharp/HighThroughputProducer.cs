using Kuestenlogik.Surgewave.Client.Producer;
using System.Diagnostics;

namespace Kuestenlogik.Surgewave.Examples;

/// <summary>
/// High-throughput producer example demonstrating batching and performance
/// </summary>
public class HighThroughputProducer
{
    public static async Task Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            ClientId = "high-throughput-producer",
            RequiredAcks = 1,
            BatchSize = 16384,
            LingerMs = 10 // Wait up to 10ms to batch messages
        };

        await using var producer = new KafkaProducer(config);

        Console.WriteLine("High-Throughput Producer Test");
        Console.WriteLine("==============================\n");

        const int messageCount = 10000;
        const int messageSize = 1024; // 1KB messages

        var payload = new byte[messageSize];
        new Random().NextBytes(payload);

        var stopwatch = Stopwatch.StartNew();
        var tasks = new List<Task<RecordMetadata>>();

        Console.WriteLine($"Sending {messageCount} messages of {messageSize} bytes each...");

        for (int i = 0; i < messageCount; i++)
        {
            var record = new ProducerRecord
            {
                Topic = "throughput-test",
                Value = payload,
                Key = System.Text.Encoding.UTF8.GetBytes($"key-{i}")
            };

            tasks.Add(producer.SendAsync(record));

            if (i > 0 && i % 1000 == 0)
            {
                Console.Write($"\rSent: {i}/{messageCount}");
            }
        }

        Console.WriteLine($"\rSent: {messageCount}/{messageCount}");
        Console.WriteLine("Waiting for acknowledgments...");

        await Task.WhenAll(tasks);

        stopwatch.Stop();

        // Calculate statistics
        var totalBytes = messageCount * messageSize;
        var throughputMBps = (totalBytes / (1024.0 * 1024.0)) / stopwatch.Elapsed.TotalSeconds;
        var messagesPerSecond = messageCount / stopwatch.Elapsed.TotalSeconds;

        Console.WriteLine("\nResults:");
        Console.WriteLine($"  Total Time: {stopwatch.Elapsed.TotalSeconds:F2} seconds");
        Console.WriteLine($"  Messages/sec: {messagesPerSecond:F0}");
        Console.WriteLine($"  Throughput: {throughputMBps:F2} MB/s");
        Console.WriteLine($"  Average Latency: {stopwatch.Elapsed.TotalMilliseconds / messageCount:F2} ms");
    }
}
