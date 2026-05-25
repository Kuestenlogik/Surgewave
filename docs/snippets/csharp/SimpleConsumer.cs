using Kuestenlogik.Surgewave.Client.Consumer;

namespace Kuestenlogik.Surgewave.Examples;

/// <summary>
/// Simple consumer example
/// </summary>
public class SimpleConsumer
{
    public static async Task Main(string[] args)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "simple-consumer-group",
            ClientId = "simple-consumer",
            AutoOffsetReset = "earliest"
        };

        await using var consumer = new KafkaConsumer(config);

        Console.WriteLine("Starting consumer...");
        Console.WriteLine("Subscribing to 'simple-topic'");

        consumer.Subscribe("simple-topic");

        Console.WriteLine("Polling for messages (Ctrl+C to exit)...\n");

        var messageCount = 0;

        try
        {
            while (true)
            {
                var records = await consumer.PollAsync(TimeSpan.FromSeconds(1));

                foreach (var record in records)
                {
                    var key = record.Key != null
                        ? System.Text.Encoding.UTF8.GetString(record.Key)
                        : "null";

                    var value = System.Text.Encoding.UTF8.GetString(record.Value);

                    messageCount++;

                    Console.WriteLine($"[{messageCount}] Offset: {record.Offset}");
                    Console.WriteLine($"    Key: {key}");
                    Console.WriteLine($"    Value: {value}");
                    Console.WriteLine($"    Timestamp: {DateTimeOffset.FromUnixTimeMilliseconds(record.Timestamp):yyyy-MM-dd HH:mm:ss}");
                    Console.WriteLine();
                }

                if (records.Count == 0)
                {
                    // No messages, wait a bit
                    await Task.Delay(100);
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("\nConsumer stopped.");
        }

        Console.WriteLine($"Total messages consumed: {messageCount}");
    }
}
