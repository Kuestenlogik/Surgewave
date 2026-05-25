using Kuestenlogik.Surgewave.Client.Producer;

namespace Kuestenlogik.Surgewave.Examples;

/// <summary>
/// Simple producer example
/// </summary>
public class SimpleProducer
{
    public static async Task Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "localhost:9092",
            ClientId = "simple-producer",
            RequiredAcks = 1
        };

        await using var producer = new KafkaProducer(config);

        Console.WriteLine("Starting producer...");
        Console.WriteLine("Type messages to send (or 'quit' to exit):");

        while (true)
        {
            Console.Write("> ");
            var input = Console.ReadLine();

            if (string.IsNullOrEmpty(input) || input.Equals("quit", StringComparison.OrdinalIgnoreCase))
            {
                break;
            }

            try
            {
                var record = new ProducerRecord
                {
                    Topic = "simple-topic",
                    Value = System.Text.Encoding.UTF8.GetBytes(input),
                    Key = System.Text.Encoding.UTF8.GetBytes($"key-{DateTime.UtcNow.Ticks}")
                };

                var metadata = await producer.SendAsync(record);

                Console.WriteLine($"✓ Sent to partition {metadata.Partition} at offset {metadata.Offset}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"✗ Error: {ex.Message}");
            }
        }

        Console.WriteLine("Producer stopped.");
    }
}
