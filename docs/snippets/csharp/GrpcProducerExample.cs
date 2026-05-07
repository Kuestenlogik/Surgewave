using Kuestenlogik.Surgewave.Grpc.Client;

namespace Kuestenlogik.Surgewave.Examples;

/// <summary>
/// Example using gRPC producer client
/// </summary>
public class GrpcProducerExample
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== gRPC Producer Example ===\n");

        await using var producer = new GrpcProducer("https://localhost:9093");

        Console.WriteLine("Sending messages via gRPC...\n");

        // Send individual messages
        for (int i = 0; i < 10; i++)
        {
            var message = $"Message {i} via gRPC";
            var key = $"key-{i}";

            var response = await producer.SendAsync(
                topic: "grpc-test-topic",
                value: System.Text.Encoding.UTF8.GetBytes(message),
                key: System.Text.Encoding.UTF8.GetBytes(key)
            );

            if (response.ErrorCode == Kuestenlogik.Surgewave.Grpc.ErrorCode.None)
            {
                Console.WriteLine($"✓ Sent: {message}");
                Console.WriteLine($"  Topic: {response.Topic}");
                Console.WriteLine($"  Partition: {response.Partition}");
                Console.WriteLine($"  Offset: {response.Offset}");
                Console.WriteLine();
            }
            else
            {
                Console.WriteLine($"✗ Error: {response.ErrorMessage}");
            }

            await Task.Delay(500);
        }

        Console.WriteLine("Done!");
    }
}
