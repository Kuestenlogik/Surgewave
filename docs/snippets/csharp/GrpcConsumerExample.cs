using Kuestenlogik.Surgewave.Grpc.Client;

namespace Kuestenlogik.Surgewave.Examples;

/// <summary>
/// Example using gRPC consumer client with streaming
/// </summary>
public class GrpcConsumerExample
{
    public static async Task Main(string[] args)
    {
        Console.WriteLine("=== gRPC Consumer Example ===\n");

        await using var consumer = new GrpcConsumer("https://localhost:9093");

        Console.WriteLine("Consuming messages via gRPC streaming...\n");

        var messageCount = 0;

        try
        {
            await foreach (var response in consumer.ConsumeStreamAsync(
                topic: "grpc-test-topic",
                partition: 0,
                offset: -2, // Start from earliest
                maxRecords: 10))
            {
                if (response.ErrorCode != Kuestenlogik.Surgewave.Grpc.ErrorCode.None)
                {
                    Console.WriteLine($"Error: {response.ErrorMessage}");
                    break;
                }

                foreach (var record in response.Records)
                {
                    var key = System.Text.Encoding.UTF8.GetString(record.Key.ToByteArray());
                    var value = System.Text.Encoding.UTF8.GetString(record.Value.ToByteArray());

                    messageCount++;

                    Console.WriteLine($"[{messageCount}] Received:");
                    Console.WriteLine($"  Key: {key}");
                    Console.WriteLine($"  Value: {value}");
                    Console.WriteLine($"  Offset: {record.Offset}");
                    Console.WriteLine($"  Timestamp: {DateTimeOffset.FromUnixTimeMilliseconds(record.Timestamp)}");
                    Console.WriteLine();
                }

                // Stop after receiving some messages
                if (messageCount >= 10)
                {
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }

        Console.WriteLine($"Total messages consumed: {messageCount}");
    }
}
