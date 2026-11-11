using System.Buffers.Binary;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// The embedded broker answers the Kafka APIs it advertises (#240). A request for an API without a handler gets a frame
/// that holds the correlation id and no body, and a Kafka client that was told the API exists fails reading it:
/// <c>kafka-topics --describe</c> lists the partition reassignments first and stopped there with "Buffer underflow".
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class EmbeddedBrokerKafkaApiTests
{
    [Fact]
    public async Task EveryApiTheBrokerAdvertisesHasAHandler()
    {
        await using var runtime = await StartAsync(TestContext.Current.CancellationToken);

        var unanswered = ApiVersionsResponse.CreateDefault(0, 3).ApiVersions
            .Select(api => (ApiKey)api.ApiKey)
            .Where(api => !runtime.HandlesKafkaApi(api))
            .ToList();

        Assert.True(unanswered.Count == 0, "advertised by ApiVersions, but not answered: " + string.Join(", ", unanswered));
    }

    [Fact(Timeout = 60_000)]
    public async Task ListPartitionReassignmentsIsAnsweredWithTheBytesKafkasSchemaImplies()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var runtime = await StartAsync(cancellationToken);
        var request = new ListPartitionReassignmentsRequest
        {
            ApiKey = ApiKey.ListPartitionReassignments,
            ApiVersion = 0,
            CorrelationId = 9,
            ClientId = "adminclient-1",
            TimeoutMs = 30_000,
            // As kafka-topics --describe asks: the partitions of the topic it is about to describe.
            Topics = [new ListPartitionReassignmentsRequest.ListPartitionReassignmentsTopic { Name = "orders", PartitionIndexes = [0, 1] }],
        };

        var response = await ExchangeAsync(runtime, request, cancellationToken);

        byte[] expected =
        [
            0x00, 0x00, 0x00, 0x09, // correlation id
            0x00,                   // response header tagged fields
            0x00, 0x00, 0x00, 0x00, // ThrottleTimeMs
            0x00, 0x00,             // ErrorCode
            0x00,                   // ErrorMessage: null compact string
            0x01,                   // Topics: compact array of 0 — nothing is being reassigned
            0x00,                   // tagged fields
        ];
        Assert.Equal(expected, response);
    }

    private static Task<SurgewaveRuntime> StartAsync(CancellationToken cancellationToken) => SurgewaveRuntime.CreateBuilder()
        .WithPort(0)
        .WithStorageEngine(StorageEngines.Memory)
        .Build()
        .StartAsync(cancellationToken);

    /// <summary>Sends one request over a connection of its own and returns the response frame, without its size.</summary>
    private static async Task<byte[]> ExchangeAsync(SurgewaveRuntime runtime, KafkaRequest request, CancellationToken cancellationToken)
    {
        var writer = new KafkaProtocolWriter();
        request.WriteTo(writer);
        var body = writer.ToArray();
        var frame = new byte[4 + body.Length];
        BinaryPrimitives.WriteInt32BigEndian(frame, body.Length);
        body.CopyTo(frame, 4);

        using var client = new TcpClient();
        await client.ConnectAsync(runtime.Host, runtime.Port, cancellationToken);
        var stream = client.GetStream();
        await stream.WriteAsync(frame, cancellationToken);

        var size = new byte[4];
        await stream.ReadExactlyAsync(size, cancellationToken);
        var response = new byte[BinaryPrimitives.ReadInt32BigEndian(size)];
        await stream.ReadExactlyAsync(response, cancellationToken);
        return response;
    }
}
