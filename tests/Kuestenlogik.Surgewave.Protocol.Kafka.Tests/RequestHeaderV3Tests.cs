using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Request header v3 (KIP-1313) carries the client instance id as tagged field 0 (#194). The broker reads it and
/// still skips every tag it does not know, so a header with more tags parses as one without.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class RequestHeaderV3Tests
{
    private static readonly Guid InstanceId = Guid.Parse("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0");

    [Fact]
    public void TheClientInstanceIdInHeaderTagZeroIsRead()
    {
        var request = Parse(headerTags: [(0, InstanceIdBytes())]);

        Assert.Equal(InstanceId, request.HeaderClientInstanceId);
        Assert.IsType<MetadataRequest>(request);
    }

    [Fact]
    public void UnknownHeaderTagsAreSkippedAndTheBodyStillParses()
    {
        var request = Parse(headerTags: [(7, [0xAB, 0xCD]), (0, InstanceIdBytes())]);

        Assert.Equal(InstanceId, request.HeaderClientInstanceId);
        var metadata = Assert.IsType<MetadataRequest>(request);
        Assert.Null(metadata.Topics);
        Assert.True(metadata.AllowAutoTopicCreation);
    }

    [Fact]
    public void AHeaderWithoutTheTagHasNoClientInstanceId()
    {
        Assert.Null(Parse(headerTags: []).HeaderClientInstanceId);
    }

    /// <summary>Kafka writes a uuid as its 16 bytes, most significant first.</summary>
    private static byte[] InstanceIdBytes()
    {
        var bytes = new byte[16];
        InstanceId.TryWriteBytes(bytes, bigEndian: true, out _);
        return bytes;
    }

    /// <summary>A Metadata v12 request (flexible header) with the given header tags and an all-topics body.</summary>
    private static KafkaRequest Parse((int Tag, byte[] Value)[] headerTags)
    {
        var message = new List<byte>();
        Span<byte> int16 = stackalloc byte[2];
        Span<byte> int32 = stackalloc byte[4];

        BinaryPrimitives.WriteInt16BigEndian(int16, (short)ApiKey.Metadata); message.AddRange(int16.ToArray());
        BinaryPrimitives.WriteInt16BigEndian(int16, 12); message.AddRange(int16.ToArray());
        BinaryPrimitives.WriteInt32BigEndian(int32, 99); message.AddRange(int32.ToArray());
        var clientId = Encoding.UTF8.GetBytes("header-v3");
        BinaryPrimitives.WriteInt16BigEndian(int16, (short)clientId.Length); message.AddRange(int16.ToArray());
        message.AddRange(clientId);

        message.Add((byte)headerTags.Length);
        foreach (var (tag, value) in headerTags)
        {
            message.Add((byte)tag);
            message.Add((byte)value.Length);
            message.AddRange(value);
        }

        // Body: Topics null (all), AllowAutoTopicCreation, IncludeTopicAuthorizedOperations, no tagged fields.
        message.AddRange([0x00, 0x01, 0x00, 0x00]);

        var buffer = message.ToArray();
        return KafkaProtocolHandler.ParseRequest(buffer, buffer.Length);
    }
}
