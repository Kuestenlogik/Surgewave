using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// JoinGroup v8+ carries a Reason before its tagged fields (KIP-800). The parser did not read it, took its
/// length for the tagged-field count, and a Java consumer's JoinGroup v9 failed with "Cannot read VarInt" —
/// a classic-protocol consumer of a current Java client could not join a group.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class JoinGroupRequestWireTests
{
    private static byte[] Body(short version, string? reason)
    {
        // The body as a Java client writes it (flexible, v6+).
        var w = new KafkaProtocolWriter();
        w.WriteCompactString("g");          // GroupId
        w.WriteInt32(45_000);               // SessionTimeoutMs
        w.WriteInt32(300_000);              // RebalanceTimeoutMs
        w.WriteCompactString("");           // MemberId
        w.WriteCompactString(null);         // GroupInstanceId
        w.WriteCompactString("consumer");   // ProtocolType
        w.WriteVarInt(2);                   // one protocol
        w.WriteCompactString("range");
        w.WriteCompactBytes([1, 2, 3]);
        w.WriteVarInt(0);                   // protocol tagged fields
        if (version >= 8)
            w.WriteCompactString(reason);   // Reason
        w.WriteVarInt(0);                   // tagged fields
        return w.ToArray();
    }

    [Theory]
    [InlineData((short)9, "rebalance needed")]
    [InlineData((short)9, null)]
    [InlineData((short)7, null)]
    public void Flexible_ReadsTheBodyAJavaClientWrites(short version, string? reason)
    {
        var reader = new KafkaProtocolReader(Body(version, reason));

        var request = JoinGroupRequest.ReadFrom(reader, version, correlationId: 1, clientId: "c");

        Assert.Equal("g", request.GroupId);
        Assert.Equal("range", Assert.Single(request.Protocols).Name);
        Assert.Equal(reason, request.Reason);
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void ResponseV9_HasProtocolTypeAndSkipAssignment_WhereAJavaClientReadsThem()
    {
        var writer = new KafkaProtocolWriter();
        new JoinGroupResponse
        {
            ApiVersion = 9,
            CorrelationId = 3,
            ErrorCode = ErrorCode.None,
            GenerationId = 1,
            ProtocolType = "consumer",
            ProtocolName = "range",
            Leader = "m-1",
            MemberId = "m-1",
            Members = [new JoinGroupResponse.JoinGroupMember { MemberId = "m-1", Metadata = [9] }],
        }.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray());

        Assert.Equal(3, reader.ReadInt32());                 // correlation id
        Assert.Equal(0, reader.ReadVarInt());                // response header tagged fields
        Assert.Equal(0, reader.ReadInt32());                 // throttle time
        Assert.Equal(0, reader.ReadInt16());                 // error code
        Assert.Equal(1, reader.ReadInt32());                 // generation id
        Assert.Equal("consumer", reader.ReadCompactString()); // protocol type (v7+)
        Assert.Equal("range", reader.ReadCompactString());    // protocol name
        Assert.Equal("m-1", reader.ReadCompactString());      // leader
        Assert.False(reader.ReadBoolean());                   // skip assignment (v9+)
        Assert.Equal("m-1", reader.ReadCompactString());      // member id
        Assert.Equal(2, reader.ReadVarInt());                 // one member
        Assert.Equal("m-1", reader.ReadCompactString());
        Assert.Null(reader.ReadCompactString());              // group instance id
        Assert.Equal([9], reader.ReadCompactBytes());
        Assert.Equal(0, reader.ReadVarInt());                 // member tagged fields
        Assert.Equal(0, reader.ReadVarInt());                 // tagged fields
        Assert.Equal(0, reader.Remaining);
    }
}
