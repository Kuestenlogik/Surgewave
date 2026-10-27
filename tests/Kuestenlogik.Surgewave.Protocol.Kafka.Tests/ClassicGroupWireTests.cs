using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// The classic consumer group as a Java 4.x consumer speaks it (#195): SyncGroup v5, Heartbeat v4 and
/// LeaveGroup v5. Each of the three failed to parse, so a Java consumer with a group never got an assignment.
/// The bodies below are laid out field by field as Kafka's message schemas declare them.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ClassicGroupWireTests
{
    [Fact]
    public void SyncGroupV5_ReadsProtocolTypeAndNameBeforeTheAssignments()
    {
        var w = new KafkaProtocolWriter();
        w.WriteCompactString("g");          // GroupId
        w.WriteInt32(3);                    // GenerationId
        w.WriteCompactString("m-1");        // MemberId
        w.WriteCompactString(null);         // GroupInstanceId
        w.WriteCompactString("consumer");   // ProtocolType (v5+)
        w.WriteCompactString("range");      // ProtocolName (v5+)
        w.WriteVarInt(2);                   // one assignment
        w.WriteCompactString("m-1");
        w.WriteCompactBytes([1, 2]);
        w.WriteVarInt(0);                   // assignment tagged fields
        w.WriteVarInt(0);                   // tagged fields
        var reader = new KafkaProtocolReader(w.ToArray());

        var request = SyncGroupRequest.ReadFrom(reader, 5, correlationId: 1, clientId: "c");

        Assert.Equal("consumer", request.ProtocolType);
        Assert.Equal("range", request.ProtocolName);
        Assert.Equal("m-1", Assert.Single(request.Assignments).MemberId);
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void SyncGroupResponseV5_CarriesProtocolTypeAndName_WhereAJavaClientReadsThem()
    {
        var w = new KafkaProtocolWriter();
        new SyncGroupResponse
        {
            ApiVersion = 5,
            CorrelationId = 4,
            ErrorCode = ErrorCode.None,
            ProtocolType = "consumer",
            ProtocolName = "range",
            Assignment = [7],
        }.WriteTo(w);
        var reader = new KafkaProtocolReader(w.ToArray());

        Assert.Equal(4, reader.ReadInt32());                    // correlation id
        Assert.Equal(0, reader.ReadVarInt());                   // response header tagged fields
        Assert.Equal(0, reader.ReadInt32());                    // throttle time
        Assert.Equal(0, reader.ReadInt16());                    // error code
        Assert.Equal("consumer", reader.ReadCompactString());   // protocol type (v5+)
        Assert.Equal("range", reader.ReadCompactString());      // protocol name (v5+)
        Assert.Equal([7], reader.ReadCompactBytes());           // assignment
        Assert.Equal(0, reader.ReadVarInt());                   // tagged fields
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void HeartbeatV4_IsReadAsTheFlexibleRequestItIs()
    {
        var w = new KafkaProtocolWriter();
        w.WriteCompactString("g");          // GroupId
        w.WriteInt32(3);                    // GenerationId
        w.WriteCompactString("m-1");        // MemberId
        w.WriteCompactString(null);         // GroupInstanceId
        w.WriteVarInt(0);                   // tagged fields
        var reader = new KafkaProtocolReader(w.ToArray());

        var request = HeartbeatRequest.ReadFrom(reader, 4, correlationId: 1, clientId: "c");

        Assert.Equal("g", request.GroupId);
        Assert.Equal(3, request.GenerationId);
        Assert.Equal("m-1", request.MemberId);
        Assert.Null(request.GroupInstanceId);
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void HeartbeatV3_StaysTheNonFlexibleRequest()
    {
        var w = new KafkaProtocolWriter();
        w.WriteString("g");
        w.WriteInt32(3);
        w.WriteString("m-1");
        w.WriteString(null);
        var reader = new KafkaProtocolReader(w.ToArray());

        var request = HeartbeatRequest.ReadFrom(reader, 3, correlationId: 1, clientId: "c");

        Assert.Equal("m-1", request.MemberId);
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void LeaveGroupV5_ReadsEachMembersReason()
    {
        var w = new KafkaProtocolWriter();
        w.WriteCompactString("g");                      // GroupId
        w.WriteVarInt(2);                               // one member
        w.WriteCompactString("m-1");                    // MemberId
        w.WriteCompactString(null);                     // GroupInstanceId
        w.WriteCompactString("the consumer is being closed"); // Reason (v5+)
        w.WriteVarInt(0);                               // member tagged fields
        w.WriteVarInt(0);                               // tagged fields
        var reader = new KafkaProtocolReader(w.ToArray());

        var request = LeaveGroupRequest.ReadFrom(reader, 5, correlationId: 1, clientId: "c");

        var member = Assert.Single(request.Members);
        Assert.Equal("m-1", member.MemberId);
        Assert.Equal("the consumer is being closed", member.Reason);
        Assert.Equal(0, reader.Remaining);
    }
}
