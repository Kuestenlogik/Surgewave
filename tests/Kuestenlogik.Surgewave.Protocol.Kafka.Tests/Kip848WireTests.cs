using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Wire-level conformance for the KIP-848 ConsumerGroupHeartbeat / ConsumerGroupDescribe
/// pair. The end-to-end Confluent.Kafka driver test is skipped (it needs OffsetCommit/
/// Fetch v9 in the next-gen path), but the wire surface itself can be verified
/// byte-for-byte by round-tripping through the same parser librdkafka talks to.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip848WireTests
{
    [Fact]
    public void ConsumerGroupHeartbeatRequest_Roundtrips_WithFullPayload()
    {
        var topicId = Guid.NewGuid();
        var original = new ConsumerGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ConsumerGroupHeartbeat,
            ApiVersion = 0,
            CorrelationId = 42,
            ClientId = "kip848-test",
            GroupId = "g1",
            MemberId = "m1",
            MemberEpoch = 7,
            InstanceId = "static-1",
            RackId = "rack-a",
            RebalanceTimeoutMs = 60_000,
            SubscribedTopicNames = ["alpha", "beta"],
            ServerAssignor = "uniform",
            TopicPartitions =
            [
                new ConsumerGroupHeartbeatRequest.Assignor
                {
                    TopicId = topicId,
                    Partitions = [0, 1, 2],
                }
            ],
        };

        using var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var bytes = writer.ToArray();

        // Re-parse using the same dispatcher logic that would receive this from a
        // librdkafka next-gen consumer.
        var reader = new KafkaProtocolReader(bytes);
        // Header: api key, api version, correlation id, client id, tagged fields.
        Assert.Equal((short)ApiKey.ConsumerGroupHeartbeat, reader.ReadInt16());
        Assert.Equal(0, reader.ReadInt16());
        Assert.Equal(42, reader.ReadInt32());
        Assert.Equal("kip848-test", reader.ReadCompactString());
        reader.SkipTaggedFields();

        var parsed = ConsumerGroupHeartbeatRequest.ReadFrom(reader, apiVersion: 0, correlationId: 42, clientId: "kip848-test");

        Assert.Equal(original.GroupId, parsed.GroupId);
        Assert.Equal(original.MemberId, parsed.MemberId);
        Assert.Equal(original.MemberEpoch, parsed.MemberEpoch);
        Assert.Equal(original.InstanceId, parsed.InstanceId);
        Assert.Equal(original.RackId, parsed.RackId);
        Assert.Equal(original.RebalanceTimeoutMs, parsed.RebalanceTimeoutMs);
        Assert.Equal(original.SubscribedTopicNames, parsed.SubscribedTopicNames);
        Assert.Equal(original.ServerAssignor, parsed.ServerAssignor);
        Assert.NotNull(parsed.TopicPartitions);
        var owned = Assert.Single(parsed.TopicPartitions);
        Assert.Equal(topicId, owned.TopicId);
        Assert.Equal([0, 1, 2], owned.Partitions);
    }

    [Fact]
    public void ConsumerGroupHeartbeatRequest_Roundtrips_WithMinimalPayload()
    {
        // Steady-state heartbeat: no subscription change, no owned-partition update.
        // librdkafka sends this between rebalances.
        var original = new ConsumerGroupHeartbeatRequest
        {
            ApiKey = ApiKey.ConsumerGroupHeartbeat,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "c",
            GroupId = "g",
            MemberId = "m",
            MemberEpoch = 3,
            InstanceId = null,
            RackId = null,
            RebalanceTimeoutMs = -1,
            SubscribedTopicNames = null,
            ServerAssignor = null,
            TopicPartitions = null,
        };

        using var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var bytes = writer.ToArray();

        var reader = new KafkaProtocolReader(bytes);
        // Skip the request header so the body parser starts at the right spot.
        _ = reader.ReadInt16(); _ = reader.ReadInt16(); _ = reader.ReadInt32();
        _ = reader.ReadCompactString(); reader.SkipTaggedFields();

        var parsed = ConsumerGroupHeartbeatRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "c");

        Assert.Equal("g", parsed.GroupId);
        Assert.Equal("m", parsed.MemberId);
        Assert.Equal(3, parsed.MemberEpoch);
        Assert.Null(parsed.InstanceId);
        Assert.Null(parsed.RackId);
        Assert.Null(parsed.SubscribedTopicNames);
        Assert.Null(parsed.ServerAssignor);
        Assert.Null(parsed.TopicPartitions);
    }

    [Fact]
    public void ConsumerGroupHeartbeatResponse_Roundtrips_WithAssignment()
    {
        var topicId = Guid.NewGuid();
        var original = new ConsumerGroupHeartbeatResponse
        {
            CorrelationId = 99,
            ApiVersion = 0,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            MemberId = "m-assigned",
            MemberEpoch = 4,
            HeartbeatIntervalMs = 5000,
            MemberAssignment = new ConsumerGroupHeartbeatResponse.Assignment
            {
                TopicPartitions =
                [
                    new ConsumerGroupHeartbeatResponse.TopicPartitions
                    {
                        TopicId = topicId,
                        Partitions = [0, 1],
                    }
                ],
            },
        };

        using var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var bytes = writer.ToArray();

        // Strip the 4-byte response header (correlationId) — ReadFrom expects to start AFTER it.
        var reader = new KafkaProtocolReader(bytes);
        Assert.Equal(99, reader.ReadInt32());

        var parsed = ConsumerGroupHeartbeatResponse.ReadFrom(reader, apiVersion: 0, correlationId: 99);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        Assert.Equal("m-assigned", parsed.MemberId);
        Assert.Equal(4, parsed.MemberEpoch);
        Assert.Equal(5000, parsed.HeartbeatIntervalMs);
        Assert.NotNull(parsed.MemberAssignment);
        var tp = Assert.Single(parsed.MemberAssignment.TopicPartitions);
        Assert.Equal(topicId, tp.TopicId);
        Assert.Equal([0, 1], tp.Partitions);
    }

    [Fact]
    public void ConsumerGroupHeartbeatResponse_Roundtrips_WhenNoAssignmentYet()
    {
        // KIP-848 reconciliation: the broker may withhold the assignment until a
        // previous member has revoked. Wire format must round-trip null cleanly.
        var original = new ConsumerGroupHeartbeatResponse
        {
            CorrelationId = 1,
            ApiVersion = 0,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            MemberId = "m-pending",
            MemberEpoch = 1,
            HeartbeatIntervalMs = 5000,
            MemberAssignment = null,
        };

        using var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var bytes = writer.ToArray();

        var reader = new KafkaProtocolReader(bytes);
        _ = reader.ReadInt32(); // correlation id

        var parsed = ConsumerGroupHeartbeatResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Null(parsed.MemberAssignment);
    }

    [Fact]
    public void ConsumerGroupDescribeRequest_Roundtrips()
    {
        var original = new ConsumerGroupDescribeRequest
        {
            ApiKey = ApiKey.ConsumerGroupDescribe,
            ApiVersion = 0,
            CorrelationId = 7,
            ClientId = "describe-client",
            GroupIds = ["g1", "g2", "g3"],
            IncludeAuthorizedOperations = true,
        };

        using var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var bytes = writer.ToArray();

        var reader = new KafkaProtocolReader(bytes);
        _ = reader.ReadInt16(); _ = reader.ReadInt16(); _ = reader.ReadInt32();
        _ = reader.ReadCompactString(); reader.SkipTaggedFields();

        var parsed = ConsumerGroupDescribeRequest.ReadFrom(reader, apiVersion: 0, correlationId: 7, clientId: "describe-client");

        Assert.Equal(["g1", "g2", "g3"], parsed.GroupIds);
        Assert.True(parsed.IncludeAuthorizedOperations);
    }
}
