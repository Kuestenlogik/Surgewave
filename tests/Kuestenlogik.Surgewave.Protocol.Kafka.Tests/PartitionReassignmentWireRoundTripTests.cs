using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Coverage-push batch — Partition-reassignment admin RPCs.
/// Covers <see cref="AlterPartitionReassignmentsRequest"/> + Response
/// (API key 45) and <see cref="ListPartitionReassignmentsRequest"/> +
/// Response (API key 46). Both are v0+ flexible.
///
/// These RPCs are emitted by <c>kafka-reassign-partitions.sh</c> and
/// the Control-UI's reassignment tab; framing regressions surface as
/// "reassignment seems to start but Status stays Pending forever".
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class PartitionReassignmentWireRoundTripTests
{
    private static KafkaProtocolReader SkipFlexibleHeader(byte[] payload)
    {
        var reader = new KafkaProtocolReader(payload);
        reader.ReadInt16(); reader.ReadInt16(); reader.ReadInt32();
        reader.ReadCompactString(); reader.SkipTaggedFields();
        return reader;
    }

    // ───────────────────────────────────────────────────────────────
    // AlterPartitionReassignments (API key 45)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void AlterRequest_FullShape_RoundTrips()
    {
        var original = new AlterPartitionReassignmentsRequest
        {
            ApiKey = ApiKey.AlterPartitionReassignments,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "reassign-admin",
            TimeoutMs = 60_000,
            Topics =
            [
                new AlterPartitionReassignmentsRequest.ReassignableTopic
                {
                    Name = "orders",
                    Partitions =
                    [
                        new AlterPartitionReassignmentsRequest.ReassignablePartition
                        {
                            PartitionIndex = 0,
                            Replicas = [1, 2, 3], // move to brokers 1, 2, 3
                        },
                        new AlterPartitionReassignmentsRequest.ReassignablePartition
                        {
                            PartitionIndex = 1,
                            Replicas = [2, 3, 4],
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AlterPartitionReassignmentsRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "reassign-admin");

        Assert.Equal(60_000, parsed.TimeoutMs);
        Assert.Single(parsed.Topics);
        Assert.Equal("orders", parsed.Topics[0].Name);
        Assert.Equal(2, parsed.Topics[0].Partitions.Count);
        Assert.Equal(0, parsed.Topics[0].Partitions[0].PartitionIndex);
        Assert.Equal(new[] { 1, 2, 3 }, parsed.Topics[0].Partitions[0].Replicas);
        Assert.Equal(new[] { 2, 3, 4 }, parsed.Topics[0].Partitions[1].Replicas);
    }

    [Fact]
    public void AlterRequest_CancelReassignment_NullReplicas_RoundTrips()
    {
        // Replicas=null is the wire signal to CANCEL a pending reassignment
        // (per the doc-comment on ReassignablePartition). Compact-array
        // null marker is varint(-1) which encodes as 0x00 (zigzag of -1) -
        // distinct from empty list which is varint(1) (count+1).
        var original = new AlterPartitionReassignmentsRequest
        {
            ApiKey = ApiKey.AlterPartitionReassignments,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "reassign-admin",
            TimeoutMs = 30_000,
            Topics =
            [
                new AlterPartitionReassignmentsRequest.ReassignableTopic
                {
                    Name = "orders",
                    Partitions =
                    [
                        new AlterPartitionReassignmentsRequest.ReassignablePartition
                        {
                            PartitionIndex = 5,
                            Replicas = null, // CANCEL
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AlterPartitionReassignmentsRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "reassign-admin");

        Assert.Null(parsed.Topics[0].Partitions[0].Replicas);
    }

    [Fact]
    public void AlterResponse_PartialFailure_RoundTrips()
    {
        // Two partitions, one accepted (None) one rejected (InvalidReplicaAssignment).
        var original = new AlterPartitionReassignmentsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            Responses =
            [
                new AlterPartitionReassignmentsResponse.ReassignableTopicResponse
                {
                    Name = "orders",
                    Partitions =
                    [
                        new AlterPartitionReassignmentsResponse.ReassignablePartitionResponse
                        {
                            PartitionIndex = 0,
                            ErrorCode = ErrorCode.None,
                            ErrorMessage = null,
                        },
                        new AlterPartitionReassignmentsResponse.ReassignablePartitionResponse
                        {
                            PartitionIndex = 1,
                            ErrorCode = ErrorCode.InvalidReplicaAssignment,
                            ErrorMessage = "Replica 99 is not a known broker",
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        // Response: CorrelationId(4) ahead of header tagged-fields varint.
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AlterPartitionReassignmentsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        Assert.Null(parsed.ErrorMessage);
        Assert.Single(parsed.Responses);

        var partitions = parsed.Responses[0].Partitions;
        Assert.Equal(ErrorCode.None, partitions[0].ErrorCode);
        Assert.Null(partitions[0].ErrorMessage);
        Assert.Equal(ErrorCode.InvalidReplicaAssignment, partitions[1].ErrorCode);
        Assert.Contains("known broker", partitions[1].ErrorMessage!);
    }

    [Fact]
    public void AlterResponse_TopLevelError_RoundTrips()
    {
        // Top-level NotController — the whole request gets rejected with
        // an empty Responses list.
        var original = new AlterPartitionReassignmentsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.NotController,
            ErrorMessage = "Not the active controller",
            Responses = [],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AlterPartitionReassignmentsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.NotController, parsed.ErrorCode);
        Assert.Equal("Not the active controller", parsed.ErrorMessage);
        Assert.Empty(parsed.Responses);
    }

    // ───────────────────────────────────────────────────────────────
    // ListPartitionReassignments (API key 46)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void ListRequest_WithExplicitTopics_RoundTrips()
    {
        var original = new ListPartitionReassignmentsRequest
        {
            ApiKey = ApiKey.ListPartitionReassignments,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "list-admin",
            TimeoutMs = 30_000,
            Topics =
            [
                new ListPartitionReassignmentsRequest.ListPartitionReassignmentsTopic
                {
                    Name = "orders",
                    PartitionIndexes = [0, 1, 2],
                },
                new ListPartitionReassignmentsRequest.ListPartitionReassignmentsTopic
                {
                    Name = "events",
                    PartitionIndexes = [0],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = ListPartitionReassignmentsRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "list-admin");

        Assert.Equal(30_000, parsed.TimeoutMs);
        Assert.NotNull(parsed.Topics);
        Assert.Equal(2, parsed.Topics!.Count);
        Assert.Equal("orders", parsed.Topics[0].Name);
        Assert.Equal(new[] { 0, 1, 2 }, parsed.Topics[0].PartitionIndexes);
        Assert.Equal("events", parsed.Topics[1].Name);
    }

    [Fact]
    public void ListRequest_NullTopics_MeansListEverything()
    {
        // Topics=null → list ALL ongoing reassignments cluster-wide.
        // Wire: compact-array null marker varint(-1)=0x00. Pin that this
        // is wire-distinct from empty list ([]).
        var original = new ListPartitionReassignmentsRequest
        {
            ApiKey = ApiKey.ListPartitionReassignments,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "list-admin",
            TimeoutMs = 30_000,
            Topics = null,
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = ListPartitionReassignmentsRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "list-admin");

        Assert.Null(parsed.Topics);
    }

    [Fact]
    public void ListResponse_FullShape_AddingAndRemovingReplicas_RoundTrips()
    {
        // Real reassignment-in-progress shape: current replicas + the
        // set being added + the set being removed. The three lists are
        // independent — pinning all three together catches the off-by-one
        // compact-array bug class (count vs count+1).
        var original = new ListPartitionReassignmentsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            Topics =
            [
                new ListPartitionReassignmentsResponse.OngoingTopicReassignment
                {
                    Name = "orders",
                    Partitions =
                    [
                        new ListPartitionReassignmentsResponse.OngoingPartitionReassignment
                        {
                            PartitionIndex = 0,
                            Replicas = [1, 2, 3, 4],       // current = old ∪ adding
                            AddingReplicas = [4],          // joining
                            RemovingReplicas = [1],        // leaving
                        },
                        new ListPartitionReassignmentsResponse.OngoingPartitionReassignment
                        {
                            PartitionIndex = 1,
                            Replicas = [2, 3, 5],
                            AddingReplicas = [5],
                            RemovingReplicas = [],         // no-op removing list
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = ListPartitionReassignmentsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        Assert.Single(parsed.Topics);
        Assert.Equal(2, parsed.Topics[0].Partitions.Count);

        var p0 = parsed.Topics[0].Partitions[0];
        Assert.Equal(new[] { 1, 2, 3, 4 }, p0.Replicas);
        Assert.Equal(new[] { 4 }, p0.AddingReplicas);
        Assert.Equal(new[] { 1 }, p0.RemovingReplicas);

        var p1 = parsed.Topics[0].Partitions[1];
        Assert.Equal(new[] { 5 }, p1.AddingReplicas);
        Assert.Empty(p1.RemovingReplicas);
    }

    [Fact]
    public void ListResponse_NoOngoingReassignments_RoundTrips()
    {
        // Steady-state cluster — Topics list empty. Pin that the
        // empty-array compact prefix doesn't drift framing.
        var original = new ListPartitionReassignmentsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            Topics = [],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = ListPartitionReassignmentsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);
        Assert.Empty(parsed.Topics);
    }
}
