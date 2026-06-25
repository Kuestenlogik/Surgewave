using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Coverage-push batch — KRaft consensus protocol RPCs.
/// Covers <see cref="VoteRequest"/> + Response (API key 52),
/// <see cref="BeginQuorumEpochRequest"/> + Response (API key 53), and
/// <see cref="EndQuorumEpochRequest"/> + Response (API key 54).
///
/// These are the leader-election trio that every controller-quorum
/// member exchanges. Vote is v0+ flexible; BeginQuorumEpoch and
/// EndQuorumEpoch have a non-flexible v0 and a flexible v1 (with
/// tagged fields + compact strings + compact arrays). The version
/// branching is the most likely place for framing drift — both v0
/// and v1 paths get a round-trip pin.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class RaftConsensusWireRoundTripTests
{
    private const string MetadataTopic = "__cluster_metadata";

    /// <summary>
    /// Skip the v0 non-flexible request header
    /// ([ApiKey(2)][ApiVersion(2)][CorrelationId(4)][ClientId int16+UTF8]).
    /// </summary>
    private static KafkaProtocolReader SkipV0Header(byte[] payload)
    {
        var reader = new KafkaProtocolReader(payload);
        reader.ReadInt16(); // ApiKey
        reader.ReadInt16(); // ApiVersion
        reader.ReadInt32(); // CorrelationId
        reader.ReadString(); // non-compact ClientId at v0
        return reader;
    }

    /// <summary>
    /// Skip the v1+ flexible request header
    /// ([ApiKey(2)][ApiVersion(2)][CorrelationId(4)][ClientId compact-string][tagged-fields varint]).
    /// </summary>
    private static KafkaProtocolReader SkipFlexibleHeader(byte[] payload)
    {
        var reader = new KafkaProtocolReader(payload);
        reader.ReadInt16(); reader.ReadInt16(); reader.ReadInt32();
        reader.ReadCompactString(); reader.SkipTaggedFields();
        return reader;
    }

    // ───────────────────────────────────────────────────────────────
    // Vote (API key 52, v0+ flexible)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void VoteRequest_RoundTrip_PreservesCandidateState()
    {
        var original = new VoteRequest
        {
            ApiKey = ApiKey.Vote,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "voter-2",
            ClusterId = "surgewave-quorum",
            Topics =
            [
                new VoteRequest.TopicData
                {
                    TopicName = MetadataTopic,
                    Partitions =
                    [
                        new VoteRequest.PartitionData
                        {
                            PartitionIndex = 0,
                            CandidateEpoch = 7,
                            CandidateId = 2,
                            LastOffsetEpoch = 6,
                            LastOffset = 1_000_000L,
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = VoteRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "voter-2");

        Assert.Equal("surgewave-quorum", parsed.ClusterId);
        Assert.Single(parsed.Topics);
        Assert.Equal(MetadataTopic, parsed.Topics[0].TopicName);
        var partition = parsed.Topics[0].Partitions[0];
        Assert.Equal(7, partition.CandidateEpoch);
        Assert.Equal(2, partition.CandidateId);
        Assert.Equal(6, partition.LastOffsetEpoch);
        Assert.Equal(1_000_000L, partition.LastOffset);
    }

    [Fact]
    public void VoteResponse_VoteGranted_RoundTrips()
    {
        var original = new VoteResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ErrorCode = ErrorCode.None,
            Topics =
            [
                new VoteResponse.TopicData
                {
                    TopicName = MetadataTopic,
                    Partitions =
                    [
                        new VoteResponse.PartitionData
                        {
                            PartitionIndex = 0,
                            ErrorCode = ErrorCode.None,
                            LeaderId = -1, // election ongoing — no current leader
                            LeaderEpoch = 7,
                            VoteGranted = true,
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = VoteResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        var partition = parsed.Topics[0].Partitions[0];
        Assert.Equal(-1, partition.LeaderId);
        Assert.Equal(7, partition.LeaderEpoch);
        Assert.True(partition.VoteGranted);
    }

    [Fact]
    public void VoteResponse_VoteDenied_RoundTrips()
    {
        // Voter already cast its vote for someone else this epoch.
        var original = new VoteResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ErrorCode = ErrorCode.None,
            Topics =
            [
                new VoteResponse.TopicData
                {
                    TopicName = MetadataTopic,
                    Partitions =
                    [
                        new VoteResponse.PartitionData
                        {
                            PartitionIndex = 0,
                            ErrorCode = ErrorCode.None,
                            LeaderId = -1,
                            LeaderEpoch = 7,
                            VoteGranted = false,
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = VoteResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);
        Assert.False(parsed.Topics[0].Partitions[0].VoteGranted);
    }

    // ───────────────────────────────────────────────────────────────
    // BeginQuorumEpoch (API key 53, v0 non-flexible + v1 flexible)
    // ───────────────────────────────────────────────────────────────

    private static BeginQuorumEpochRequest NewBeginRequest(short apiVersion, int leaderId, int leaderEpoch) => new()
    {
        ApiKey = ApiKey.BeginQuorumEpoch,
        ApiVersion = apiVersion,
        CorrelationId = 1,
        ClientId = "voter-1",
        ClusterId = apiVersion >= 1 ? "surgewave-quorum" : null,
        Topics =
        [
            new BeginQuorumEpochRequest.TopicData
            {
                TopicName = MetadataTopic,
                Partitions =
                [
                    new BeginQuorumEpochRequest.PartitionData
                    {
                        PartitionIndex = 0,
                        LeaderId = leaderId,
                        LeaderEpoch = leaderEpoch,
                    },
                ],
            },
        ],
    };

    [Fact]
    public void BeginQuorumEpochRequest_V0_NonFlexible_RoundTrips()
    {
        var original = NewBeginRequest(apiVersion: 0, leaderId: 1, leaderEpoch: 5);
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipV0Header(writer.ToArray());
        var parsed = BeginQuorumEpochRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "voter-1");

        Assert.Null(parsed.ClusterId); // v0 doesn't carry it
        Assert.Single(parsed.Topics);
        Assert.Equal(MetadataTopic, parsed.Topics[0].TopicName);
        Assert.Equal(1, parsed.Topics[0].Partitions[0].LeaderId);
        Assert.Equal(5, parsed.Topics[0].Partitions[0].LeaderEpoch);
    }

    [Fact]
    public void BeginQuorumEpochRequest_V1_Flexible_RoundTrips()
    {
        var original = NewBeginRequest(apiVersion: 1, leaderId: 1, leaderEpoch: 5);
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = BeginQuorumEpochRequest.ReadFrom(reader, apiVersion: 1, correlationId: 1, clientId: "voter-1");

        Assert.Equal("surgewave-quorum", parsed.ClusterId);
        Assert.Equal(1, parsed.Topics[0].Partitions[0].LeaderId);
    }

    [Fact]
    public void BeginQuorumEpochResponse_V0_RoundTrips()
    {
        var original = new BeginQuorumEpochResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ErrorCode = ErrorCode.None,
            Topics =
            [
                new BeginQuorumEpochResponse.TopicData
                {
                    TopicName = MetadataTopic,
                    Partitions =
                    [
                        new BeginQuorumEpochResponse.PartitionData
                        {
                            PartitionIndex = 0,
                            ErrorCode = ErrorCode.None,
                            LeaderId = 1,
                            LeaderEpoch = 5,
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        // v0 response: no header tagged-fields varint. CorrelationId(4) then body.
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = BeginQuorumEpochResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        Assert.Equal(1, parsed.Topics[0].Partitions[0].LeaderId);
        Assert.Equal(5, parsed.Topics[0].Partitions[0].LeaderEpoch);
    }

    [Fact]
    public void BeginQuorumEpochResponse_V1_Flexible_PerPartitionError_RoundTrips()
    {
        // NotController (e.g. partition's epoch advanced past the request's,
        // or recipient is no longer controller). Realistic partial-failure
        // shape during a split brain. NB: Surgewave's ErrorCode enum
        // doesn't (yet) carry a dedicated FENCED_LEADER_EPOCH value;
        // NotController is the closest semantic match.
        var original = new BeginQuorumEpochResponse
        {
            ApiVersion = 1,
            CorrelationId = 1,
            ErrorCode = ErrorCode.None,
            Topics =
            [
                new BeginQuorumEpochResponse.TopicData
                {
                    TopicName = MetadataTopic,
                    Partitions =
                    [
                        new BeginQuorumEpochResponse.PartitionData
                        {
                            PartitionIndex = 0,
                            ErrorCode = ErrorCode.NotController,
                            LeaderId = 2,
                            LeaderEpoch = 8, // newer epoch known
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = BeginQuorumEpochResponse.ReadFrom(reader, apiVersion: 1, correlationId: 1);

        var partition = parsed.Topics[0].Partitions[0];
        Assert.Equal(ErrorCode.NotController, partition.ErrorCode);
        Assert.Equal(8, partition.LeaderEpoch);
    }

    // ───────────────────────────────────────────────────────────────
    // EndQuorumEpoch (API key 54, v0 non-flexible + v1 flexible)
    // ───────────────────────────────────────────────────────────────

    private static EndQuorumEpochRequest NewEndRequest(short apiVersion, int[] preferredSuccessors) => new()
    {
        ApiKey = ApiKey.EndQuorumEpoch,
        ApiVersion = apiVersion,
        CorrelationId = 1,
        ClientId = "leader-1",
        ClusterId = apiVersion >= 1 ? "surgewave-quorum" : null,
        Topics =
        [
            new EndQuorumEpochRequest.TopicData
            {
                TopicName = MetadataTopic,
                Partitions =
                [
                    new EndQuorumEpochRequest.PartitionData
                    {
                        PartitionIndex = 0,
                        LeaderId = 1,
                        LeaderEpoch = 5,
                        PreferredSuccessors = [.. preferredSuccessors],
                    },
                ],
            },
        ],
    };

    [Fact]
    public void EndQuorumEpochRequest_V0_NonFlexible_RoundTrips()
    {
        // Resigning leader hands off to voters 2, 3 in priority order.
        var original = NewEndRequest(apiVersion: 0, preferredSuccessors: [2, 3]);
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipV0Header(writer.ToArray());
        var parsed = EndQuorumEpochRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "leader-1");

        Assert.Null(parsed.ClusterId);
        var partition = parsed.Topics[0].Partitions[0];
        Assert.Equal(1, partition.LeaderId);
        Assert.Equal(new[] { 2, 3 }, partition.PreferredSuccessors);
    }

    [Fact]
    public void EndQuorumEpochRequest_V1_Flexible_RoundTrips()
    {
        var original = NewEndRequest(apiVersion: 1, preferredSuccessors: [2]);
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = EndQuorumEpochRequest.ReadFrom(reader, apiVersion: 1, correlationId: 1, clientId: "leader-1");

        Assert.Equal("surgewave-quorum", parsed.ClusterId);
        Assert.Equal(new[] { 2 }, parsed.Topics[0].Partitions[0].PreferredSuccessors);
    }

    [Fact]
    public void EndQuorumEpochRequest_EmptyPreferredSuccessors_RoundTrips()
    {
        // Resigning leader without a successor preference — voters pick
        // freely. Pin that the empty list (count=0 or compact 1) doesn't
        // drift framing.
        var original = NewEndRequest(apiVersion: 1, preferredSuccessors: Array.Empty<int>());
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = EndQuorumEpochRequest.ReadFrom(reader, apiVersion: 1, correlationId: 1, clientId: "leader-1");

        Assert.Empty(parsed.Topics[0].Partitions[0].PreferredSuccessors);
    }
}
