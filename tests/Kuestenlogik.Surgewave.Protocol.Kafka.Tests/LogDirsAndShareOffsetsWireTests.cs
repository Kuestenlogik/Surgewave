using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// Coverage-push batch — completes the JBOD admin trio (adds
/// <see cref="DescribeLogDirsRequest"/> + Response after the
/// AlterReplicaLogDirs / AssignReplicasToDirs pin) and KIP-932's
/// <see cref="AlterShareGroupOffsetsRequest"/> + Response.
///
/// DescribeLogDirs has the messiest version matrix in the JBOD
/// surface: v0 baseline, v1 added IsFutureKey, v2 became flexible,
/// v3 added a top-level ErrorCode, v4 added TotalBytes + UsableBytes.
/// Every version boundary gets a round-trip pin so any future
/// "simplify the WriteTo" can't silently drop one path.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class LogDirsAndShareOffsetsWireTests
{
    private static readonly Guid TopicIdA = new("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

    private static KafkaProtocolReader SkipFlexibleHeader(byte[] payload)
    {
        var reader = new KafkaProtocolReader(payload);
        reader.ReadInt16(); reader.ReadInt16(); reader.ReadInt32();
        reader.ReadCompactString(); reader.SkipTaggedFields();
        return reader;
    }

    private static KafkaProtocolReader SkipV0NonFlexibleHeader(byte[] payload)
    {
        var reader = new KafkaProtocolReader(payload);
        reader.ReadInt16(); reader.ReadInt16(); reader.ReadInt32();
        reader.ReadString(); // non-compact ClientId
        return reader;
    }

    // ───────────────────────────────────────────────────────────────
    // DescribeLogDirs (API key 35, v0-4)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void DescribeLogDirsRequest_V0_NonFlexible_NullTopics_RoundTrips()
    {
        // Topics=null → describe ALL log dirs. v0 wire uses int32(-1)
        // for null arrays.
        var original = new DescribeLogDirsRequest
        {
            ApiKey = ApiKey.DescribeLogDirs,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "jbod-admin",
            Topics = null,
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipV0NonFlexibleHeader(writer.ToArray());
        var parsed = DescribeLogDirsRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "jbod-admin");
        Assert.Null(parsed.Topics);
    }

    [Fact]
    public void DescribeLogDirsRequest_V2_Flexible_ExplicitTopics_RoundTrips()
    {
        var original = new DescribeLogDirsRequest
        {
            ApiKey = ApiKey.DescribeLogDirs,
            ApiVersion = 2,
            CorrelationId = 1,
            ClientId = "jbod-admin",
            Topics =
            [
                new DescribeLogDirsRequest.TopicRequest { Topic = "orders", Partitions = [0, 1, 2] },
                new DescribeLogDirsRequest.TopicRequest { Topic = "events", Partitions = [0] },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = DescribeLogDirsRequest.ReadFrom(reader, apiVersion: 2, correlationId: 1, clientId: "jbod-admin");

        Assert.NotNull(parsed.Topics);
        Assert.Equal(2, parsed.Topics!.Count);
        Assert.Equal("orders", parsed.Topics[0].Topic);
        Assert.Equal(new[] { 0, 1, 2 }, parsed.Topics[0].Partitions);
        Assert.Equal("events", parsed.Topics[1].Topic);
    }

    [Fact]
    public void DescribeLogDirsResponse_V0_Baseline_RoundTrips()
    {
        // v0: no IsFutureKey, no top-level ErrorCode, no Total/UsableBytes.
        var original = new DescribeLogDirsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            Results =
            [
                new DescribeLogDirsResponse.LogDirResult
                {
                    ErrorCode = ErrorCode.None,
                    LogDir = "/var/kafka/data-1",
                    Topics =
                    [
                        new DescribeLogDirsResponse.TopicResult
                        {
                            Topic = "orders",
                            Partitions =
                            [
                                new DescribeLogDirsResponse.PartitionResult { PartitionIndex = 0, Size = 100_000_000L, OffsetLag = 0 },
                            ],
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        // v0 response: no header tag-varint; CorrelationId(4) → ThrottleTime → body.
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = DescribeLogDirsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        Assert.Single(parsed.Results);
        Assert.Equal("/var/kafka/data-1", parsed.Results[0].LogDir);
        Assert.Equal(100_000_000L, parsed.Results[0].Topics[0].Partitions[0].Size);
        Assert.False(parsed.Results[0].Topics[0].Partitions[0].IsFutureKey); // v0 default
        Assert.Equal(-1L, parsed.Results[0].TotalBytes); // v0 default
    }

    [Fact]
    public void DescribeLogDirsResponse_V1_IsFutureKey_RoundTrips()
    {
        // v1 added IsFutureKey per partition (true = log being created by
        // an AlterReplicaLogDirs move-in-progress).
        var original = new DescribeLogDirsResponse
        {
            ApiVersion = 1,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            Results =
            [
                new DescribeLogDirsResponse.LogDirResult
                {
                    ErrorCode = ErrorCode.None,
                    LogDir = "/var/kafka/data-1",
                    Topics =
                    [
                        new DescribeLogDirsResponse.TopicResult
                        {
                            Topic = "orders",
                            Partitions =
                            [
                                new DescribeLogDirsResponse.PartitionResult
                                {
                                    PartitionIndex = 0,
                                    Size = 50_000_000L,
                                    OffsetLag = 1_500L,
                                    IsFutureKey = true,
                                },
                            ],
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = DescribeLogDirsResponse.ReadFrom(reader, apiVersion: 1, correlationId: 1);

        Assert.True(parsed.Results[0].Topics[0].Partitions[0].IsFutureKey);
    }

    [Fact]
    public void DescribeLogDirsResponse_V3_TopLevelErrorCode_RoundTrips()
    {
        // v3 added top-level ErrorCode. Test that a top-level error
        // propagates AND the partition list can still be parsed (empty).
        var original = new DescribeLogDirsResponse
        {
            ApiVersion = 3,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.NotController,
            Results = [],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = DescribeLogDirsResponse.ReadFrom(reader, apiVersion: 3, correlationId: 1);

        Assert.Equal(ErrorCode.NotController, parsed.ErrorCode);
        Assert.Empty(parsed.Results);
    }

    [Fact]
    public void DescribeLogDirsResponse_V4_TotalAndUsableBytes_RoundTrip()
    {
        // v4 added TotalBytes + UsableBytes per LogDirResult.
        var original = new DescribeLogDirsResponse
        {
            ApiVersion = 4,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            Results =
            [
                new DescribeLogDirsResponse.LogDirResult
                {
                    ErrorCode = ErrorCode.None,
                    LogDir = "/var/kafka/data-1",
                    Topics = [],
                    TotalBytes = 1_099_511_627_776L,  // 1 TiB
                    UsableBytes = 549_755_813_888L,   // 512 GiB free
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = DescribeLogDirsResponse.ReadFrom(reader, apiVersion: 4, correlationId: 1);

        Assert.Equal(1_099_511_627_776L, parsed.Results[0].TotalBytes);
        Assert.Equal(549_755_813_888L, parsed.Results[0].UsableBytes);
    }

    // ───────────────────────────────────────────────────────────────
    // AlterShareGroupOffsets (API key 91, v0)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void AlterShareGroupOffsetsRequest_FullShape_RoundTrips()
    {
        // Reset share-group start offsets to specific values per partition.
        // Real-world use case: replay a chunk of records after a poison-pill
        // got stuck in a share group.
        var original = new AlterShareGroupOffsetsRequest
        {
            ApiKey = ApiKey.AlterShareGroupOffsets,
            ApiVersion = 0,
            CorrelationId = 1,
            ClientId = "share-admin",
            GroupId = "share-orders",
            Topics =
            [
                new AlterShareGroupOffsetsRequest.AlterTopic
                {
                    TopicName = "orders",
                    Partitions =
                    [
                        new AlterShareGroupOffsetsRequest.AlterPartition { PartitionIndex = 0, StartOffset = 100_000L },
                        new AlterShareGroupOffsetsRequest.AlterPartition { PartitionIndex = 1, StartOffset = 99_500L  },
                    ],
                },
                new AlterShareGroupOffsetsRequest.AlterTopic
                {
                    TopicName = "audit",
                    Partitions =
                    [
                        new AlterShareGroupOffsetsRequest.AlterPartition { PartitionIndex = 0, StartOffset = 0L },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = SkipFlexibleHeader(writer.ToArray());
        var parsed = AlterShareGroupOffsetsRequest.ReadFrom(reader, apiVersion: 0, correlationId: 1, clientId: "share-admin");

        Assert.Equal("share-orders", parsed.GroupId);
        Assert.Equal(2, parsed.Topics.Count);
        Assert.Equal("orders", parsed.Topics[0].TopicName);
        Assert.Equal(100_000L, parsed.Topics[0].Partitions[0].StartOffset);
        Assert.Equal(99_500L, parsed.Topics[0].Partitions[1].StartOffset);
        Assert.Equal("audit", parsed.Topics[1].TopicName);
        Assert.Equal(0L, parsed.Topics[1].Partitions[0].StartOffset);
    }

    [Fact]
    public void AlterShareGroupOffsetsResponse_PerPartitionMixedErrors_RoundTrips()
    {
        var original = new AlterShareGroupOffsetsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ErrorMessage = null,
            Responses =
            [
                new AlterShareGroupOffsetsResponse.AlterTopicResult
                {
                    TopicName = "orders",
                    TopicId = TopicIdA,
                    Partitions =
                    [
                        new AlterShareGroupOffsetsResponse.AlterPartitionResult
                        {
                            PartitionIndex = 0,
                            ErrorCode = ErrorCode.None,
                            ErrorMessage = null,
                        },
                        new AlterShareGroupOffsetsResponse.AlterPartitionResult
                        {
                            PartitionIndex = 99,
                            ErrorCode = ErrorCode.UnknownTopicOrPartition,
                            ErrorMessage = "Partition 99 does not exist",
                        },
                    ],
                },
            ],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AlterShareGroupOffsetsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal(ErrorCode.None, parsed.ErrorCode);
        Assert.Single(parsed.Responses);
        Assert.Equal(TopicIdA, parsed.Responses[0].TopicId);
        Assert.Equal(2, parsed.Responses[0].Partitions.Count);
        Assert.Equal(ErrorCode.None, parsed.Responses[0].Partitions[0].ErrorCode);
        Assert.Equal(ErrorCode.UnknownTopicOrPartition, parsed.Responses[0].Partitions[1].ErrorCode);
        Assert.Contains("does not exist", parsed.Responses[0].Partitions[1].ErrorMessage);
    }

    [Fact]
    public void AlterShareGroupOffsetsResponse_TopLevelError_RoundTrips()
    {
        // GROUP_AUTHORIZATION_FAILED with empty Responses.
        var original = new AlterShareGroupOffsetsResponse
        {
            ApiVersion = 0,
            CorrelationId = 1,
            ThrottleTimeMs = 0,
            ErrorCode = (ErrorCode)30, // GROUP_AUTHORIZATION_FAILED
            ErrorMessage = "Not authorized to alter share-group offsets",
            Responses = [],
        };
        var writer = new KafkaProtocolWriter();
        original.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = AlterShareGroupOffsetsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 1);

        Assert.Equal((ErrorCode)30, parsed.ErrorCode);
        Assert.Contains("Not authorized", parsed.ErrorMessage);
        Assert.Empty(parsed.Responses);
    }
}
