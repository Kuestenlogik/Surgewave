using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// KIP-1226 — <c>DescribeShareGroupOffsetsResponse</c> v1 adds a <c>Lag</c>
/// int64 (default -1, <c>ignorable: true</c>) per partition between
/// <c>LeaderEpoch</c> and <c>ErrorCode</c>. Without it, kafka-share-groups.sh
/// --describe shows <c>Lag = -1</c> for every partition.
///
/// The wire glue (v1 gating + the Lag field on DescribePartitionResult) and
/// the broker-side computation (<c>HighWatermark - startOffset</c>, floor 0)
/// were already in <c>ShareGroupCoordinator.HandleDescribeShareGroupOffsets</c>;
/// these tests pin the round-trip framing so a future refactor can't silently
/// drop the field again.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip1226ShareGroupLagTests
{
    [Fact]
    public void V1_RoundTrip_PreservesLag()
    {
        var response = new DescribeShareGroupOffsetsResponse
        {
            ApiVersion = 1,
            CorrelationId = 7,
            ThrottleTimeMs = 0,
            Groups =
            [
                new DescribeShareGroupOffsetsResponse.DescribeGroupResult
                {
                    GroupId = "share-grp-1",
                    Topics =
                    [
                        new DescribeShareGroupOffsetsResponse.DescribeTopicResult
                        {
                            TopicName = "orders",
                            TopicId = Guid.NewGuid(),
                            Partitions =
                            [
                                new DescribeShareGroupOffsetsResponse.DescribePartitionResult
                                {
                                    PartitionIndex = 0,
                                    StartOffset = 100,
                                    LeaderEpoch = 4,
                                    Lag = 42,
                                    ErrorCode = ErrorCode.None,
                                },
                                new DescribeShareGroupOffsetsResponse.DescribePartitionResult
                                {
                                    PartitionIndex = 1,
                                    StartOffset = 0,
                                    LeaderEpoch = 4,
                                    Lag = 0, // caught up
                                    ErrorCode = ErrorCode.None,
                                },
                            ],
                        },
                    ],
                },
            ],
        };

        var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        // WriteTo emits CorrelationId (int32, 4 bytes) ahead of the body.
        // ReadFrom expects the reader to point at the response-header
        // tagged-fields varint that comes after CorrelationId; the framing
        // layer in the broker pre-strips the CorrelationId. Skip 4 bytes.
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = DescribeShareGroupOffsetsResponse.ReadFrom(reader, apiVersion: 1, correlationId: 7);

        var partitions = parsed.Groups[0].Topics[0].Partitions;
        Assert.Equal(42, partitions[0].Lag);
        Assert.Equal(0, partitions[1].Lag);
        Assert.Equal(100, partitions[0].StartOffset);
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void V0_RoundTrip_DropsLagField()
    {
        // At v0 the wire must NOT include the Lag field — otherwise the
        // bytes for the partition's ErrorCode would mis-align. Set Lag=42 on
        // the wire-source response; after a v0 round-trip the parsed value
        // must come back at the protocol default (-1) and every byte must be
        // consumed.
        var response = new DescribeShareGroupOffsetsResponse
        {
            ApiVersion = 0,
            CorrelationId = 7,
            ThrottleTimeMs = 0,
            Groups =
            [
                new DescribeShareGroupOffsetsResponse.DescribeGroupResult
                {
                    GroupId = "share-grp-1",
                    Topics =
                    [
                        new DescribeShareGroupOffsetsResponse.DescribeTopicResult
                        {
                            TopicName = "orders",
                            TopicId = Guid.NewGuid(),
                            Partitions =
                            [
                                new DescribeShareGroupOffsetsResponse.DescribePartitionResult
                                {
                                    PartitionIndex = 0,
                                    StartOffset = 100,
                                    LeaderEpoch = 4,
                                    Lag = 42,
                                    ErrorCode = ErrorCode.None,
                                },
                            ],
                        },
                    ],
                },
            ],
        };

        var writer = new KafkaProtocolWriter();
        response.WriteTo(writer);
        // Same 4-byte CorrelationId skip as the v1 case — see comment above.
        var reader = new KafkaProtocolReader(writer.ToArray().AsSpan(4).ToArray());
        var parsed = DescribeShareGroupOffsetsResponse.ReadFrom(reader, apiVersion: 0, correlationId: 7);

        Assert.Equal(-1, parsed.Groups[0].Topics[0].Partitions[0].Lag);
        Assert.Equal(100, parsed.Groups[0].Topics[0].Partitions[0].StartOffset);
        Assert.Equal(0, reader.Remaining);
    }
}
