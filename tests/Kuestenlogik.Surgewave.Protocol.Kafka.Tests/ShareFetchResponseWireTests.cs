using Kuestenlogik.Surgewave.Protocol;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// ShareFetch v1 as a Java client reads it: a partition's records are nullable in v0 only. The writer sent null for a
/// partition without records, and a Java share consumer (4.1) failed every fetch with "non-nullable field records was
/// serialized as null" and consumed nothing (#228).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ShareFetchResponseWireTests
{
    [Fact]
    public void V1_APartitionWithoutRecordsCarriesAnEmptySetNotNull()
    {
        var topicId = Guid.NewGuid();
        var writer = new KafkaProtocolWriter();
        new ShareFetchResponse
        {
            ApiVersion = 1,
            CorrelationId = 9,
            AcquisitionLockTimeoutMs = 30_000,
            Responses =
            [
                new ShareFetchResponse.ShareFetchableTopicResponse
                {
                    TopicId = topicId,
                    Partitions =
                    [
                        new ShareFetchResponse.PartitionData
                        {
                            PartitionIndex = 0,
                            CurrentLeader = new ShareFetchResponse.LeaderIdAndEpoch { LeaderId = 2, LeaderEpoch = 4 },
                            Records = null,
                            AcquiredRecords = [],
                        },
                    ],
                },
            ],
            NodeEndpoints = [],
        }.WriteTo(writer);
        var reader = new KafkaProtocolReader(writer.ToArray());

        Assert.Equal(9, reader.ReadInt32());          // correlation id
        Assert.Equal(0, reader.ReadVarInt());         // response header tagged fields
        Assert.Equal(0, reader.ReadInt32());          // throttle time
        Assert.Equal(0, reader.ReadInt16());          // error code
        Assert.Null(reader.ReadCompactString());      // error message
        Assert.Equal(30_000, reader.ReadInt32());     // acquisition lock timeout
        Assert.Equal(2, reader.ReadVarInt());         // one topic
        Assert.Equal(topicId, reader.ReadUuid());
        Assert.Equal(2, reader.ReadVarInt());         // one partition
        Assert.Equal(0, reader.ReadInt32());
        Assert.Equal(0, reader.ReadInt16());          // error code
        Assert.Null(reader.ReadCompactString());
        Assert.Equal(0, reader.ReadInt16());          // acknowledge error code
        Assert.Null(reader.ReadCompactString());
        Assert.Equal(2, reader.ReadInt32());          // leader id
        Assert.Equal(4, reader.ReadInt32());          // leader epoch
        Assert.Equal(0, reader.ReadVarInt());         // leader tagged fields

        // Records: a compact length of 1 is an empty set; 0 would be null.
        Assert.Equal(1, reader.ReadVarInt());
    }
}
