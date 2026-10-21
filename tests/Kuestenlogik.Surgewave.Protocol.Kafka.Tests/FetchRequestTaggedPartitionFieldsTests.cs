using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Kafka.Tests;

/// <summary>
/// In Fetch v17+ a partition's ReplicaDirectoryId (tag 0) and, from v18, HighWatermark (tag 1) are tagged
/// fields. The parser read ReplicaDirectoryId as a fixed UUID, so a Java 4.x consumer — which sends no tags —
/// had the next bytes taken for it and the fetch failed ("UUID requires 16 bytes").
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class FetchRequestTaggedPartitionFieldsTests
{
    private static readonly Guid TopicId = Guid.Parse("6f1d2c3b-4a59-4e6f-8a7b-9c0d1e2f3a4b");

    private static byte[] Body(short version, Action<KafkaProtocolWriter> partitionTags)
    {
        // A consumer's Fetch body as the Java client writes it (v15+: no top-level ReplicaId).
        var w = new KafkaProtocolWriter();
        w.WriteInt32(500);          // MaxWaitMs
        w.WriteInt32(1);            // MinBytes
        w.WriteInt32(52_428_800);   // MaxBytes
        w.WriteInt8(1);             // IsolationLevel: read_committed
        w.WriteInt32(0);            // SessionId
        w.WriteInt32(-1);           // SessionEpoch
        w.WriteVarInt(2);           // one topic
        w.WriteUuid(TopicId);
        w.WriteVarInt(2);           // one partition
        w.WriteInt32(0);            // Partition
        w.WriteInt32(-1);           // CurrentLeaderEpoch
        w.WriteInt64(42);           // FetchOffset
        w.WriteInt32(-1);           // LastFetchedEpoch
        w.WriteInt64(-1);           // LogStartOffset
        w.WriteInt32(1_048_576);    // PartitionMaxBytes
        partitionTags(w);
        w.WriteVarInt(0);           // topic tagged fields
        w.WriteVarInt(1);           // no forgotten topics
        w.WriteCompactString("");   // RackId
        w.WriteVarInt(0);           // tagged fields
        return w.ToArray();
    }

    [Theory]
    [InlineData((short)17)]
    [InlineData((short)18)]
    public void AConsumersFetch_WithoutPartitionTags_Parses(short version)
    {
        var reader = new KafkaProtocolReader(Body(version, w => w.WriteVarInt(0)));

        var request = FetchRequest.ReadFrom(reader, version, correlationId: 1, clientId: "c");

        var partition = Assert.Single(Assert.Single(request.Topics).Partitions);
        Assert.Equal(42, partition.FetchOffset);
        Assert.Equal(Guid.Empty, partition.ReplicaDirectoryId);
        Assert.Equal(0, reader.Remaining);
    }

    [Fact]
    public void AFollowersFetch_WithReplicaDirectoryIdAndHighWatermarkTags_Parses()
    {
        var directory = Guid.NewGuid();
        var reader = new KafkaProtocolReader(Body(18, w =>
        {
            w.WriteVarInt(2);       // two tagged fields
            w.WriteVarInt(0);       // ReplicaDirectoryId
            w.WriteVarInt(16);
            w.WriteUuid(directory);
            w.WriteVarInt(1);       // HighWatermark
            w.WriteVarInt(8);
            w.WriteInt64(100);
        }));

        var request = FetchRequest.ReadFrom(reader, 18, correlationId: 1, clientId: "c");

        Assert.Equal(directory, Assert.Single(Assert.Single(request.Topics).Partitions).ReplicaDirectoryId);
        Assert.Equal(0, reader.Remaining);
    }
}
