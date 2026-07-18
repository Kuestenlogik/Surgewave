using System.Buffers;
using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.Replication;

/// <summary>
/// #82 S5: LeaderConnection.SerializeFetchRequest was rewritten from MemoryStream+BinaryWriter to a
/// two-pass exact-size serialization into a pooled buffer. These tests pin that the new wire bytes are
/// byte-identical to the old path and that the leader parser round-trips every field, so the request
/// wire format is provably unchanged.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class ReplicationFetchRequestRoundTripTests
{
    private static ReplicaFetchRequest MultiTopicRequest() => new()
    {
        ReplicaId = 7,
        MaxWaitMs = 500,
        MinBytes = 1,
        MaxBytes = 8_388_608,
        IsolationLevel = 0,
        Topics =
        [
            new ReplicaFetchRequest.TopicData
            {
                Topic = "orders",
                Partitions =
                [
                    new ReplicaFetchRequest.PartitionData { Partition = 0, FetchOffset = 42, PartitionMaxBytes = 1_048_576 },
                    new ReplicaFetchRequest.PartitionData { Partition = 3, FetchOffset = 999, PartitionMaxBytes = 524_288 },
                ],
            },
            new ReplicaFetchRequest.TopicData
            {
                // Multi-byte UTF-8 name (Ω = 2 bytes) exercises GetByteCount vs char count.
                Topic = "user-events-utf8-Ω",
                Partitions =
                [
                    new ReplicaFetchRequest.PartitionData { Partition = 1, FetchOffset = 0, PartitionMaxBytes = 2048 },
                ],
            },
        ],
    };

    [Fact]
    public void SerializeFetchRequest_RoundTripsThroughLeaderParser()
    {
        var req = MultiTopicRequest();

        var (buffer, total) = LeaderConnection.SerializeFetchRequest(req, correlationId: 4242);
        try
        {
            // Strip the 4-byte size prefix — that is exactly what the leader's ReadBodyAsync hands to ParseRequest.
            var body = buffer.AsSpan(4, total - 4).ToArray();
            var parsed = ReplicationServer.ParseRequest(body);
            var rt = parsed.FetchRequest!;

            Assert.Equal(1, parsed.ApiKey);
            Assert.Equal(4242, parsed.CorrelationId);
            Assert.Equal(req.ReplicaId, rt.ReplicaId);
            Assert.Equal(req.MaxWaitMs, rt.MaxWaitMs);
            Assert.Equal(req.MinBytes, rt.MinBytes);
            Assert.Equal(req.MaxBytes, rt.MaxBytes);
            Assert.Equal(req.IsolationLevel, rt.IsolationLevel);
            Assert.Equal(req.Topics.Count, rt.Topics.Count);

            for (var t = 0; t < req.Topics.Count; t++)
            {
                Assert.Equal(req.Topics[t].Topic, rt.Topics[t].Topic);
                Assert.Equal(req.Topics[t].Partitions.Count, rt.Topics[t].Partitions.Count);
                for (var p = 0; p < req.Topics[t].Partitions.Count; p++)
                {
                    // NOTE: the parser deliberately skips currentLeaderEpoch and logStartOffset (structural
                    // -1 padding with no target field), so only these three fields are asserted.
                    Assert.Equal(req.Topics[t].Partitions[p].Partition, rt.Topics[t].Partitions[p].Partition);
                    Assert.Equal(req.Topics[t].Partitions[p].FetchOffset, rt.Topics[t].Partitions[p].FetchOffset);
                    Assert.Equal(req.Topics[t].Partitions[p].PartitionMaxBytes, rt.Topics[t].Partitions[p].PartitionMaxBytes);
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4242)]
    [InlineData(-5)]
    public void SerializeFetchRequest_IsByteIdenticalToLegacyBinaryWriterPath(int correlationId)
    {
        // Compare the new two-pass serializer against the exact pre-#82-S5 MemoryStream+BinaryWriter
        // implementation for arbitrary inputs — a stronger, non-brittle byte-identity guard than a
        // hand-derived golden literal.
        var requests = new[]
        {
            MultiTopicRequest(),
            new ReplicaFetchRequest // single topic / single partition, ASCII name
            {
                ReplicaId = 1, MaxWaitMs = 500, MinBytes = 1, MaxBytes = 1_048_576, IsolationLevel = 0,
                Topics = [ new() { Topic = "t", Partitions = [ new() { Partition = 0, FetchOffset = 0, PartitionMaxBytes = 1024 } ] } ],
            },
            new ReplicaFetchRequest { ReplicaId = 3, MaxWaitMs = 0, MinBytes = 1, MaxBytes = 65_536, IsolationLevel = 1, Topics = [] }, // empty topics
        };

        foreach (var req in requests)
        {
            var (buffer, total) = LeaderConnection.SerializeFetchRequest(req, correlationId);
            try
            {
                var actual = buffer.AsSpan(0, total).ToArray();
                var expected = LegacySerialize(req, correlationId);
                Assert.Equal(expected, actual);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }

    [Fact]
    public void SerializeFetchRequest_IsDeterministic()
    {
        // Guards against uninitialized rented-tail bytes leaking into TotalLength.
        var req = MultiTopicRequest();
        var (b1, t1) = LeaderConnection.SerializeFetchRequest(req, 1);
        var (b2, t2) = LeaderConnection.SerializeFetchRequest(req, 1);
        try
        {
            Assert.Equal(t1, t2);
            Assert.True(b1.AsSpan(0, t1).SequenceEqual(b2.AsSpan(0, t2)));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(b1);
            ArrayPool<byte>.Shared.Return(b2);
        }
    }

    /// <summary>Exact copy of the pre-#82-S5 serializer; the reference the new path must match byte-for-byte.</summary>
    private static byte[] LegacySerialize(ReplicaFetchRequest request, int correlationId)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(0); // size placeholder
        writer.Write(BinaryPrimitives.ReverseEndianness((short)1));  // apiKey
        writer.Write(BinaryPrimitives.ReverseEndianness((short)11)); // apiVersion
        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));
        writer.Write(BinaryPrimitives.ReverseEndianness((short)-1)); // clientId
        writer.Write(BinaryPrimitives.ReverseEndianness(request.ReplicaId));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.MaxWaitMs));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.MinBytes));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.MaxBytes));
        writer.Write((byte)request.IsolationLevel);
        writer.Write(BinaryPrimitives.ReverseEndianness(0));  // sessionId
        writer.Write(BinaryPrimitives.ReverseEndianness(-1)); // sessionEpoch
        writer.Write(BinaryPrimitives.ReverseEndianness(request.Topics.Count));
        foreach (var topic in request.Topics)
        {
            var topicBytes = System.Text.Encoding.UTF8.GetBytes(topic.Topic);
            writer.Write(BinaryPrimitives.ReverseEndianness((short)topicBytes.Length));
            writer.Write(topicBytes);
            writer.Write(BinaryPrimitives.ReverseEndianness(topic.Partitions.Count));
            foreach (var partition in topic.Partitions)
            {
                writer.Write(BinaryPrimitives.ReverseEndianness(partition.Partition));
                writer.Write(BinaryPrimitives.ReverseEndianness(-1)); // currentLeaderEpoch
                writer.Write(BinaryPrimitives.ReverseEndianness(partition.FetchOffset));
                writer.Write(BinaryPrimitives.ReverseEndianness(-1L)); // logStartOffset
                writer.Write(BinaryPrimitives.ReverseEndianness(partition.PartitionMaxBytes));
            }
        }
        writer.Write(BinaryPrimitives.ReverseEndianness(0));         // forgottenTopics
        writer.Write(BinaryPrimitives.ReverseEndianness((short)-1)); // rackId

        var data = ms.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(data.AsSpan(0, 4), data.Length - 4);
        return data;
    }
}
