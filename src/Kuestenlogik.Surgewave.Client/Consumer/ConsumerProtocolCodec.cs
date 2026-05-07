using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Encodes and decodes consumer protocol messages for consumer group coordination.
/// Handles binary serialization of subscription metadata and partition assignments.
/// </summary>
public static class ConsumerProtocolCodec
{
    /// <summary>
    /// Build consumer protocol metadata bytes.
    /// Format: version (2) + topic count (2) + topics + user data length (4) + user data
    /// </summary>
    public static byte[] BuildConsumerMetadata(List<string> topics)
    {
        // Calculate size: version(2) + topic count(2) + topic strings + user data length(4)
        var size = 2 + 2 + topics.Sum(t => 2 + Encoding.UTF8.GetByteCount(t)) + 4;
        var buffer = new byte[size];
        var span = buffer.AsSpan();
        var offset = 0;

        // Version
        BinaryPrimitives.WriteInt16BigEndian(span[offset..], 0);
        offset += 2;

        // Topic count
        BinaryPrimitives.WriteInt16BigEndian(span[offset..], (short)topics.Count);
        offset += 2;

        // Topics
        foreach (var topic in topics)
        {
            var bytes = Encoding.UTF8.GetBytes(topic);
            BinaryPrimitives.WriteInt16BigEndian(span[offset..], (short)bytes.Length);
            offset += 2;
            bytes.CopyTo(span[offset..]);
            offset += bytes.Length;
        }

        // User data length (0)
        BinaryPrimitives.WriteInt32BigEndian(span[offset..], 0);

        return buffer;
    }

    /// <summary>
    /// Build assignment bytes.
    /// Format: version (2) + topic count (2) + [topic + partition count + partitions]... + user data length (4)
    /// </summary>
    public static byte[] BuildAssignment(List<(string Topic, int Partition)> partitions)
    {
        if (partitions.Count == 0)
            return [0, 0, 0, 0, 0, 0]; // version=0, topics=0, userData=0

        // Group by topic
        var byTopic = partitions.GroupBy(p => p.Topic).ToList();

        // Calculate size
        var size = 2; // version
        size += 2; // topic count
        foreach (var group in byTopic)
        {
            size += 2 + Encoding.UTF8.GetByteCount(group.Key); // topic name
            size += 4; // partition count
            size += group.Count() * 4; // partitions
        }
        size += 4; // user data length

        var buffer = new byte[size];
        var span = buffer.AsSpan();
        var offset = 0;

        // Version
        BinaryPrimitives.WriteInt16BigEndian(span[offset..], 0);
        offset += 2;

        // Topic count
        BinaryPrimitives.WriteInt16BigEndian(span[offset..], (short)byTopic.Count);
        offset += 2;

        // Topics
        foreach (var group in byTopic)
        {
            // Topic name
            var topicBytes = Encoding.UTF8.GetBytes(group.Key);
            BinaryPrimitives.WriteInt16BigEndian(span[offset..], (short)topicBytes.Length);
            offset += 2;
            topicBytes.CopyTo(span[offset..]);
            offset += topicBytes.Length;

            // Partition count
            BinaryPrimitives.WriteInt32BigEndian(span[offset..], group.Count());
            offset += 4;

            // Partitions
            foreach (var (_, partition) in group)
            {
                BinaryPrimitives.WriteInt32BigEndian(span[offset..], partition);
                offset += 4;
            }
        }

        // User data length (0)
        BinaryPrimitives.WriteInt32BigEndian(span[offset..], 0);

        return buffer;
    }

    /// <summary>
    /// Parse assignment bytes to get assigned partitions.
    /// </summary>
    public static List<(string Topic, int Partition)> ParseAssignment(byte[] assignment)
    {
        if (assignment.Length < 6)
            return [];

        var span = assignment.AsSpan();
        var offset = 0;

        // Version
        _ = BinaryPrimitives.ReadInt16BigEndian(span[offset..]);
        offset += 2;

        // Topic count
        var topicCount = BinaryPrimitives.ReadInt16BigEndian(span[offset..]);
        offset += 2;

        var result = new List<(string Topic, int Partition)>();

        for (int t = 0; t < topicCount; t++)
        {
            // Topic name
            var topicLen = BinaryPrimitives.ReadInt16BigEndian(span[offset..]);
            offset += 2;
            var topic = Encoding.UTF8.GetString(span.Slice(offset, topicLen));
            offset += topicLen;

            // Partition count
            var partitionCount = BinaryPrimitives.ReadInt32BigEndian(span[offset..]);
            offset += 4;

            // Partitions
            for (int p = 0; p < partitionCount; p++)
            {
                var partition = BinaryPrimitives.ReadInt32BigEndian(span[offset..]);
                offset += 4;
                result.Add((topic, partition));
            }
        }

        return result;
    }
}
