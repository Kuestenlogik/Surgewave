using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Text;

namespace Kuestenlogik.Surgewave.Broker.Native.Assignors;

/// <summary>
/// Factory for creating partition assignors
/// </summary>
public static class PartitionAssignorFactory
{
    private static readonly FrozenDictionary<string, IPartitionAssignor> Assignors = new Dictionary<string, IPartitionAssignor>(StringComparer.OrdinalIgnoreCase)
    {
        ["range"] = new RangeAssignor(),
        ["roundrobin"] = new RoundRobinAssignor(),
        ["sticky"] = new StickyAssignor(),
        ["cooperative-sticky"] = new CooperativeStickyAssignor()
    }.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);

    public static IPartitionAssignor GetAssignor(string name)
    {
        if (Assignors.TryGetValue(name, out var assignor))
            return assignor;

        // Default to range if unknown
        return Assignors["range"];
    }

    public static IReadOnlyList<string> AvailableStrategies => Assignors.Keys.ToList();
}

/// <summary>
/// Helper to serialize/deserialize consumer group subscription metadata
/// </summary>
public static class SubscriptionMetadata
{
    /// <summary>
    /// Serialize subscription topics and user data
    /// </summary>
    public static byte[] Serialize(List<string> topics, byte[]? userData = null)
    {
        using var writer = new BigEndianWriter();

        // Version
        writer.Write((short)0);

        // Topics
        writer.Write((short)topics.Count);
        foreach (var topic in topics)
        {
            writer.WriteString(topic);
        }

        // User data
        if (userData != null && userData.Length > 0)
        {
            writer.Write(userData.Length);
            writer.Write(userData);
        }
        else
        {
            writer.Write(0);
        }

        return writer.ToArray();
    }

    /// <summary>
    /// Deserialize subscription metadata
    /// </summary>
    public static (List<string> Topics, byte[] UserData) Deserialize(byte[] data)
    {
        var topics = new List<string>();
        var userData = Array.Empty<byte>();

        if (data.Length < 4)
            return (topics, userData);

        try
        {
            var span = data.AsSpan();
            var pos = 0;

            // Version
            pos += 2;

            // Topics
            var topicCount = BinaryPrimitives.ReadInt16BigEndian(span[pos..]);
            pos += 2;

            for (int i = 0; i < topicCount && pos < span.Length; i++)
            {
                var topicLen = BinaryPrimitives.ReadInt16BigEndian(span[pos..]);
                pos += 2;
                var topic = Encoding.UTF8.GetString(span.Slice(pos, topicLen));
                pos += topicLen;
                topics.Add(topic);
            }

            // User data
            if (pos + 4 <= span.Length)
            {
                var userDataLen = BinaryPrimitives.ReadInt32BigEndian(span[pos..]);
                pos += 4;
                if (userDataLen > 0 && pos + userDataLen <= span.Length)
                {
                    userData = span.Slice(pos, userDataLen).ToArray();
                }
            }
        }
        catch
        {
            // Return empty on parse error
        }

        return (topics, userData);
    }
}

/// <summary>
/// Helper to serialize/deserialize assignment data
/// </summary>
public static class AssignmentData
{
    /// <summary>
    /// Serialize partition assignments
    /// </summary>
    public static byte[] Serialize(List<AssignedPartition> partitions, byte[]? userData = null)
    {
        using var writer = new BigEndianWriter();

        // Version
        writer.Write((short)0);

        // Group by topic
        var grouped = partitions
            .GroupBy(p => p.Topic)
            .OrderBy(g => g.Key)
            .ToList();

        writer.Write((short)grouped.Count);
        foreach (var group in grouped)
        {
            writer.WriteString(group.Key);

            var parts = group.OrderBy(p => p.Partition).ToList();
            writer.Write(parts.Count);
            foreach (var part in parts)
            {
                writer.Write(part.Partition);
            }
        }

        // User data
        if (userData != null && userData.Length > 0)
        {
            writer.Write(userData.Length);
            writer.Write(userData);
        }
        else
        {
            writer.Write(0);
        }

        return writer.ToArray();
    }

    /// <summary>
    /// Deserialize partition assignments
    /// </summary>
    public static (List<AssignedPartition> Partitions, byte[] UserData) Deserialize(byte[] data)
    {
        var partitions = new List<AssignedPartition>();
        var userData = Array.Empty<byte>();

        if (data.Length < 4)
            return (partitions, userData);

        try
        {
            var span = data.AsSpan();
            var pos = 0;

            // Version
            pos += 2;

            // Topics
            var topicCount = BinaryPrimitives.ReadInt16BigEndian(span[pos..]);
            pos += 2;

            for (int i = 0; i < topicCount && pos < span.Length; i++)
            {
                var topicLen = BinaryPrimitives.ReadInt16BigEndian(span[pos..]);
                pos += 2;
                var topic = Encoding.UTF8.GetString(span.Slice(pos, topicLen));
                pos += topicLen;

                var partitionCount = BinaryPrimitives.ReadInt32BigEndian(span[pos..]);
                pos += 4;

                for (int p = 0; p < partitionCount && pos + 4 <= span.Length; p++)
                {
                    var partition = BinaryPrimitives.ReadInt32BigEndian(span[pos..]);
                    pos += 4;
                    partitions.Add(new AssignedPartition(topic, partition));
                }
            }

            // User data
            if (pos + 4 <= span.Length)
            {
                var userDataLen = BinaryPrimitives.ReadInt32BigEndian(span[pos..]);
                pos += 4;
                if (userDataLen > 0 && pos + userDataLen <= span.Length)
                {
                    userData = span.Slice(pos, userDataLen).ToArray();
                }
            }
        }
        catch
        {
            // Return empty on parse error
        }

        return (partitions, userData);
    }
}
