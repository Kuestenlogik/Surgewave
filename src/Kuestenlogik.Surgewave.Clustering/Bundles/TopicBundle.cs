using System.IO.Hashing;
using System.Text;

namespace Kuestenlogik.Surgewave.Clustering.Bundles;

/// <summary>
/// Represents a bundle — a hash range that owns a set of topics.
/// Topics are mapped to bundles via consistent hashing of the topic name.
/// </summary>
public sealed class TopicBundle
{
    /// <summary>
    /// Unique identifier for this bundle, e.g. "ns-0x00000000-0x40000000".
    /// </summary>
    public required string BundleId { get; init; }

    /// <summary>
    /// Inclusive start of the hash range owned by this bundle.
    /// </summary>
    public uint HashRangeStart { get; init; }

    /// <summary>
    /// Exclusive end of the hash range owned by this bundle.
    /// </summary>
    public uint HashRangeEnd { get; init; }

    /// <summary>
    /// Broker that currently owns this bundle. -1 means unloaded / unassigned.
    /// </summary>
    public int OwnerBrokerId { get; set; } = -1;

    /// <summary>
    /// Check whether a given hash falls within this bundle's range.
    /// The range is [HashRangeStart, HashRangeEnd).
    /// </summary>
    public bool ContainsHash(uint hash) => hash >= HashRangeStart && hash < HashRangeEnd;

    /// <summary>
    /// Compute a deterministic 32-bit hash for a topic name using XxHash32.
    /// </summary>
    public static uint HashTopic(string topicName)
    {
        return XxHash32.HashToUInt32(Encoding.UTF8.GetBytes(topicName));
    }
}
