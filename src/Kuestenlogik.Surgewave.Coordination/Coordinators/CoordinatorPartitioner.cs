namespace Kuestenlogik.Surgewave.Coordination.Coordinators;

/// <summary>
/// The partition of a coordinator topic a key belongs to, exactly as Kafka computes it:
/// <c>Utils.abs(key.hashCode()) % partitionCount</c>, with Java's <c>String.hashCode</c> and Kafka's
/// <c>Utils.abs</c> (<c>n &amp; 0x7fffffff</c>, so <c>int.MinValue</c> maps to 0, not to an overflow). The same
/// formula keeps a group on the partition Kafka tooling expects.
/// </summary>
public static class CoordinatorPartitioner
{
    /// <summary>The internal topic that holds group state.</summary>
    public const string GroupTopic = "__consumer_offsets";

    /// <summary>The internal topic that holds transaction state.</summary>
    public const string TransactionTopic = "__transaction_state";

    /// <summary>The internal topic that holds share-partition state (KIP-932, #228).</summary>
    public const string ShareStateTopic = "__share_group_state";

    /// <summary>The coordinator topic's partition for <paramref name="key"/>.</summary>
    public static int PartitionFor(string key, int partitionCount)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(partitionCount, 1);
        return (JavaStringHashCode(key) & 0x7fffffff) % partitionCount;
    }

    /// <summary>Java's <c>String.hashCode</c>: <c>s[0]*31^(n-1) + … + s[n-1]</c> over UTF-16 code units, wrapping.</summary>
    public static int JavaStringHashCode(string value)
    {
        var hash = 0;
        foreach (var c in value)
        {
            hash = unchecked((31 * hash) + c);
        }
        return hash;
    }
}
