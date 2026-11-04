using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Broker.GroupOffsets;

/// <summary>
/// The records of <c>__consumer_offsets</c> beyond offsets and classic group metadata (#219): Kafka's records of the
/// other group types, from key version 3 on, each key leading with the group id. The offset store holds them per group
/// for the coordinator of that type; what they mean is the coordinator's.
/// </summary>
internal static class GroupStateRecords
{
    private const short FirstGroupStateKeyVersion = 3;

    /// <summary>Whether a key is a group-state record's.</summary>
    public static bool IsGroupStateKey(ReadOnlySpan<byte> key) =>
        key.Length >= 4 && BinaryPrimitives.ReadInt16BigEndian(key) >= FirstGroupStateKeyVersion;

    /// <summary>The group a group-state record's key names.</summary>
    public static string GroupOf(ReadOnlySpan<byte> key)
    {
        var length = BinaryPrimitives.ReadInt16BigEndian(key[2..]);
        return length <= 0 ? string.Empty : Encoding.UTF8.GetString(key.Slice(4, length));
    }
}
