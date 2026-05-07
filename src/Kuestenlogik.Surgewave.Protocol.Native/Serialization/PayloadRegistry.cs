using System.Collections.Frozen;

namespace Kuestenlogik.Surgewave.Protocol.Native.Serialization;

/// <summary>
/// Registry mapping OpCodes to payload reader functions.
/// Enables generic deserialization based on opcode.
/// </summary>
public static class PayloadRegistry
{
    private static readonly FrozenDictionary<SurgewaveOpCode, Func<SurgewavePayloadReader, object>> _readers;

    static PayloadRegistry()
    {
        var readers = new Dictionary<SurgewaveOpCode, Func<SurgewavePayloadReader, object>>();
        // Registration happens via RegisterPayload calls during startup
        _readers = readers.ToFrozenDictionary();
    }

    /// <summary>
    /// Check if a reader is registered for the given opcode.
    /// </summary>
    public static bool HasReader(SurgewaveOpCode opCode) => _readers.ContainsKey(opCode);

    /// <summary>
    /// Read a payload for the given opcode.
    /// </summary>
    public static object? ReadPayload(SurgewaveOpCode opCode, ref SurgewavePayloadReader reader)
    {
        if (!_readers.TryGetValue(opCode, out var readerFunc))
            return null;

        return readerFunc(reader);
    }

    /// <summary>
    /// Read a strongly-typed payload.
    /// </summary>
    public static T Read<T>(ref SurgewavePayloadReader reader) where T : ISerializablePayload<T>
        => T.Read(ref reader);
}
