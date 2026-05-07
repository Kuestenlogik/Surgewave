namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Optional interface for state stores that support direct raw byte access.
/// Avoids reflection overhead in RemoteQueryServer.
/// </summary>
public interface IRawByteStore
{
    byte[]? GetRaw(byte[] keyBytes);
    List<(byte[] key, byte[] value)> RangeRaw(byte[] fromBytes, byte[] toBytes);
    List<(byte[] key, byte[] value)> AllRaw();
}
