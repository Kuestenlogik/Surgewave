namespace Kuestenlogik.Surgewave.Broker.KeyValue;

/// <summary>
/// Operation type for a KV entry.
/// </summary>
public enum KvOperation : byte
{
    Put = 0,
    Delete = 1,
    Purge = 2,
}
