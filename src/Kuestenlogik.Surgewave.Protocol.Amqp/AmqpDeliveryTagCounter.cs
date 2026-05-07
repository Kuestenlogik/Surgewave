namespace Kuestenlogik.Surgewave.Protocol.Amqp;

/// <summary>
/// Mutable counter for AMQP delivery tags, shared across channels on a single connection.
/// </summary>
internal sealed class AmqpDeliveryTagCounter
{
    private ulong _value;

    /// <summary>Atomically increments and returns the new delivery tag.</summary>
    public ulong Next() => Interlocked.Increment(ref _value);

    /// <summary>Current value without incrementing.</summary>
    public ulong Current => Volatile.Read(ref _value);
}
