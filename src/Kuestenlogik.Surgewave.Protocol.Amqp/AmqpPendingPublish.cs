namespace Kuestenlogik.Surgewave.Protocol.Amqp;

/// <summary>
/// Holds state for an in-progress AMQP Basic.Publish sequence.
/// A publish spans three frames: Method (Basic.Publish) → Header → Body.
/// This mutable bag is shared by the method/header/body handlers within a single connection.
/// </summary>
internal sealed class AmqpPendingPublish
{
    /// <summary>Exchange name from the Basic.Publish method frame (may be empty for default exchange).</summary>
    public string? Exchange { get; set; }

    /// <summary>Routing key from the Basic.Publish method frame.</summary>
    public string? RoutingKey { get; set; }

    /// <summary>Channel number on which the publish was initiated.</summary>
    public ushort Channel { get; set; }

    /// <summary>Accumulated body bytes (sized after the content header frame is received).</summary>
    public byte[]? Body { get; set; }

    /// <summary>Number of body bytes still to arrive.</summary>
    public long RemainingBytes { get; set; }

    /// <summary>True when a Basic.Publish method frame has been received and we are awaiting header + body.</summary>
    public bool IsActive => Exchange is not null;

    /// <summary>Resets all fields so the object can be reused for the next publish.</summary>
    public void Reset()
    {
        Exchange = null;
        RoutingKey = null;
        Body = null;
        RemainingBytes = 0;
        Channel = 0;
    }
}
