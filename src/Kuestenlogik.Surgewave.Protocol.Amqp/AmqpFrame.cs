namespace Kuestenlogik.Surgewave.Protocol.Amqp;

/// <summary>
/// AMQP 0.9.1 frame type constants (octet 0 of every frame).
/// </summary>
internal static class AmqpFrameType
{
    /// <summary>Method frame carrying a class+method and arguments.</summary>
    public const byte Method = 1;

    /// <summary>Content header frame carrying message properties and body size.</summary>
    public const byte Header = 2;

    /// <summary>Content body frame carrying a chunk of the message body.</summary>
    public const byte Body = 3;

    /// <summary>Heartbeat frame (empty payload).</summary>
    public const byte Heartbeat = 8;

    /// <summary>Frame-end octet that must terminate every frame.</summary>
    public const byte FrameEnd = 0xCE;
}

/// <summary>
/// Represents a parsed AMQP 0.9.1 frame.
/// </summary>
/// <param name="Type">Frame type byte (<see cref="AmqpFrameType"/>).</param>
/// <param name="Channel">Channel number (0 for connection-level frames).</param>
/// <param name="Payload">Frame payload (not including frame-end byte).</param>
internal sealed record AmqpFrame(byte Type, ushort Channel, byte[] Payload);
