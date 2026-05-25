using System.Buffers.Binary;

namespace Kuestenlogik.Surgewave.Protocol.Native;

/// <summary>
/// Request header for Surgewave native protocol
/// </summary>
public readonly struct SurgewaveRequestHeader
{
    public SurgewaveProtocolFlags Flags { get; init; }
    public uint RequestId { get; init; }
    public SurgewaveOpCode OpCode { get; init; }
    public int PayloadLength { get; init; }

    public void WriteTo(Span<byte> buffer)
    {
        buffer[0] = (byte)Flags;
        buffer[1] = 0; // Reserved
        BinaryPrimitives.WriteUInt32BigEndian(buffer[2..], RequestId);
        BinaryPrimitives.WriteUInt16BigEndian(buffer[6..], (ushort)OpCode);
        BinaryPrimitives.WriteInt32BigEndian(buffer[8..], PayloadLength);
    }

    public static SurgewaveRequestHeader ReadFrom(ReadOnlySpan<byte> buffer)
    {
        return new SurgewaveRequestHeader
        {
            Flags = (SurgewaveProtocolFlags)buffer[0],
            RequestId = BinaryPrimitives.ReadUInt32BigEndian(buffer[2..]),
            OpCode = (SurgewaveOpCode)BinaryPrimitives.ReadUInt16BigEndian(buffer[6..]),
            PayloadLength = BinaryPrimitives.ReadInt32BigEndian(buffer[8..])
        };
    }
}
