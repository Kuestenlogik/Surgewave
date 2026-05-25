using System.Buffers.Binary;

namespace Kuestenlogik.Surgewave.Protocol.Native;

/// <summary>
/// Response header for Surgewave native protocol
/// </summary>
public readonly struct SurgewaveResponseHeader
{
    public SurgewaveProtocolFlags Flags { get; init; }
    public uint RequestId { get; init; }
    public SurgewaveOpCode OpCode { get; init; }
    public SurgewaveErrorCode ErrorCode { get; init; }
    public int PayloadLength { get; init; }

    public const int Size = 14; // flags(1) + reserved(1) + requestId(4) + opCode(2) + errorCode(2) + payloadLength(4)

    public void WriteTo(Span<byte> buffer)
    {
        buffer[0] = (byte)Flags;
        buffer[1] = 0; // Reserved
        BinaryPrimitives.WriteUInt32BigEndian(buffer[2..], RequestId);
        BinaryPrimitives.WriteUInt16BigEndian(buffer[6..], (ushort)OpCode);
        BinaryPrimitives.WriteUInt16BigEndian(buffer[8..], (ushort)ErrorCode);
        BinaryPrimitives.WriteInt32BigEndian(buffer[10..], PayloadLength);
    }

    public static SurgewaveResponseHeader ReadFrom(ReadOnlySpan<byte> buffer)
    {
        return new SurgewaveResponseHeader
        {
            Flags = (SurgewaveProtocolFlags)buffer[0],
            RequestId = BinaryPrimitives.ReadUInt32BigEndian(buffer[2..]),
            OpCode = (SurgewaveOpCode)BinaryPrimitives.ReadUInt16BigEndian(buffer[6..]),
            ErrorCode = (SurgewaveErrorCode)BinaryPrimitives.ReadUInt16BigEndian(buffer[8..]),
            PayloadLength = BinaryPrimitives.ReadInt32BigEndian(buffer[10..])
        };
    }
}
