using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Schema;

/// <summary>
/// Wire format for CheckCompatibility response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct CompatibilityResultPayload
{
    public bool IsCompatible { get; init; }
    public IReadOnlyList<string>? Messages { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static CompatibilityResultPayload Read(ref SurgewavePayloadReader reader)
    {
        var isCompatible = reader.ReadUInt8() != 0;
        var messageCount = reader.ReadInt32();

        IReadOnlyList<string>? messages = null;
        if (messageCount > 0)
        {
            var messageList = new string[messageCount];
            for (int i = 0; i < messageCount; i++)
            {
                messageList[i] = reader.ReadString() ?? string.Empty;
            }
            messages = messageList;
        }

        return new CompatibilityResultPayload
        {
            IsCompatible = isCompatible,
            Messages = messages
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt8(IsCompatible ? (byte)1 : (byte)0);

        if (Messages != null && Messages.Count > 0)
        {
            writer.WriteInt32(Messages.Count);
            foreach (var message in Messages)
            {
                writer.WriteString(message);
            }
        }
        else
        {
            writer.WriteInt32(0);
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt8(IsCompatible ? (byte)1 : (byte)0);

        if (Messages != null && Messages.Count > 0)
        {
            writer.WriteInt32(Messages.Count);
            foreach (var message in Messages)
            {
                writer.WriteString(message);
            }
        }
        else
        {
            writer.WriteInt32(0);
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        int size =
            1 +     // IsCompatible
            4;      // Message count

        if (Messages != null && Messages.Count > 0)
        {
            foreach (var message in Messages)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(message ?? ""); // length prefix + bytes
            }
        }

        return size;
    }
}
