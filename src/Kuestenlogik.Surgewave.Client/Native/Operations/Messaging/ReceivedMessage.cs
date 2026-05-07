using System.Text;

namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// A received message.
/// </summary>
public record ReceivedMessage(
    long Offset,
    long Timestamp,
    byte[]? Key,
    byte[] Value,
    IReadOnlyDictionary<string, byte[]>? Headers = null)
{
    public string? KeyString => Key != null ? Encoding.UTF8.GetString(Key) : null;
    public string ValueString => Encoding.UTF8.GetString(Value);
}
