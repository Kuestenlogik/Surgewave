using System.Text;

namespace Kuestenlogik.Surgewave.Client.Partitioning;

/// <summary>
/// Priority level for a message routed through priority lanes.
/// </summary>
public enum MessagePriority
{
    /// <summary>High priority — routed to the first partition range.</summary>
    High = 0,

    /// <summary>Normal priority — routed to the middle partition range (default).</summary>
    Normal = 1,

    /// <summary>Low priority — routed to the last partition range.</summary>
    Low = 2
}

/// <summary>
/// Extension methods for reading and writing <see cref="MessagePriority"/> on message headers.
/// </summary>
public static class MessagePriorityExtensions
{
    /// <summary>Header key used to carry the priority value.</summary>
    public const string HeaderKey = "surgewave-priority";

    private static readonly byte[] _high   = "high"u8.ToArray();
    private static readonly byte[] _normal = "normal"u8.ToArray();
    private static readonly byte[] _low    = "low"u8.ToArray();

    /// <summary>
    /// Returns a header dictionary with <c>surgewave-priority</c> set to the given priority,
    /// merging any existing headers.
    /// </summary>
    public static Dictionary<string, byte[]> WithPriority(
        this Dictionary<string, byte[]>? existing,
        MessagePriority priority)
    {
        var headers = existing != null
            ? new Dictionary<string, byte[]>(existing)
            : new Dictionary<string, byte[]>();

        headers[HeaderKey] = PriorityToBytes(priority);
        return headers;
    }

    /// <summary>
    /// Reads the priority from a header dictionary.
    /// Returns <see cref="MessagePriority.Normal"/> when the header is absent or unrecognised.
    /// </summary>
    public static MessagePriority GetPriority(this Dictionary<string, byte[]>? headers)
    {
        if (headers == null || !headers.TryGetValue(HeaderKey, out var raw))
            return MessagePriority.Normal;

        return ParsePriority(raw);
    }

    /// <summary>
    /// Converts a <see cref="MessagePriority"/> to its wire-format bytes.
    /// </summary>
    public static byte[] PriorityToBytes(MessagePriority priority) => priority switch
    {
        MessagePriority.High   => _high,
        MessagePriority.Low    => _low,
        _                      => _normal
    };

    /// <summary>
    /// Parses a raw header value into a <see cref="MessagePriority"/>.
    /// Unknown values default to <see cref="MessagePriority.Normal"/>.
    /// </summary>
    public static MessagePriority ParsePriority(ReadOnlySpan<byte> raw)
    {
        if (raw.SequenceEqual("high"u8))   return MessagePriority.High;
        if (raw.SequenceEqual("low"u8))    return MessagePriority.Low;
        if (raw.SequenceEqual("normal"u8)) return MessagePriority.Normal;

        // Fallback: decode as UTF-8 string for case-insensitive comparison
        var str = Encoding.UTF8.GetString(raw).Trim().ToLowerInvariant();
        return str switch
        {
            "high"   => MessagePriority.High,
            "low"    => MessagePriority.Low,
            _        => MessagePriority.Normal
        };
    }
}
