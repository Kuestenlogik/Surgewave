namespace Kuestenlogik.Surgewave.Core.Serialization;

/// <summary>
/// Auto-detects message content type from payload bytes when no content-type header is set.
/// </summary>
public static class ContentTypeDetector
{
    /// <summary>
    /// Detect the content type of a message payload.
    /// </summary>
    /// <param name="payload">The raw message bytes to inspect.</param>
    /// <returns>A content type string identifying the detected format.</returns>
    public static string Detect(ReadOnlySpan<byte> payload)
    {
        if (payload.IsEmpty) return ContentTypes.OctetStream;

        // Confluent Schema Registry wire format (magic byte 0x00 + 4-byte schema ID)
        if (payload.Length >= 5 && payload[0] == 0x00)
            return "application/x-confluent-schema"; // needs schema registry lookup

        // MessagePack detection: fixmap (0x80-0x8F), fixarray (0x90-0x9F),
        // map16/map32 (0xDE-0xDF), array16/array32 (0xDC-0xDD)
        if (payload[0] is >= 0x80 and <= 0x8F  // fixmap
            or >= 0x90 and <= 0x9F              // fixarray
            or >= 0xDC and <= 0xDF)             // map/array 16/32
            return ContentTypes.MessagePack;

        // JSON detection (starts with { or [)
        if (payload[0] == '{' || payload[0] == '[')
            return ContentTypes.Json;

        // UTF-8 text detection (printable ASCII range)
        if (payload[0] >= 0x20 && payload[0] < 0x7F)
            return "text/plain";

        return ContentTypes.OctetStream;
    }
}
