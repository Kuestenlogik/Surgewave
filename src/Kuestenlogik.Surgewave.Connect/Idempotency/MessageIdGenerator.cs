using System.Security.Cryptography;
using System.Text;

namespace Kuestenlogik.Surgewave.Connect.Idempotency;

/// <summary>
/// Generates unique message IDs for deduplication purposes.
/// Provides multiple strategies for creating deduplication keys.
/// </summary>
public static class MessageIdGenerator
{
    /// <summary>
    /// Generates a message ID from the topic-partition-offset coordinates.
    /// This is the most reliable method when consuming from Surgewave/Kafka topics.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    /// <param name="partition">The partition number.</param>
    /// <param name="offset">The message offset.</param>
    /// <returns>A unique message ID.</returns>
    public static string FromOffset(string topic, int partition, long offset)
        => $"{topic}:{partition}:{offset}";

    /// <summary>
    /// Generates a message ID from a SinkRecord's coordinates.
    /// </summary>
    public static string FromRecord(SinkRecord record)
        => FromOffset(record.Topic, record.Partition, record.Offset);

    /// <summary>
    /// Generates a message ID from the record key.
    /// Use when the key is guaranteed to be unique per logical message.
    /// </summary>
    /// <param name="key">The record key bytes.</param>
    /// <returns>A hex-encoded hash of the key.</returns>
    public static string FromKey(byte[]? key)
    {
        if (key == null || key.Length == 0)
            return Guid.NewGuid().ToString("N");

        return Convert.ToHexString(SHA256.HashData(key));
    }

    /// <summary>
    /// Generates a message ID from a string key.
    /// </summary>
    public static string FromKey(string? key)
    {
        if (string.IsNullOrEmpty(key))
            return Guid.NewGuid().ToString("N");

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(key)));
    }

    /// <summary>
    /// Generates a message ID from the record content hash.
    /// Use for content-based deduplication when the same content should not be written twice.
    /// </summary>
    /// <param name="key">The record key (optional).</param>
    /// <param name="value">The record value.</param>
    /// <returns>A hex-encoded hash of the content.</returns>
    public static string FromContent(byte[]? key, byte[] value)
    {
        using var hasher = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        if (key != null && key.Length > 0)
        {
            hasher.AppendData(key);
            hasher.AppendData([(byte)':']); // Separator
        }

        hasher.AppendData(value);

        return Convert.ToHexString(hasher.GetHashAndReset());
    }

    /// <summary>
    /// Generates a message ID from a SinkRecord's content.
    /// </summary>
    public static string FromContent(SinkRecord record)
        => FromContent(record.Key, record.Value);

    /// <summary>
    /// Generates a message ID from a custom header value.
    /// Use when the source system provides a unique identifier in a header.
    /// </summary>
    /// <param name="record">The sink record.</param>
    /// <param name="headerName">The name of the header containing the ID.</param>
    /// <returns>The header value as a string, or a generated ID if the header is missing.</returns>
    public static string FromHeader(SinkRecord record, string headerName)
    {
        if (record.Headers?.TryGetValue(headerName, out var headerValue) == true && headerValue.Length > 0)
        {
            return Encoding.UTF8.GetString(headerValue);
        }

        // Fall back to offset-based ID
        return FromRecord(record);
    }

    /// <summary>
    /// Generates a composite message ID from multiple fields.
    /// Useful when deduplication requires multiple dimensions.
    /// </summary>
    /// <param name="parts">The parts to combine into an ID.</param>
    /// <returns>A combined message ID.</returns>
    public static string Composite(params string[] parts)
        => string.Join(":", parts.Where(p => !string.IsNullOrEmpty(p)));
}
