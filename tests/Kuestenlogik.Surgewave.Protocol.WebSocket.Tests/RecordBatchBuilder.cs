using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Core;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

/// <summary>
/// Builds minimal Kafka RecordBatch v2 frames (61-byte header + record payload) that the
/// storage engine accepts: batchLength at bytes 8-11, magic v2 at byte 16, recordCount = 1 at
/// bytes 57-60. All bytes stay in the ASCII range so a frame survives a UTF-8 string round-trip
/// (needed to push one through the JSON produce path).
/// </summary>
internal static class RecordBatchBuilder
{
    /// <summary>Creates a single-record batch whose record section is the given ASCII marker.</summary>
    public static byte[] BuildAsciiRecordBatch(string asciiMarker)
    {
        var payload = Encoding.ASCII.GetBytes(asciiMarker);
        var batch = new byte[KafkaConstants.RecordBatch.HeaderSize + payload.Length];

        var batchLength = batch.Length - 12;
        if (batchLength > 127)
        {
            throw new ArgumentException("Marker too long: batchLength byte must stay in the ASCII range.", nameof(asciiMarker));
        }

        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(KafkaConstants.RecordBatch.LengthOffset, 4), batchLength);
        batch[KafkaConstants.RecordBatch.MagicOffset] = 2;
        BinaryPrimitives.WriteInt32BigEndian(batch.AsSpan(KafkaConstants.RecordBatch.RecordsCountOffset, 4), 1);
        payload.CopyTo(batch.AsSpan(KafkaConstants.RecordBatch.HeaderSize));
        return batch;
    }

    /// <summary>
    /// Maps each batch byte to the char with the same code point (all bytes are &lt;= 0x7F),
    /// so <c>Encoding.UTF8.GetBytes</c> on the result reproduces the original frame.
    /// </summary>
    public static string ToAsciiTransparentString(byte[] batch)
    {
        var chars = new char[batch.Length];
        for (var i = 0; i < batch.Length; i++)
        {
            chars[i] = (char)batch[i];
        }

        return new string(chars);
    }
}
