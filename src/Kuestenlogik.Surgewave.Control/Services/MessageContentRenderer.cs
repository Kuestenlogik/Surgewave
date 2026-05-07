using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Core.Serialization;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Renders message payloads as human-readable text based on detected content type.
/// Used by the Message Browser to display decoded message content.
/// </summary>
public static class MessageContentRenderer
{
    private static readonly JsonSerializerOptions IndentedOptions = new() { WriteIndented = true };

    /// <summary>
    /// Attempts to render binary message payload as readable text.
    /// Returns null if the format cannot be decoded (fallback to raw display).
    /// </summary>
    public static string? TryRender(byte[] payload, string? contentType)
    {
        if (payload.Length == 0 || contentType == null)
            return null;

        try
        {
            return contentType switch
            {
                ContentTypes.Json => RenderJson(payload),
                "application/x-confluent-schema" => RenderSchemaRegistryPayload(payload),
                ContentTypes.MessagePack => RenderMessagePack(payload),
                "text/plain" => Encoding.UTF8.GetString(payload),
                _ => null // Unknown format — caller shows raw/hex
            };
        }
        catch
        {
            return null; // Decode failed — caller shows raw/hex
        }
    }

    private static string RenderJson(byte[] payload)
    {
        var doc = JsonDocument.Parse(payload);
        return JsonSerializer.Serialize(doc, IndentedOptions);
    }

    private static string RenderSchemaRegistryPayload(byte[] payload)
    {
        if (payload.Length < 5) return null!;

        // Extract schema ID from wire format header
        var schemaId = (payload[1] << 24) | (payload[2] << 16) | (payload[3] << 8) | payload[4];

        // Skip header (5 bytes) — try to decode the remaining payload
        var dataPayload = payload.AsSpan(5);

        // Try JSON first (common for JSON Schema format)
        if (dataPayload.Length > 0 && (dataPayload[0] == '{' || dataPayload[0] == '['))
        {
            try
            {
                var jsonDoc = JsonDocument.Parse(dataPayload.ToArray());
                var json = JsonSerializer.Serialize(jsonDoc, IndentedOptions);
                return $"// Schema ID: {schemaId}\n{json}";
            }
            catch { /* not JSON — try next */ }
        }

        // Skip varint (Protobuf message index) and try JSON on remaining
        var offset = 0;
        while (offset < dataPayload.Length && (dataPayload[offset] & 0x80) != 0) offset++;
        offset++; // skip last varint byte

        if (offset < dataPayload.Length)
        {
            var afterVarint = dataPayload[offset..];
            if (afterVarint.Length > 0 && (afterVarint[0] == '{' || afterVarint[0] == '['))
            {
                try
                {
                    var jsonDoc = JsonDocument.Parse(afterVarint.ToArray());
                    var json = JsonSerializer.Serialize(jsonDoc, IndentedOptions);
                    return $"// Schema ID: {schemaId}\n{json}";
                }
                catch { /* not JSON after varint — give up on text decoding */ }
            }
        }

        // Can't decode payload as text — show schema ID + hex summary
        return $"// Schema ID: {schemaId}, Payload: {dataPayload.Length} bytes (binary)";
    }

    private static string RenderMessagePack(byte[] payload)
    {
        // Use MessagePack → JSON conversion
        var json = MessagePack.MessagePackSerializer.ConvertToJson(payload);
        // Pretty-print the JSON
        var doc = JsonDocument.Parse(json);
        return JsonSerializer.Serialize(doc, IndentedOptions);
    }
}
