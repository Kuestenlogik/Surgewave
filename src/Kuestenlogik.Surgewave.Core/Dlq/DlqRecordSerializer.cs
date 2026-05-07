using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Core.Dlq;

/// <summary>
/// Serializes and deserializes DLQ records to/from JSON.
/// </summary>
public static class DlqRecordSerializer
{
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        WriteIndented = false
    };

    /// <summary>
    /// Serialize a DLQ record to JSON bytes.
    /// </summary>
    public static byte[] Serialize(DlqRecord record)
    {
        var dto = new DlqRecordDto
        {
            OriginalTopic = record.OriginalTopic,
            OriginalPartition = record.OriginalPartition,
            OriginalOffset = record.OriginalOffset,
            OriginalKey = record.OriginalKey != null ? Convert.ToBase64String(record.OriginalKey) : null,
            OriginalValue = Convert.ToBase64String(record.OriginalValue),
            OriginalTimestamp = record.OriginalTimestamp,
            OriginalHeaders = record.OriginalHeaders?.ToDictionary(
                kvp => kvp.Key,
                kvp => Convert.ToBase64String(kvp.Value)),
            ExceptionType = record.ExceptionType,
            ExceptionMessage = record.ExceptionMessage,
            StackTrace = record.StackTrace,
            SourceName = record.SourceName,
            SourceType = record.SourceType,
            TaskId = record.TaskId,
            AttemptCount = record.AttemptCount,
            FailedAt = record.FailedAt,
            AdditionalContext = record.AdditionalContext?.ToDictionary(kvp => kvp.Key, kvp => kvp.Value)
        };

        return JsonSerializer.SerializeToUtf8Bytes(dto, Options);
    }

    /// <summary>
    /// Deserialize a DLQ record from JSON bytes.
    /// </summary>
    public static DlqRecord Deserialize(ReadOnlySpan<byte> data)
    {
        var dto = JsonSerializer.Deserialize<DlqRecordDto>(data, Options)
            ?? throw new JsonException("Failed to deserialize DLQ record");

        return new DlqRecord
        {
            OriginalTopic = dto.OriginalTopic,
            OriginalPartition = dto.OriginalPartition,
            OriginalOffset = dto.OriginalOffset,
            OriginalKey = dto.OriginalKey != null ? Convert.FromBase64String(dto.OriginalKey) : null,
            OriginalValue = Convert.FromBase64String(dto.OriginalValue),
            OriginalTimestamp = dto.OriginalTimestamp,
            OriginalHeaders = dto.OriginalHeaders?.ToDictionary(
                kvp => kvp.Key,
                kvp => Convert.FromBase64String(kvp.Value)),
            ExceptionType = dto.ExceptionType,
            ExceptionMessage = dto.ExceptionMessage,
            StackTrace = dto.StackTrace,
            SourceName = dto.SourceName,
            SourceType = dto.SourceType,
            TaskId = dto.TaskId,
            AttemptCount = dto.AttemptCount,
            FailedAt = dto.FailedAt,
            AdditionalContext = dto.AdditionalContext
        };
    }

    /// <summary>
    /// DTO for JSON serialization with base64-encoded binary fields.
    /// </summary>
    private sealed class DlqRecordDto
    {
        public required string OriginalTopic { get; set; }
        public required int OriginalPartition { get; set; }
        public required long OriginalOffset { get; set; }
        public string? OriginalKey { get; set; }
        public required string OriginalValue { get; set; }
        public DateTimeOffset OriginalTimestamp { get; set; }
        public Dictionary<string, string>? OriginalHeaders { get; set; }
        public required string ExceptionType { get; set; }
        public required string ExceptionMessage { get; set; }
        public string? StackTrace { get; set; }
        public required string SourceName { get; set; }
        public required string SourceType { get; set; }
        public string? TaskId { get; set; }
        public int AttemptCount { get; set; }
        public DateTimeOffset FailedAt { get; set; }
        public Dictionary<string, string>? AdditionalContext { get; set; }
    }
}
