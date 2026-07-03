namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// A single record decoded from a Kafka v2 RecordBatch by <see cref="RecordBatchBrowser"/>.
/// Key and value are raw bytes — presentation (UTF-8, Base64, JSON, ...) is the caller's concern.
/// </summary>
/// <param name="Offset">Absolute offset (base offset + offset delta).</param>
/// <param name="TimestampMs">Absolute timestamp in Unix milliseconds (first timestamp + delta).</param>
/// <param name="Key">Record key, or <c>null</c> when the key was written as null.</param>
/// <param name="Value">Record value, or <c>null</c> when the value was written as null (tombstone).</param>
/// <param name="Headers">Record headers decoded as UTF-8 strings; empty when the record has none.</param>
public sealed record BrowsedRecord(
    long Offset,
    long TimestampMs,
    byte[]? Key,
    byte[]? Value,
    Dictionary<string, string> Headers);
