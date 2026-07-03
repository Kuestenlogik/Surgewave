namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// Outcome of decoding a record batch with <see cref="RecordBatchBrowser"/>.
/// Carries enough batch-level metadata for callers to render an honest placeholder
/// when a compressed batch could not be decoded.
/// </summary>
public sealed class RecordBatchBrowseResult
{
    /// <summary>The decoded records; empty when the batch was malformed or undecodable.</summary>
    public required IReadOnlyList<BrowsedRecord> Records { get; init; }

    /// <summary>Whether the batch was stored compressed (records were decompressed transparently).</summary>
    public required bool IsCompressed { get; init; }

    /// <summary>Compression type from the batch attributes (bits 0-2), see <see cref="CompressionCodec"/>.</summary>
    public required int CompressionType { get; init; }

    /// <summary>Record count as declared in the batch header (even when decoding failed).</summary>
    public required int HeaderRecordCount { get; init; }

    /// <summary>True when the batch is compressed with an unsupported codec or the payload failed to decompress.</summary>
    public required bool DecompressionFailed { get; init; }

    /// <summary>Base offset from the batch header.</summary>
    public required long BaseOffset { get; init; }

    /// <summary>First timestamp from the batch header, Unix milliseconds.</summary>
    public required long FirstTimestampMs { get; init; }

    /// <summary>Result for inputs too short to be a v2 record batch.</summary>
    public static RecordBatchBrowseResult Empty { get; } = new()
    {
        Records = [],
        IsCompressed = false,
        CompressionType = KafkaConstants.Compression.None,
        HeaderRecordCount = 0,
        DecompressionFailed = false,
        BaseOffset = 0,
        FirstTimestampMs = 0,
    };
}
