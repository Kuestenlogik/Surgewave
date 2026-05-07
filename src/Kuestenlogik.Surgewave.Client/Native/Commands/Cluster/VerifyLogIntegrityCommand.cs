using Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Cluster;

/// <summary>
/// Command to verify log integrity (CRC validation).
/// </summary>
public sealed class VerifyLogIntegrityCommand : ISurgewaveCommand<LogVerificationInfo>
{
    private readonly string? _topic;
    private readonly int? _partition;
    private readonly int _maxCorruptedBatches;
    private readonly bool _includeDetails;

    public VerifyLogIntegrityCommand(
        string? topic = null,
        int? partition = null,
        int maxCorruptedBatches = 0,
        bool includeDetails = true)
    {
        _topic = topic;
        _partition = partition;
        _maxCorruptedBatches = maxCorruptedBatches;
        _includeDetails = includeDetails;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.VerifyLogIntegrity;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_topic ?? string.Empty);
        writer.WriteInt32(_partition ?? -1);
        writer.WriteInt32(_maxCorruptedBatches);
        writer.WriteUInt8(_includeDetails ? (byte)1 : (byte)0);
    }

    public int EstimateRequestSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(_topic ?? "") + // Topic
        4 + // Partition
        4 + // MaxCorruptedBatches
        1;  // IncludeDetails

    public LogVerificationInfo ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var wirePayload = VerifyLogIntegrityResponsePayload.Read(ref reader);

        var details = wirePayload.CorruptedBatchDetails
            .Select(d => new CorruptedBatchDetail(
                d.Topic,
                d.Partition,
                d.BaseOffset,
                d.ExpectedCrc,
                d.ActualCrc,
                d.BatchLength))
            .ToList();

        return new LogVerificationInfo
        {
            BatchesChecked = wirePayload.BatchesChecked,
            CorruptedBatches = wirePayload.CorruptedBatches,
            BytesChecked = wirePayload.BytesChecked,
            CorruptedBytes = wirePayload.CorruptedBytes,
            PartitionsChecked = wirePayload.PartitionsChecked,
            Duration = TimeSpan.FromMilliseconds(wirePayload.DurationMs),
            TopicsVerified = wirePayload.TopicsVerified.ToList(),
            CorruptedBatchDetails = details
        };
    }
}
