namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Connect;

/// <summary>
/// Wire format for connector task status.
/// Used as part of ConnectorStatusPayload and ConnectorInfoPayload.
/// </summary>
public readonly record struct ConnectorTaskStatusPayload
{
    public int Id { get; init; }
    public string State { get; init; }
    public string WorkerId { get; init; }
    public string? Trace { get; init; }

    /// <summary>
    /// Estimate buffer size needed for this task status.
    /// </summary>
    public int EstimateSize()
    {
        int size = 4; // Id
        size += 2 + System.Text.Encoding.UTF8.GetByteCount(State ?? "");
        size += 2 + System.Text.Encoding.UTF8.GetByteCount(WorkerId ?? "");
        if (Trace != null)
        {
            size += 2 + System.Text.Encoding.UTF8.GetByteCount(Trace);
        }
        return size;
    }
}
