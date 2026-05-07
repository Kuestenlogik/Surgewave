namespace Kuestenlogik.Surgewave.Streams.EventTime;

/// <summary>
/// Generates watermarks based on incoming events.
/// </summary>
public interface IWatermarkGenerator<T>
{
    /// <summary>
    /// Called for every event to potentially advance the watermark.
    /// </summary>
    void OnEvent(T element, long eventTimestamp);

    /// <summary>
    /// Called periodically to emit watermarks (for periodic watermark generation).
    /// </summary>
    Watermark GetCurrentWatermark();
}

/// <summary>
/// Output for watermark generation that includes both records and watermarks.
/// </summary>
public interface IWatermarkOutput
{
    /// <summary>
    /// Emits a watermark.
    /// </summary>
    void EmitWatermark(Watermark watermark);

    /// <summary>
    /// Marks an event as late (arrived after the watermark).
    /// </summary>
    void MarkLate();
}
