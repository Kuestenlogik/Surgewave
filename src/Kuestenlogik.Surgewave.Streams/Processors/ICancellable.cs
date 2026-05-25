namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Handle to cancel a scheduled punctuation.
/// </summary>
public interface ICancellable
{
    void Cancel();
}
