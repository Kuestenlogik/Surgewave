namespace Kuestenlogik.Surgewave.Client.Consumer;

/// <summary>
/// Where to start consuming if no committed offset exists.
/// </summary>
public enum AutoOffsetReset
{
    /// <summary>
    /// Start from the earliest available offset.
    /// </summary>
    Earliest,

    /// <summary>
    /// Start from the latest offset (new messages only).
    /// </summary>
    Latest
}
