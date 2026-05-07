namespace Kuestenlogik.Surgewave.Control.Components.Pipeline;

/// <summary>
/// Defines what information is displayed on link labels.
/// </summary>
public enum LinkLabelMode
{
    /// <summary>
    /// Display the internal topic name.
    /// </summary>
    Topic,

    /// <summary>
    /// Display the message count.
    /// </summary>
    MessageCount,

    /// <summary>
    /// Display the throughput (messages per second).
    /// </summary>
    Throughput
}
