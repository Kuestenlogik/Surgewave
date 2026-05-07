namespace Kuestenlogik.Surgewave.Edge;

/// <summary>
/// Specifies the direction of message synchronization between edge and cloud brokers.
/// </summary>
public enum SyncDirection
{
    /// <summary>
    /// Sync messages from the edge broker to the cloud broker only.
    /// </summary>
    EdgeToCloud,

    /// <summary>
    /// Sync messages from the cloud broker to the edge broker only.
    /// </summary>
    CloudToEdge,

    /// <summary>
    /// Sync messages in both directions between edge and cloud brokers.
    /// </summary>
    Bidirectional
}
