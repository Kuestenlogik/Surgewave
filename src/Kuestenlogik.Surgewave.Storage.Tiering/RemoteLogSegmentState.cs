namespace Kuestenlogik.Surgewave.Storage.Tiering;

/// <summary>
/// State of a remote log segment during its lifecycle.
/// Mirrors Kafka's RemoteLogSegmentState (KIP-405).
///
/// State transitions:
/// COPY_SEGMENT_STARTED → COPY_SEGMENT_FINISHED → DELETE_SEGMENT_STARTED → DELETE_SEGMENT_FINISHED
///                    ↓
///              DELETE_SEGMENT_STARTED (direct deletion)
/// </summary>
public enum RemoteLogSegmentState
{
    /// <summary>
    /// Segment copy to remote storage has started but not yet completed.
    /// Segment is not available for reads in this state.
    /// </summary>
    CopySegmentStarted = 0,

    /// <summary>
    /// Segment copy to remote storage has completed successfully.
    /// Segment is now available for reads.
    /// </summary>
    CopySegmentFinished = 1,

    /// <summary>
    /// Segment deletion from remote storage has started.
    /// Segment is not available for reads in this state.
    /// </summary>
    DeleteSegmentStarted = 2,

    /// <summary>
    /// Segment deletion from remote storage has completed.
    /// Terminal state - segment is removed from metadata.
    /// </summary>
    DeleteSegmentFinished = 3
}

/// <summary>
/// State of a remote partition deletion.
/// Mirrors Kafka's RemotePartitionDeleteState (KIP-405).
/// </summary>
public enum RemotePartitionDeleteState
{
    /// <summary>
    /// Partition marked for deletion but not started.
    /// </summary>
    DeletePartitionMarked = 0,

    /// <summary>
    /// Partition deletion in progress.
    /// </summary>
    DeletePartitionStarted = 1,

    /// <summary>
    /// Partition deletion completed.
    /// </summary>
    DeletePartitionFinished = 2
}

/// <summary>
/// Index types that can be stored in remote storage.
/// Mirrors Kafka's RemoteStorageManager.IndexType.
/// </summary>
public enum RemoteIndexType
{
    /// <summary>Offset index file (.index)</summary>
    Offset = 0,

    /// <summary>Timestamp index file (.timeindex)</summary>
    Timestamp = 1,

    /// <summary>Producer snapshot file (.snapshot)</summary>
    ProducerSnapshot = 2,

    /// <summary>Transaction index file (.txnindex)</summary>
    Transaction = 3,

    /// <summary>Leader epoch index data</summary>
    LeaderEpoch = 4
}

/// <summary>
/// Extension methods for state validation and transitions.
/// </summary>
public static class RemoteLogSegmentStateExtensions
{
    /// <summary>
    /// Get valid next states from the current state.
    /// </summary>
    public static RemoteLogSegmentState[] ValidTransitions(this RemoteLogSegmentState state)
    {
        return state switch
        {
            RemoteLogSegmentState.CopySegmentStarted => [
                RemoteLogSegmentState.CopySegmentFinished,
                RemoteLogSegmentState.DeleteSegmentStarted // Can delete before copy finishes
            ],
            RemoteLogSegmentState.CopySegmentFinished => [
                RemoteLogSegmentState.DeleteSegmentStarted
            ],
            RemoteLogSegmentState.DeleteSegmentStarted => [
                RemoteLogSegmentState.DeleteSegmentFinished
            ],
            RemoteLogSegmentState.DeleteSegmentFinished => [], // Terminal state
            _ => []
        };
    }

    /// <summary>
    /// Check if transition to the target state is valid.
    /// </summary>
    public static bool CanTransitionTo(this RemoteLogSegmentState current, RemoteLogSegmentState target)
    {
        // Allow self-transitions for idempotency (like Kafka)
        if (current == target)
            return true;

        return current.ValidTransitions().Contains(target);
    }

    /// <summary>
    /// Check if segment is available for reads.
    /// Only CopySegmentFinished state allows reads.
    /// </summary>
    public static bool IsReadable(this RemoteLogSegmentState state)
    {
        return state == RemoteLogSegmentState.CopySegmentFinished;
    }

    /// <summary>
    /// Check if segment should be visible for cleanup operations.
    /// </summary>
    public static bool IsVisibleForCleanup(this RemoteLogSegmentState state)
    {
        return state != RemoteLogSegmentState.DeleteSegmentFinished;
    }
}
