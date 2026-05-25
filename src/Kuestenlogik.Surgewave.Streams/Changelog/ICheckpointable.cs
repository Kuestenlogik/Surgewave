namespace Kuestenlogik.Surgewave.Streams.Changelog;

/// <summary>
/// Interface for state stores that support periodic snapshotting/checkpointing.
/// Enables faster recovery by replaying only from the last checkpoint offset instead of the entire changelog.
/// </summary>
public interface ICheckpointable
{
    /// <summary>
    /// Creates a checkpoint (snapshot) of the current store state.
    /// Returns the changelog offset up to which the checkpoint covers.
    /// </summary>
    StoreCheckpoint CreateCheckpoint();

    /// <summary>
    /// Restores the store from a previously created checkpoint.
    /// </summary>
    void RestoreFromCheckpoint(StoreCheckpoint checkpoint);
}
