namespace Kuestenlogik.Surgewave.Broker.GroupStatePersistence;

/// <summary>
/// Generic persistence backplane for KIP-848 / KIP-932 group state. Each
/// group is one row keyed by group id; saves are debounced so a busy
/// coordinator doesn't fsync on every heartbeat. The implementation is
/// expected to be safe for concurrent <see cref="Save"/> calls and to
/// handle missing / corrupt rows on <see cref="LoadAll"/> by skipping them
/// — a partial recovery is always better than refusing to start.
/// </summary>
/// <typeparam name="TState">The serialised state shape.</typeparam>
public interface IGroupStateStore<TState>
{
    /// <summary>Marks a group as dirty so it gets flushed on the next debounce tick.</summary>
    void Save(string groupId, TState state);

    /// <summary>Removes a group's persisted state, e.g. after the last member leaves.</summary>
    void Delete(string groupId);

    /// <summary>Loads every persisted group up-front. Called once at coordinator startup.</summary>
    IReadOnlyDictionary<string, TState> LoadAll();
}
