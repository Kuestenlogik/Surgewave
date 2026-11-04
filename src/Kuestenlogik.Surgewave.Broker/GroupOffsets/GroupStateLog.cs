using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Broker.Coordination;

namespace Kuestenlogik.Surgewave.Broker.GroupOffsets;

/// <summary>
/// The groups of one type — KIP-848 consumer groups or share groups — kept as their records in <c>__consumer_offsets</c>
/// (#219). A group is read from its records when the coordinator next needs it after its partition was taken over,
/// dropped when another broker serves it, and a change is written — only the records that differ — before it is
/// answered.
/// </summary>
/// <typeparam name="TState">The coordinator's state of a group.</typeparam>
/// <remarks>
/// Read lazily rather than pushed at the coordinator: the offset store starts reading its partitions as soon as it
/// follows the broker's leadership, before the coordinators of the other group types exist. What was taken over shows
/// as a moved load epoch of the group's partition.
/// </remarks>
internal sealed class GroupStateLog<TState>(
    OffsetStore offsets,
    Func<byte[], bool> owns,
    Func<string, IReadOnlyList<(byte[] Key, byte[] Value)>, TState?> read,
    Func<TState, List<(byte[] Key, byte[] Value)>> recordsOf)
    where TState : class
{
    private readonly Dictionary<string, Held> _held = new(StringComparer.Ordinal);

    // One change of a group at a time, from the change to the answer, so its records reach the log in the order its
    // state changed.
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _gates = new(StringComparer.Ordinal);

    /// <summary>Guards <see cref="Groups"/>; the coordinator holds it while it reads or changes a group.</summary>
    public Lock SyncRoot { get; } = new();

    /// <summary>The groups as this broker holds them, under <see cref="SyncRoot"/>.</summary>
    public Dictionary<string, TState> Groups { get; } = new(StringComparer.Ordinal);

    /// <summary>The store the records are in.</summary>
    public OffsetStore Offsets => offsets;

    /// <summary>
    /// The group as this broker holds it now, under <see cref="SyncRoot"/>: read from its records when its partition
    /// was taken over since, dropped when another broker serves it. Null when the group does not exist here.
    /// </summary>
    public TState? Current(string groupId)
    {
        var snapshot = offsets.GroupStateOf(groupId);
        if (snapshot is not { } held)
        {
            Groups.Remove(groupId);
            _held.Remove(groupId);
            return null;
        }
        if (_held.TryGetValue(groupId, out var known) && known.LoadEpoch == held.LoadEpoch)
            return Groups.GetValueOrDefault(groupId);

        var records = held.Records.Where(r => owns(r.Key)).ToList();
        _held[groupId] = new Held(held.LoadEpoch, records.ToDictionary(r => Convert.ToBase64String(r.Key), r => r.Value, StringComparer.Ordinal));
        var group = read(groupId, records);
        if (group is null)
            Groups.Remove(groupId);
        else
            Groups[groupId] = group;
        return group;
    }

    /// <summary>A group this broker starts, under <see cref="SyncRoot"/>; written with the change that made it.</summary>
    public void Add(string groupId, TState group) => Groups[groupId] = group;

    /// <summary>
    /// Puts a group in place as this broker holds it now, for a test that stages a group's state without the requests
    /// that would lead to it. Nothing is written: the log has none of it.
    /// </summary>
    public void Seed(string groupId, TState group)
    {
        lock (SyncRoot)
        {
            Groups[groupId] = group;
            _held[groupId] = new Held(offsets.LoadEpochOf(groupId), new Dictionary<string, byte[]>(StringComparer.Ordinal));
        }
    }

    /// <summary>The groups this broker serves now, read where needed.</summary>
    public IReadOnlyList<TState> All()
    {
        lock (SyncRoot)
        {
            return [.. offsets.GroupStateIds().Concat(Groups.Keys).Distinct(StringComparer.Ordinal).ToList()
                .Select(Current).OfType<TState>()];
        }
    }

    /// <summary>
    /// Makes a change of the group under its gate and <see cref="SyncRoot"/>, writes the records that changed and
    /// answers with the change's result once they are durable — or with <paramref name="refused"/> when they are not.
    /// </summary>
    public async Task<T> ChangeAsync<T>(string groupId, Func<T> change, Func<StateWriteStatus, T> refused, CancellationToken cancellationToken)
    {
        var gate = _gates.GetOrAdd(groupId, _ => new SemaphoreSlim(1, 1));
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            T result;
            List<(byte[] Key, byte[] Value)> changes;
            lock (SyncRoot)
            {
                result = change();
                changes = ChangesOf(groupId);
            }
            if (changes.Count == 0)
                return result;

            var written = await offsets.WriteGroupStateAsync(groupId, changes, cancellationToken).ConfigureAwait(false);
            if (written != StateWriteStatus.Ok)
                return refused(written);
            lock (SyncRoot)
                Remember(groupId, changes);
            return result;
        }
        finally
        {
            gate.Release();
        }
    }

    // Under SyncRoot: the records that differ from what the log has of the group — changed ones, and tombstones for
    // records gone.
    private List<(byte[] Key, byte[] Value)> ChangesOf(string groupId)
    {
        if (!_held.TryGetValue(groupId, out var held))
            return [];
        var now = Groups.TryGetValue(groupId, out var group) ? recordsOf(group) : [];
        var changes = new List<(byte[] Key, byte[] Value)>();
        var present = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (key, value) in now)
        {
            var id = Convert.ToBase64String(key);
            present.Add(id);
            if (!held.Written.TryGetValue(id, out var written) || !written.AsSpan().SequenceEqual(value))
                changes.Add((key, value));
        }
        foreach (var id in held.Written.Keys.Where(id => !present.Contains(id)))
            changes.Add((Convert.FromBase64String(id), []));
        return changes;
    }

    // Under SyncRoot: the changes are in the log.
    private void Remember(string groupId, List<(byte[] Key, byte[] Value)> changes)
    {
        if (!_held.TryGetValue(groupId, out var held))
            return;
        foreach (var (key, value) in changes)
        {
            var id = Convert.ToBase64String(key);
            if (value.Length == 0)
                held.Written.Remove(id);
            else
                held.Written[id] = value;
        }
    }

    private sealed record Held(int LoadEpoch, Dictionary<string, byte[]> Written);
}
