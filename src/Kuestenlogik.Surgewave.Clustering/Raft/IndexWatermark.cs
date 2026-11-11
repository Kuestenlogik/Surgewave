namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// A monotonic log index that callers can wait on: <see cref="WaitAsync"/> completes once
/// <see cref="Advance"/> has moved the index to or past the one asked for (#201).
/// </summary>
/// <remarks>
/// Built for the hot path. <see cref="Advance"/> runs on every commit-index move; while nobody waits it takes an
/// uncontended lock, stores a long and returns, without allocating. A waiter is allocated only by a caller that
/// actually has to wait, and leaves the list the moment it completes or its token is cancelled, so neither an
/// index that is never reached nor a cancelled caller leaves anything behind.
/// </remarks>
internal sealed class IndexWatermark
{
    private readonly object _lock = new();
    private readonly List<Waiter> _waiters = [];
    private long _value;

    public IndexWatermark(long initial = 0)
    {
        _value = initial;
    }

    /// <summary>The index reached so far.</summary>
    public long Value
    {
        get { lock (_lock) return _value; }
    }

    /// <summary>The callers currently waiting; for tests that check nothing is left behind.</summary>
    internal int WaiterCount
    {
        get { lock (_lock) return _waiters.Count; }
    }

    /// <summary>
    /// Moves the index to <paramref name="value"/> and completes every waiter it reaches. A lower value than the
    /// current one is ignored: the index never moves back.
    /// </summary>
    public void Advance(long value)
    {
        lock (_lock)
        {
            if (value <= _value)
                return;

            _value = value;

            // Backwards, so removing in place keeps the walk valid and allocates nothing.
            for (var i = _waiters.Count - 1; i >= 0; i--)
            {
                var waiter = _waiters[i];
                if (waiter.Index > value)
                    continue;

                _waiters.RemoveAt(i);
                // Unregister, not Dispose: Dispose would block on a cancellation callback already running, and that
                // callback waits for this lock.
                waiter.Registration.Unregister();
                // The continuations run asynchronously, so completing under the lock runs no caller code here.
                waiter.Completion.TrySetResult();
            }
        }
    }

    /// <summary>
    /// Completes when the index reaches <paramref name="index"/>: at once if it already has, even with a cancelled
    /// token. Cancelled with <paramref name="ct"/> otherwise.
    /// </summary>
    public Task WaitAsync(long index, CancellationToken ct)
    {
        lock (_lock)
        {
            if (_value >= index)
                return Task.CompletedTask;

            if (ct.IsCancellationRequested)
                return Task.FromCanceled(ct);

            var waiter = new Waiter(index);
            _waiters.Add(waiter);
            if (ct.CanBeCanceled)
            {
                // Registered under the lock: a token cancelled meanwhile runs the callback inline, which takes this
                // (re-entrant) lock and removes the waiter it just added.
                waiter.Registration = ct.Register(static (state, token) =>
                {
                    var (self, w) = ((IndexWatermark, Waiter))state!;
                    self.Cancel(w, token);
                }, (this, waiter));
            }

            return waiter.Completion.Task;
        }
    }

    private void Cancel(Waiter waiter, CancellationToken ct)
    {
        lock (_lock)
        {
            // Not found: Advance completed it first.
            if (!_waiters.Remove(waiter))
                return;
        }

        waiter.Completion.TrySetCanceled(ct);
    }

    private sealed class Waiter(long index)
    {
        public long Index { get; } = index;

        public TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public CancellationTokenRegistration Registration { get; set; }
    }
}
