namespace Kuestenlogik.Surgewave.Clustering.Raft;

/// <summary>
/// What a component's writes pass through so that its stop is a handshake: a write is admitted until the gate is
/// closed, and closing returns once the writes admitted before are done. After that nothing is written, whatever of
/// the component's work is still running.
/// </summary>
/// <remarks>
/// Cancelling such work does not do it: a write under way when the stop is asked for is not undone by the
/// cancellation, and a write that only reads the cancellation before it starts can still land after the stop has
/// returned. A Raft node and its controller wrote to <c>raft-log.bin</c> after they were disposed — the controller's
/// own registration and the internal topics of a broker that had just taken office — into a file the broker started
/// next on that directory had read by then, or was reading.
/// </remarks>
internal sealed class WriteGate
{
    private readonly Lock _lock = new();
    private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _admitted;
    private bool _closed;

    /// <summary>
    /// Admits one write, unless the gate is closed. An admitted write ends with <see cref="Exit"/>.
    /// </summary>
    public bool TryEnter()
    {
        lock (_lock)
        {
            if (_closed)
                return false;

            _admitted++;
            return true;
        }
    }

    /// <summary>
    /// Ends a write <see cref="TryEnter"/> admitted.
    /// </summary>
    public void Exit()
    {
        lock (_lock)
        {
            if (--_admitted == 0 && _closed)
                _drained.TrySetResult();
        }
    }

    /// <summary>
    /// Admits no further write and completes when those admitted are done. The same on every call.
    /// </summary>
    public Task CloseAsync()
    {
        lock (_lock)
        {
            _closed = true;
            if (_admitted == 0)
                _drained.TrySetResult();
        }

        return _drained.Task;
    }
}
