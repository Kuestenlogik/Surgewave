namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Runs what it is given one at a time, in the order it was given: each waits for the one before it to end, on that
/// one's completion and not on a lock held across the work.
/// </summary>
internal sealed class OneAtATime
{
    private readonly Lock _lock = new();
    private Task _last = Task.CompletedTask;

    /// <summary>Runs <paramref name="work"/> once everything given before it has ended.</summary>
    public async Task<T> RunAsync<T>(Func<Task<T>> work)
    {
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task before;
        lock (_lock)
        {
            before = _last;
            _last = done.Task;
        }

        // Never faulted and never cancelled: it is the completion below, of the one before.
        await before.ConfigureAwait(false);
        try
        {
            return await work().ConfigureAwait(false);
        }
        finally
        {
            done.TrySetResult();
        }
    }
}
