namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// A handshake for "something changed": a waiter takes <see cref="Next"/>, checks what it waits for, and awaits the task,
/// which completes at the next <see cref="Notify"/>. Taking the task before the check means no change is missed.
/// </summary>
public sealed class ChangeSignal
{
    private TaskCompletionSource _next = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes at the next change.</summary>
    public Task Next => Volatile.Read(ref _next).Task;

    /// <summary>Completes the waiters of this change and starts the next one.</summary>
    public void Notify() =>
        Interlocked.Exchange(ref _next, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult();
}
