using System.Collections.Concurrent;
using System.Threading.Tasks.Sources;

namespace Kuestenlogik.Surgewave.Core.Pipeline;

/// <summary>
/// A pooled IValueTaskSource implementation for high-throughput async completion.
/// Reduces allocation overhead by reusing completion source objects.
/// </summary>
public sealed class PooledCompletionSource<T> : IValueTaskSource<T>
{
    private static readonly ConcurrentQueue<PooledCompletionSource<T>> Pool = new();
    private const int MaxPoolSize = 1024;
    private static int s_poolSize;

    private ManualResetValueTaskSourceCore<T> _core;

    private PooledCompletionSource() { }

    /// <summary>
    /// Rent a completion source from the pool or create a new one.
    /// </summary>
    public static PooledCompletionSource<T> Rent()
    {
        if (Pool.TryDequeue(out var source))
        {
            Interlocked.Decrement(ref s_poolSize);
            return source;
        }

        return new PooledCompletionSource<T>();
    }

    /// <summary>
    /// Return the completion source to the pool for reuse.
    /// </summary>
    public void Return()
    {
        _core.Reset();

        if (Interlocked.Increment(ref s_poolSize) <= MaxPoolSize)
        {
            Pool.Enqueue(this);
        }
        else
        {
            Interlocked.Decrement(ref s_poolSize);
        }
    }

    /// <summary>
    /// Get a ValueTask that completes when SetResult or SetException is called.
    /// </summary>
    public ValueTask<T> ValueTask => new(this, _core.Version);

    /// <summary>
    /// Set the result and complete the ValueTask.
    /// </summary>
    public void SetResult(T result) => _core.SetResult(result);

    /// <summary>
    /// Set an exception and complete the ValueTask.
    /// </summary>
    public void SetException(Exception exception) => _core.SetException(exception);

    /// <summary>
    /// Try to set the result. Returns false if already completed.
    /// </summary>
    public bool TrySetResult(T result)
    {
        try
        {
            _core.SetResult(result);
            return true;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    /// <summary>
    /// Try to set an exception. Returns false if already completed.
    /// </summary>
    public bool TrySetException(Exception exception)
    {
        try
        {
            _core.SetException(exception);
            return true;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    // IValueTaskSource<T> implementation
    T IValueTaskSource<T>.GetResult(short token) => _core.GetResult(token);

    ValueTaskSourceStatus IValueTaskSource<T>.GetStatus(short token) => _core.GetStatus(token);

    void IValueTaskSource<T>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        => _core.OnCompleted(continuation, state, token, flags);
}
