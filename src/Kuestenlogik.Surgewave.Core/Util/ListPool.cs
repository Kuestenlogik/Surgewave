using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// High-performance pool for List&lt;T&gt; objects to reduce GC pressure in hot paths.
/// Lists are cleared before being returned to the pool and reused.
/// </summary>
public static class ListPool<T>
{
    private static readonly ConcurrentBag<List<T>> _pool = new();
    private const int MaxPoolSize = 64;

    /// <summary>
    /// Rent a list from the pool with specified initial capacity.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static List<T> Rent(int capacity = 16)
    {
        if (_pool.TryTake(out var list))
        {
            if (list.Capacity < capacity)
            {
                list.Capacity = capacity;
            }
            return list;
        }
        return new List<T>(capacity);
    }

    /// <summary>
    /// Return a list to the pool for reuse.
    /// The list is cleared before being added to the pool.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void Return(List<T> list)
    {
        if (list == null || _pool.Count >= MaxPoolSize)
            return;

        list.Clear();
        _pool.Add(list);
    }
}

/// <summary>
/// Disposable wrapper for pooled lists that returns the list on dispose.
/// Usage: using var messages = PooledList&lt;Message&gt;.Rent(count);
/// </summary>
public readonly struct PooledList<T> : IDisposable
{
    public List<T> List { get; }

    private PooledList(List<T> list) => List = list;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static PooledList<T> Rent(int capacity = 16)
    {
        return new PooledList<T>(ListPool<T>.Rent(capacity));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose()
    {
        ListPool<T>.Return(List);
    }

    // Allow using the list directly
    public static implicit operator List<T>(PooledList<T> pooled) => pooled.List;
}
