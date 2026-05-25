using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Maintains bidirectional mapping between primary keys and foreign keys
/// for foreign key table-table joins.
/// Uses <see cref="ReaderWriterLockSlim"/> for better read concurrency.
/// </summary>
internal sealed class ForeignKeySubscriptionStore<TPrimaryKey, TForeignKey> : IDisposable
    where TPrimaryKey : notnull
    where TForeignKey : notnull
{
    // FK -> set of PKs that reference this FK
    private readonly ConcurrentDictionary<TForeignKey, HashSet<TPrimaryKey>> _fkToPks = new();
    // PK -> current FK for this PK
    private readonly ConcurrentDictionary<TPrimaryKey, TForeignKey> _pkToFk = new();

    private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);
    private bool _disposed;

    /// <summary>Gets the total number of PK→FK subscriptions currently tracked.</summary>
    public int Count => _pkToFk.Count; // ConcurrentDictionary.Count is thread-safe

    public void Subscribe(TPrimaryKey pk, TForeignKey fk)
    {
        _lock.EnterWriteLock();
        try
        {
            _pkToFk[pk] = fk;
            var subscribers = _fkToPks.GetOrAdd(fk, _ => []);
            subscribers.Add(pk);
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void Unsubscribe(TPrimaryKey pk, TForeignKey? oldFk)
    {
        _lock.EnterWriteLock();
        try
        {
            _pkToFk.TryRemove(pk, out _);

            if (oldFk != null && _fkToPks.TryGetValue(oldFk, out var subscribers))
            {
                subscribers.Remove(pk);
                if (subscribers.Count == 0)
                    _fkToPks.TryRemove(oldFk, out _);
            }
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public void UpdateSubscription(TPrimaryKey pk, TForeignKey? oldFk, TForeignKey newFk)
    {
        if (oldFk != null && !oldFk.Equals(newFk))
        {
            Unsubscribe(pk, oldFk);
        }

        Subscribe(pk, newFk);
    }

    public IReadOnlySet<TPrimaryKey> GetSubscribers(TForeignKey fk)
    {
        _lock.EnterReadLock();
        try
        {
            if (_fkToPks.TryGetValue(fk, out var subscribers))
                return new HashSet<TPrimaryKey>(subscribers);

            return new HashSet<TPrimaryKey>();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public TForeignKey? GetForeignKey(TPrimaryKey pk)
    {
        _lock.EnterReadLock();
        try
        {
            _pkToFk.TryGetValue(pk, out var fk);
            return fk;
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _lock.Dispose();
    }
}
