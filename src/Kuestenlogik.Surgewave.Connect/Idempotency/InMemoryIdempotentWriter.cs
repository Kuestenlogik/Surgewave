using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Connect.Idempotency;

/// <summary>
/// In-memory reference implementation of IIdempotentWriter.
/// Useful for testing and as a template for custom implementations.
/// </summary>
/// <typeparam name="TKey">The type of the record key.</typeparam>
/// <typeparam name="TValue">The type of the record value.</typeparam>
public class InMemoryIdempotentWriter<TKey, TValue> : IIdempotentWriter<TKey, TValue>
    where TKey : notnull
{
    private readonly ConcurrentDictionary<TKey, TValue> _store = new();
    private readonly IEqualityComparer<TValue>? _valueComparer;

    /// <summary>
    /// Creates an in-memory idempotent writer.
    /// </summary>
    /// <param name="valueComparer">Optional comparer for detecting value changes. If null, uses default equality.</param>
    public InMemoryIdempotentWriter(IEqualityComparer<TValue>? valueComparer = null)
    {
        _valueComparer = valueComparer;
    }

    /// <summary>
    /// Gets the current store contents for inspection.
    /// </summary>
    public IReadOnlyDictionary<TKey, TValue> Store => _store;

    public Task<WriteResult> WriteAsync(TKey key, TValue value, WriteMetadata metadata, CancellationToken cancellationToken = default)
    {
        if (_store.TryGetValue(key, out var existing))
        {
            var areEqual = _valueComparer != null
                ? _valueComparer.Equals(existing, value)
                : EqualityComparer<TValue>.Default.Equals(existing, value);

            if (areEqual)
            {
                return Task.FromResult(WriteResult.Skipped());
            }

            _store[key] = value;
            return Task.FromResult(WriteResult.Updated());
        }

        _store[key] = value;
        return Task.FromResult(WriteResult.Inserted());
    }

    public Task<IReadOnlyList<WriteResult>> WriteBatchAsync(IReadOnlyList<WriteRecord<TKey, TValue>> records, CancellationToken cancellationToken = default)
    {
        var results = new List<WriteResult>(records.Count);

        foreach (var record in records)
        {
            var result = WriteAsync(record.Key, record.Value, record.Metadata, cancellationToken).GetAwaiter().GetResult();
            results.Add(result);
        }

        return Task.FromResult<IReadOnlyList<WriteResult>>(results);
    }

    public Task<bool> ExistsAsync(TKey key, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_store.ContainsKey(key));
    }

    public Task<bool> DeleteAsync(TKey key, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(_store.TryRemove(key, out _));
    }

    public ValueTask DisposeAsync()
    {
        _store.Clear();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }
}
