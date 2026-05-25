using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Streams.Testing;

/// <summary>
/// Raw output record captured from sink nodes.
/// </summary>
public readonly record struct RawOutputRecord(byte[] Key, byte[] Value, long Timestamp);

/// <summary>
/// Test abstraction for reading output records from a topology.
/// Captures records written to sink topics via OutputHandler callbacks.
/// </summary>
public sealed class TestOutputTopic<TKey, TValue>
{
    private readonly string _topic;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly ConcurrentQueue<RawOutputRecord> _buffer;

    internal TestOutputTopic(
        string topic,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        ConcurrentQueue<RawOutputRecord> buffer)
    {
        _topic = topic;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _buffer = buffer;
    }

    /// <summary>
    /// Returns true if no more records are available.
    /// </summary>
    public bool IsEmpty => _buffer.IsEmpty;

    /// <summary>
    /// Returns the number of records available.
    /// </summary>
    public int QueueSize => _buffer.Count;

    /// <summary>
    /// Reads the next key-value pair. Throws if empty.
    /// </summary>
    public KeyValuePair<TKey, TValue> ReadKeyValue()
    {
        if (!_buffer.TryDequeue(out var raw))
            throw new InvalidOperationException($"No more records available on output topic '{_topic}'");

        var key = _keySerde.Deserialize(raw.Key);
        var value = _valueSerde.Deserialize(raw.Value);
        return new KeyValuePair<TKey, TValue>(key, value);
    }

    /// <summary>
    /// Reads the next record with full metadata. Throws if empty.
    /// </summary>
    public TestRecord<TKey, TValue> ReadRecord()
    {
        if (!_buffer.TryDequeue(out var raw))
            throw new InvalidOperationException($"No more records available on output topic '{_topic}'");

        var key = _keySerde.Deserialize(raw.Key);
        var value = _valueSerde.Deserialize(raw.Value);
        return new TestRecord<TKey, TValue>(key, value, raw.Timestamp);
    }

    /// <summary>
    /// Reads the next value only (ignores key). Throws if empty.
    /// </summary>
    public TValue ReadValue()
    {
        if (!_buffer.TryDequeue(out var raw))
            throw new InvalidOperationException($"No more records available on output topic '{_topic}'");

        return _valueSerde.Deserialize(raw.Value);
    }

    /// <summary>
    /// Reads all remaining key-value pairs.
    /// </summary>
    public List<KeyValuePair<TKey, TValue>> ReadKeyValuesToList()
    {
        var results = new List<KeyValuePair<TKey, TValue>>();
        while (_buffer.TryDequeue(out var raw))
        {
            var key = _keySerde.Deserialize(raw.Key);
            var value = _valueSerde.Deserialize(raw.Value);
            results.Add(new KeyValuePair<TKey, TValue>(key, value));
        }
        return results;
    }

    /// <summary>
    /// Reads all remaining records with full metadata.
    /// </summary>
    public List<TestRecord<TKey, TValue>> ReadRecordsToList()
    {
        var results = new List<TestRecord<TKey, TValue>>();
        while (_buffer.TryDequeue(out var raw))
        {
            var key = _keySerde.Deserialize(raw.Key);
            var value = _valueSerde.Deserialize(raw.Value);
            results.Add(new TestRecord<TKey, TValue>(key, value, raw.Timestamp));
        }
        return results;
    }

    /// <summary>
    /// Reads all remaining values (ignores keys).
    /// </summary>
    public List<TValue> ReadValuesToList()
    {
        var results = new List<TValue>();
        while (_buffer.TryDequeue(out var raw))
        {
            results.Add(_valueSerde.Deserialize(raw.Value));
        }
        return results;
    }
}
