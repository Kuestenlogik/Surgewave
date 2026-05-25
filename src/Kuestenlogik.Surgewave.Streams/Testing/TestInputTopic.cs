using Kuestenlogik.Surgewave.Streams.Processors;

namespace Kuestenlogik.Surgewave.Streams.Testing;

/// <summary>
/// Test abstraction for piping records into a topology.
/// Wraps serialization and direct source node invocation.
/// </summary>
public sealed class TestInputTopic<TKey, TValue>
{
    private readonly string _topic;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly TopologyTestDriver _driver;
    private long _autoTimestamp;

    internal TestInputTopic(
        string topic,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        TopologyTestDriver driver,
        long startTimestamp = 0,
        long autoAdvanceMs = 1)
    {
        _topic = topic;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _driver = driver;
        _autoTimestamp = startTimestamp > 0 ? startTimestamp : DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        AutoAdvanceMs = autoAdvanceMs;
    }

    /// <summary>
    /// Auto-advance interval for timestamps in milliseconds.
    /// Each PipeInput call advances the timestamp by this amount.
    /// </summary>
    public long AutoAdvanceMs { get; set; }

    /// <summary>
    /// Pipes a key-value pair into the topology.
    /// </summary>
    public void PipeInput(TKey key, TValue value)
    {
        PipeInput(key, value, _autoTimestamp);
        _autoTimestamp += AutoAdvanceMs;
    }

    /// <summary>
    /// Pipes a key-value pair with explicit timestamp.
    /// </summary>
    public void PipeInput(TKey key, TValue value, long timestamp)
    {
        var keyBytes = _keySerde.Serialize(key);
        var valueBytes = _valueSerde.Serialize(value);
        _driver.PipeInput(_topic, keyBytes, valueBytes, timestamp);
    }

    /// <summary>
    /// Pipes a TestRecord into the topology.
    /// </summary>
    public void PipeInput(TestRecord<TKey, TValue> record)
    {
        PipeInput(record.Key, record.Value, record.Timestamp > 0 ? record.Timestamp : _autoTimestamp);
        if (record.Timestamp <= 0)
            _autoTimestamp += AutoAdvanceMs;
    }

    /// <summary>
    /// Pipes a list of key-value pairs into the topology.
    /// </summary>
    public void PipeInputList(IEnumerable<KeyValuePair<TKey, TValue>> records)
    {
        foreach (var (key, value) in records)
        {
            PipeInput(key, value);
        }
    }

    /// <summary>
    /// Pipes a list of TestRecords into the topology.
    /// </summary>
    public void PipeRecordList(IEnumerable<TestRecord<TKey, TValue>> records)
    {
        foreach (var record in records)
        {
            PipeInput(record);
        }
    }
}
