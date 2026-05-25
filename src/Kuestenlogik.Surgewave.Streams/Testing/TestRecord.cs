namespace Kuestenlogik.Surgewave.Streams.Testing;

/// <summary>
/// A test record with key, value, and optional timestamp/headers.
/// Used by TestInputTopic and TestOutputTopic.
/// </summary>
public sealed class TestRecord<TKey, TValue>
{
    public TKey Key { get; }
    public TValue Value { get; }
    public long Timestamp { get; }
    public IReadOnlyDictionary<string, byte[]>? Headers { get; }

    public TestRecord(TKey key, TValue value, long timestamp = 0, IReadOnlyDictionary<string, byte[]>? headers = null)
    {
        Key = key;
        Value = value;
        Timestamp = timestamp;
        Headers = headers;
    }

    public override string ToString() => $"TestRecord({Key}, {Value}, ts={Timestamp})";
}
