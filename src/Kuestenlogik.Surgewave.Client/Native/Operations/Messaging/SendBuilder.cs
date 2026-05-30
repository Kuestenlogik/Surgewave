using System.Text;
using Kuestenlogik.Surgewave.Client.Validation;

namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// Message in a send batch.
/// </summary>
public record Message(byte[]? Key, byte[] Value, Dictionary<string, byte[]>? Headers = null);

/// <summary>
/// Fluent builder for send operations.
/// </summary>
public sealed class SendBuilder
{
    private readonly SurgewaveNativeClient _client;
    private readonly string _topic;
    private int _partition;
    private int _partitionCount = 1;
    private IPartitionStrategy? _partitionStrategy;
    private byte[]? _key;
    private byte[]? _value;
    private Dictionary<string, byte[]>? _headers;
    private long? _timestamp;
    private CompressionType _compression = CompressionType.None;
    private int _compressionLevel = -1;
    private SendPreset? _preset;
    private List<Message>? _batchMessages;

    internal SendBuilder(SurgewaveNativeClient client, string topic)
    {
        _client = client;
        Guard.ValidTopicName(topic);
        _topic = topic;
    }

    /// <summary>
    /// Set the target partition directly.
    /// </summary>
    public SendBuilder ToPartition(int partition)
    {
        Guard.ValidPartition(partition);
        _partition = partition;
        _partitionStrategy = null;
        return this;
    }

    /// <summary>
    /// Use a partition selection strategy.
    /// </summary>
    public SendBuilder ToPartition(IPartitionStrategy strategy) { _partitionStrategy = strategy; return this; }

    /// <summary>
    /// Use a custom partition selector function.
    /// </summary>
    public SendBuilder ToPartition(Func<byte[]?, int, int> selector) { _partitionStrategy = Partitioner.Custom(selector); return this; }

    /// <summary>
    /// Set the partition count for strategy-based selection.
    /// </summary>
    public SendBuilder WithPartitionCount(int count) { _partitionCount = count; return this; }

    /// <summary>
    /// Set the message key.
    /// </summary>
    public SendBuilder WithKey(byte[] key) { _key = key; return this; }

    /// <summary>
    /// Set the message key as a string.
    /// </summary>
    public SendBuilder WithKey(string key) { _key = Encoding.UTF8.GetBytes(key); return this; }

    /// <summary>
    /// Set the message value.
    /// </summary>
    public SendBuilder WithValue(byte[] value) { _value = value; return this; }

    /// <summary>
    /// Set the message value as a string.
    /// </summary>
    public SendBuilder WithValue(string value) { _value = Encoding.UTF8.GetBytes(value); return this; }

    /// <summary>
    /// Add current key+value to batch and set the next value (reusing current key).
    /// Enables chaining: .WithKey("k").WithValue("v1").Also("v2").Also("v3").SendAllAsync()
    /// </summary>
    public SendBuilder Also(byte[] value)
    {
        if (_value != null)
        {
            _batchMessages ??= new List<Message>();
            _batchMessages.Add(new Message(_key, _value, _headers));
        }
        _value = value;
        return this;
    }

    /// <summary>
    /// Add current key+value to batch and set the next value as string (reusing current key).
    /// </summary>
    public SendBuilder Also(string value) => Also(Encoding.UTF8.GetBytes(value));

    /// <summary>
    /// Add a header to the message.
    /// </summary>
    public SendBuilder WithHeader(string key, byte[] value)
    {
        _headers ??= new Dictionary<string, byte[]>();
        _headers[key] = value;
        return this;
    }

    /// <summary>
    /// Add a string header to the message.
    /// </summary>
    public SendBuilder WithHeader(string key, string value)
        => WithHeader(key, Encoding.UTF8.GetBytes(value));

    /// <summary>
    /// Add multiple headers to the message.
    /// </summary>
    public SendBuilder WithHeaders(Dictionary<string, byte[]> headers)
    {
        _headers ??= new Dictionary<string, byte[]>();
        foreach (var (key, value) in headers)
            _headers[key] = value;
        return this;
    }

    /// <summary>
    /// Set the message timestamp.
    /// </summary>
    public SendBuilder At(DateTimeOffset timestamp)
    {
        _timestamp = timestamp.ToUnixTimeMilliseconds();
        return this;
    }

    /// <summary>
    /// Set the message timestamp in milliseconds.
    /// </summary>
    public SendBuilder At(long timestampMs)
    {
        _timestamp = timestampMs;
        return this;
    }

    /// <summary>
    /// Set compression type.
    /// </summary>
    public SendBuilder WithCompression(CompressionType compression)
    {
        _compression = compression;
        return this;
    }

    /// <summary>
    /// Set compression level.
    /// </summary>
    public SendBuilder WithCompressionLevel(int level)
    {
        _compressionLevel = level;
        return this;
    }

    /// <summary>
    /// Apply a preset configuration.
    /// </summary>
    public SendBuilder UsePreset(SendPreset preset)
    {
        _preset = preset;
        _compression = preset.Compression;
        _compressionLevel = preset.CompressionLevel;
        _partitionStrategy = preset.PartitionStrategy;
        if (preset.DefaultHeaders != null)
            WithHeaders(preset.DefaultHeaders);
        return this;
    }

    /// <summary>
    /// Add another message to the batch with a different key.
    /// </summary>
    public SendBuilder And(byte[]? key, byte[] value, Dictionary<string, byte[]>? headers = null)
    {
        _batchMessages ??= new List<Message>();
        _batchMessages.Add(new Message(key, value, headers));
        return this;
    }

    /// <summary>
    /// Add another string message to the batch with a different key.
    /// </summary>
    public SendBuilder And(string? key, string value, Dictionary<string, byte[]>? headers = null)
    {
        var keyBytes = key != null ? Encoding.UTF8.GetBytes(key) : null;
        var valueBytes = Encoding.UTF8.GetBytes(value);
        return And(keyBytes, valueBytes, headers);
    }

    /// <summary>
    /// Get the resolved partition (using strategy if set).
    /// </summary>
    internal int ResolvePartition()
    {
        if (_partitionStrategy != null)
            return _partitionStrategy.SelectPartition(_key, _partitionCount);
        return _partition;
    }

    /// <summary>
    /// Execute sending a single message.
    /// </summary>
    public Task<long> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        if (_value == null) throw new InvalidConfigurationException("Value", null, "Use WithValue() to set the message content");
        var partition = ResolvePartition();
        return _client.Messaging.SendAsync(_topic, partition, _key, _value, _headers, cancellationToken);
    }

    /// <summary>
    /// Send all batched messages (includes current value if set).
    /// </summary>
    public Task<long> SendAllAsync(CancellationToken cancellationToken = default)
    {
        // Include current value in batch if set
        if (_value != null)
        {
            _batchMessages ??= new List<Message>();
            _batchMessages.Add(new Message(_key, _value, _headers));
        }

        if (_batchMessages == null || _batchMessages.Count == 0)
            throw new InvalidConfigurationException("Messages", null, "Use WithValue() or And() to add messages");

        var partition = ResolvePartition();
        var messages = _batchMessages
            .Select(m => (m.Key, m.Value, (IReadOnlyDictionary<string, byte[]>?)m.Headers))
            .ToList();
        return _client.Messaging.SendBatchAsync(_topic, partition, messages, cancellationToken);
    }

    /// <summary>
    /// Get the topic name.
    /// </summary>
    internal string Topic => _topic;

    /// <summary>
    /// Get the headers.
    /// </summary>
    internal Dictionary<string, byte[]>? Headers => _headers;
}
