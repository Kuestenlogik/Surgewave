using System.Text;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// Typed fluent builder for send operations with serialization support.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public sealed class TypedSendBuilder<TKey, TValue>
{
    private readonly SurgewaveNativeClient _client;
    private readonly string _topic;
    private int _partition;
    private int _partitionCount = 1;
    private IPartitionStrategy? _partitionStrategy;
    private TKey? _key;
    private TValue? _value;
    private bool _keySet;
    private bool _valueSet;
    private ISerializer<TKey>? _keySerializer;
    private ISerializer<TValue>? _valueSerializer;
    private Dictionary<string, byte[]>? _headers;
    private long? _timestamp;
    private CompressionType _compression = CompressionType.None;
    private List<TypedMessage<TKey, TValue>>? _batchMessages;

    internal TypedSendBuilder(SurgewaveNativeClient client, string topic)
    {
        _client = client;
        _topic = topic;
    }

    /// <summary>
    /// Set the target partition directly.
    /// </summary>
    public TypedSendBuilder<TKey, TValue> ToPartition(int partition)
    {
        _partition = partition;
        _partitionStrategy = null;
        return this;
    }

    /// <summary>
    /// Use a partition selection strategy.
    /// </summary>
    public TypedSendBuilder<TKey, TValue> ToPartition(IPartitionStrategy strategy)
    {
        _partitionStrategy = strategy;
        return this;
    }

    /// <summary>
    /// Set partition count for strategy-based selection.
    /// </summary>
    public TypedSendBuilder<TKey, TValue> WithPartitionCount(int count)
    {
        _partitionCount = count;
        return this;
    }

    /// <summary>
    /// Set the message key.
    /// </summary>
    public TypedSendBuilder<TKey, TValue> WithKey(TKey key)
    {
        _key = key;
        _keySet = true;
        return this;
    }

    /// <summary>
    /// Set the message value.
    /// </summary>
    public TypedSendBuilder<TKey, TValue> WithValue(TValue value)
    {
        _value = value;
        _valueSet = true;
        return this;
    }

    /// <summary>
    /// Add current key+value to batch and set the next value (reusing current key).
    /// Enables chaining: .WithKey("k").WithValue(v1).Also(v2).Also(v3).SendAllAsync()
    /// </summary>
    public TypedSendBuilder<TKey, TValue> Also(TValue value)
    {
        if (_valueSet)
        {
            _batchMessages ??= new List<TypedMessage<TKey, TValue>>();
            _batchMessages.Add(new TypedMessage<TKey, TValue>(_key, _value!, _headers));
        }
        _value = value;
        _valueSet = true;
        return this;
    }

    /// <summary>
    /// Set a custom key serializer.
    /// </summary>
    public TypedSendBuilder<TKey, TValue> WithKeySerializer(ISerializer<TKey> serializer)
    {
        _keySerializer = serializer;
        return this;
    }

    /// <summary>
    /// Set a custom value serializer.
    /// </summary>
    public TypedSendBuilder<TKey, TValue> WithValueSerializer(ISerializer<TValue> serializer)
    {
        _valueSerializer = serializer;
        return this;
    }

    /// <summary>
    /// Add a header to the message.
    /// </summary>
    public TypedSendBuilder<TKey, TValue> WithHeader(string key, byte[] value)
    {
        _headers ??= new Dictionary<string, byte[]>();
        _headers[key] = value;
        return this;
    }

    /// <summary>
    /// Add a string header to the message.
    /// </summary>
    public TypedSendBuilder<TKey, TValue> WithHeader(string key, string value)
        => WithHeader(key, Encoding.UTF8.GetBytes(value));

    /// <summary>
    /// Add multiple headers to the message.
    /// </summary>
    public TypedSendBuilder<TKey, TValue> WithHeaders(Dictionary<string, byte[]> headers)
    {
        _headers ??= new Dictionary<string, byte[]>();
        foreach (var (key, value) in headers)
            _headers[key] = value;
        return this;
    }

    /// <summary>
    /// Set the message timestamp.
    /// </summary>
    public TypedSendBuilder<TKey, TValue> At(DateTimeOffset timestamp)
    {
        _timestamp = timestamp.ToUnixTimeMilliseconds();
        return this;
    }

    /// <summary>
    /// Set compression type.
    /// </summary>
    public TypedSendBuilder<TKey, TValue> WithCompression(CompressionType compression)
    {
        _compression = compression;
        return this;
    }

    /// <summary>
    /// Add a message to the batch with a different key.
    /// </summary>
    public TypedSendBuilder<TKey, TValue> And(TKey? key, TValue value, Dictionary<string, byte[]>? headers = null)
    {
        _batchMessages ??= new List<TypedMessage<TKey, TValue>>();
        _batchMessages.Add(new TypedMessage<TKey, TValue>(key, value, headers));
        return this;
    }

    private int ResolvePartition(byte[]? keyBytes)
    {
        if (_partitionStrategy != null)
            return _partitionStrategy.SelectPartition(keyBytes, _partitionCount);
        return _partition;
    }

    /// <summary>
    /// Execute sending a single message.
    /// </summary>
    public Task<long> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        if (!_valueSet) throw new InvalidConfigurationException("Value", null, "Use WithValue() to set the message content");

        var keySerializer = GetKeySerializer();
        var valueSerializer = GetValueSerializer();

        var keyBytes = _keySet ? keySerializer.Serialize(_key, _topic) : null;
        var valueBytes = valueSerializer.Serialize(_value, _topic)
            ?? throw new SerializationException(SerializationDirection.Serialize, typeof(TValue), _topic);

        var partition = ResolvePartition(keyBytes);
        return _client.Messaging.SendAsync(_topic, partition, keyBytes, valueBytes, cancellationToken);
    }

    /// <summary>
    /// Send all batched messages (includes current value if set).
    /// </summary>
    public Task<long> SendAllAsync(CancellationToken cancellationToken = default)
    {
        // Include current value in batch if set
        if (_valueSet)
        {
            _batchMessages ??= new List<TypedMessage<TKey, TValue>>();
            _batchMessages.Add(new TypedMessage<TKey, TValue>(_key, _value!, _headers));
        }

        if (_batchMessages == null || _batchMessages.Count == 0)
            throw new InvalidConfigurationException("Messages", null, "Use WithValue() or And() to add messages");

        var keySerializer = GetKeySerializer();
        var valueSerializer = GetValueSerializer();

        var byteMessages = new List<(byte[]? Key, byte[] Value)>(_batchMessages.Count);
        byte[]? firstKeyBytes = null;
        foreach (var msg in _batchMessages)
        {
            var keyBytes = msg.Key != null ? keySerializer.Serialize(msg.Key, _topic) : null;
            firstKeyBytes ??= keyBytes;
            var valueBytes = valueSerializer.Serialize(msg.Value, _topic)
                ?? throw new SerializationException(SerializationDirection.Serialize, typeof(TValue), _topic);
            byteMessages.Add((keyBytes, valueBytes));
        }

        var partition = ResolvePartition(firstKeyBytes);
        return _client.Messaging.SendBatchAsync(_topic, partition, byteMessages, cancellationToken);
    }

    private ISerializer<TKey> GetKeySerializer()
    {
        if (_keySerializer != null) return _keySerializer;
        return GetDefaultSerializer<TKey>();
    }

    private ISerializer<TValue> GetValueSerializer()
    {
        if (_valueSerializer != null) return _valueSerializer;
        return GetDefaultSerializer<TValue>();
    }

    internal static ISerializer<T> GetDefaultSerializer<T>()
    {
        var type = typeof(T);

        if (type == typeof(string))
            return (ISerializer<T>)(object)Serializers.String;

        if (type == typeof(byte[]))
            return (ISerializer<T>)(object)Serializers.ByteArray;

        if (type == typeof(int))
            return (ISerializer<T>)(object)Serializers.Int32;

        if (type == typeof(long))
            return (ISerializer<T>)(object)Serializers.Int64;

        if (type == typeof(Guid))
            return (ISerializer<T>)(object)Serializers.Guid;

        // Fall back to JSON for complex types
        return Serializers.Json<T>();
    }
}
