namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// Callbacks for send operations.
/// </summary>
public sealed class SendCallbacks
{
    /// <summary>
    /// Called when send succeeds with the resulting offset.
    /// </summary>
    public Action<long>? OnSuccess { get; set; }

    /// <summary>
    /// Called when send fails with the exception.
    /// </summary>
    public Action<Exception>? OnError { get; set; }

    /// <summary>
    /// Called before sending the request.
    /// </summary>
    public Action<string, int, byte[]?, byte[]>? OnBeforeSend { get; set; }

    /// <summary>
    /// Called after receiving the response (success or failure).
    /// </summary>
    public Action<string, int, TimeSpan>? OnAfterSend { get; set; }
}

/// <summary>
/// Builder that wraps send operations with callbacks.
/// </summary>
public sealed class CallbackSendBuilder
{
    private readonly SendBuilder _inner;
    private readonly string _topic;
    private int _partition;
    private Action<long>? _onSuccess;
    private Action<Exception>? _onError;
    private Action<string, int, byte[]?, byte[]>? _onBeforeSend;
    private Action<string, int, TimeSpan>? _onAfterSend;

    internal CallbackSendBuilder(SendBuilder inner, string topic)
    {
        _inner = inner;
        _topic = topic;
    }

    /// <summary>
    /// Set the target partition.
    /// </summary>
    public CallbackSendBuilder ToPartition(int partition)
    {
        _partition = partition;
        _inner.ToPartition(partition);
        return this;
    }

    /// <summary>
    /// Set the message key.
    /// </summary>
    public CallbackSendBuilder WithKey(byte[] key)
    {
        _inner.WithKey(key);
        return this;
    }

    /// <summary>
    /// Set the message key.
    /// </summary>
    public CallbackSendBuilder WithKey(string key)
    {
        _inner.WithKey(key);
        return this;
    }

    /// <summary>
    /// Set the message value.
    /// </summary>
    public CallbackSendBuilder WithValue(byte[] value)
    {
        _inner.WithValue(value);
        return this;
    }

    /// <summary>
    /// Set the message value.
    /// </summary>
    public CallbackSendBuilder WithValue(string value)
    {
        _inner.WithValue(value);
        return this;
    }

    /// <summary>
    /// Register success callback.
    /// </summary>
    public CallbackSendBuilder OnSuccess(Action<long> callback)
    {
        _onSuccess = callback;
        return this;
    }

    /// <summary>
    /// Register error callback.
    /// </summary>
    public CallbackSendBuilder OnError(Action<Exception> callback)
    {
        _onError = callback;
        return this;
    }

    /// <summary>
    /// Register before-send callback.
    /// </summary>
    public CallbackSendBuilder OnBeforeSend(Action<string, int, byte[]?, byte[]> callback)
    {
        _onBeforeSend = callback;
        return this;
    }

    /// <summary>
    /// Register after-send callback.
    /// </summary>
    public CallbackSendBuilder OnAfterSend(Action<string, int, TimeSpan> callback)
    {
        _onAfterSend = callback;
        return this;
    }

    /// <summary>
    /// Execute the send with callbacks.
    /// </summary>
    public async Task<long> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            var offset = await _inner.ExecuteAsync(cancellationToken);
            sw.Stop();
            _onSuccess?.Invoke(offset);
            _onAfterSend?.Invoke(_topic, _partition, sw.Elapsed);
            return offset;
        }
        catch (Exception ex)
        {
            sw.Stop();
            _onError?.Invoke(ex);
            _onAfterSend?.Invoke(_topic, _partition, sw.Elapsed);
            throw;
        }
    }
}

/// <summary>
/// Extension methods for adding callbacks to builders.
/// </summary>
public static class CallbackExtensions
{
    /// <summary>
    /// Add callbacks to a send builder.
    /// </summary>
    public static CallbackSendBuilder WithCallbacks(this SendBuilder builder, string topic)
        => new(builder, topic);

    /// <summary>
    /// Add success callback directly to send builder.
    /// </summary>
    public static SendBuilderWithCallback OnSuccess(this SendBuilder builder, Action<long> callback)
        => new(builder, callback, null);

    /// <summary>
    /// Add error callback directly to send builder.
    /// </summary>
    public static SendBuilderWithCallback OnError(this SendBuilder builder, Action<Exception> callback)
        => new(builder, null, callback);
}

/// <summary>
/// Send builder with inline callbacks.
/// </summary>
public sealed class SendBuilderWithCallback
{
    private readonly SendBuilder _inner;
    private Action<long>? _onSuccess;
    private Action<Exception>? _onError;

    internal SendBuilderWithCallback(SendBuilder inner, Action<long>? onSuccess, Action<Exception>? onError)
    {
        _inner = inner;
        _onSuccess = onSuccess;
        _onError = onError;
    }

    /// <summary>
    /// Add success callback.
    /// </summary>
    public SendBuilderWithCallback OnSuccess(Action<long> callback)
    {
        _onSuccess = callback;
        return this;
    }

    /// <summary>
    /// Add error callback.
    /// </summary>
    public SendBuilderWithCallback OnError(Action<Exception> callback)
    {
        _onError = callback;
        return this;
    }

    /// <summary>
    /// Execute with callbacks.
    /// </summary>
    public async Task<long> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            var offset = await _inner.ExecuteAsync(cancellationToken);
            _onSuccess?.Invoke(offset);
            return offset;
        }
        catch (Exception ex)
        {
            _onError?.Invoke(ex);
            throw;
        }
    }
}
