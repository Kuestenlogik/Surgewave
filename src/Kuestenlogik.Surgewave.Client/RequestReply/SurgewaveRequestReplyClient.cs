using System.Collections.Concurrent;
using System.Text;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Serialization;

namespace Kuestenlogik.Surgewave.Client.RequestReply;

/// <summary>
/// Provides synchronous request-reply (RPC) semantics over Surgewave topics.
/// Uses two topics: a shared request topic and a per-client ephemeral reply topic.
///
/// The client side sends a request to the request topic with an embedded correlation ID
/// and reply topic, then waits for a correlated response on its private reply topic.
///
/// The server side consumes from the request topic, processes each request, and
/// produces the response to the reply topic specified in the request envelope.
///
/// Usage (client):
/// <code>
/// await using var rpc = new SurgewaveRequestReplyClient(client, "my-service");
/// await rpc.StartAsync();
/// var reply = await rpc.RequestAsync(key, value);
/// </code>
///
/// Usage (server):
/// <code>
/// await using var rpc = new SurgewaveRequestReplyClient(client, "my-service");
/// await using var responder = await rpc.RespondAsync(async req => ProcessRequest(req));
/// </code>
/// </summary>
public sealed class SurgewaveRequestReplyClient : IAsyncDisposable
{
    private readonly SurgewaveNativeClient _client;
    private readonly string _requestTopic;
    private readonly RequestReplyConfig _config;
    private readonly string _clientId;
    private readonly string _replyTopic;
    private readonly ConcurrentDictionary<string, TaskCompletionSource<ReplyMessage>> _pendingRequests = new();

    private CancellationTokenSource? _replyListenerCts;
    private Task? _replyListenerTask;
    private volatile bool _started;
    private volatile bool _disposed;

    /// <summary>
    /// Creates a request-reply client for the specified request topic.
    /// </summary>
    /// <param name="client">The underlying Surgewave native client (must be connected).</param>
    /// <param name="requestTopic">The topic where requests are sent.</param>
    /// <param name="config">Optional configuration. Uses defaults if null.</param>
    public SurgewaveRequestReplyClient(SurgewaveNativeClient client, string requestTopic, RequestReplyConfig? config = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _requestTopic = requestTopic ?? throw new ArgumentNullException(nameof(requestTopic));
        _config = config ?? new RequestReplyConfig();
        _clientId = Guid.NewGuid().ToString("N");
        _replyTopic = $"{_config.ReplyTopicPrefix}.{_clientId}";
    }

    /// <summary>
    /// The reply topic name for this client instance.
    /// </summary>
    public string ReplyTopic => _replyTopic;

    /// <summary>
    /// The number of currently pending (in-flight) requests.
    /// </summary>
    public int PendingCount => _pendingRequests.Count;

    /// <summary>
    /// Starts the background reply listener. Must be called before sending requests.
    /// Creates the reply topic if <see cref="RequestReplyConfig.AutoCreateTopics"/> is enabled.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_started)
            return;

        if (_config.AutoCreateTopics)
        {
            await EnsureTopicExistsAsync(_replyTopic, cancellationToken).ConfigureAwait(false);
        }

        _replyListenerCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _replyListenerTask = RunReplyListenerAsync(_replyListenerCts.Token);
        _started = true;
    }

    /// <summary>
    /// Sends a request and waits for the correlated reply.
    /// The reply listener must be started via <see cref="StartAsync"/> before calling this method.
    /// </summary>
    /// <param name="key">The message key.</param>
    /// <param name="value">The message value (request payload).</param>
    /// <param name="timeout">Optional timeout override. Uses <see cref="RequestReplyConfig.DefaultTimeout"/> if null.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The correlated reply message.</returns>
    /// <exception cref="TimeoutException">Thrown when no reply is received within the timeout.</exception>
    /// <exception cref="RequestReplyException">Thrown when the responder returns an error reply.</exception>
    public async Task<ReplyMessage> RequestAsync(
        byte[] key,
        byte[] value,
        TimeSpan? timeout = null,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_started)
            throw new InvalidOperationException("Call StartAsync() before sending requests.");

        var correlationId = Guid.NewGuid().ToString("N");
        var effectiveTimeout = timeout ?? _config.DefaultTimeout;

        var tcs = new TaskCompletionSource<ReplyMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        _pendingRequests[correlationId] = tcs;

        try
        {
            // Wrap the payload with correlation envelope
            var envelope = RequestReplyEnvelope.WrapRequest(correlationId, _replyTopic, value);

            // Send to the request topic
            await _client.Messaging.SendAsync(_requestTopic, 0, key, envelope, ct).ConfigureAwait(false);

            // Wait for the correlated reply with timeout
            using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeoutCts.CancelAfter(effectiveTimeout);

            try
            {
                var registration = timeoutCts.Token.Register(() =>
                    tcs.TrySetException(new TimeoutException(
                        $"Request-reply timed out after {effectiveTimeout.TotalSeconds:F1}s (correlationId: {correlationId})")));

                await using (registration.ConfigureAwait(false))
                {
                    return await tcs.Task.ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
            {
                throw new TimeoutException(
                    $"Request-reply timed out after {effectiveTimeout.TotalSeconds:F1}s (correlationId: {correlationId})");
            }
        }
        finally
        {
            _pendingRequests.TryRemove(correlationId, out _);
        }
    }

    /// <summary>
    /// Sends a typed request and waits for a typed reply using JSON serialization.
    /// </summary>
    /// <typeparam name="TRequest">The request type.</typeparam>
    /// <typeparam name="TReply">The reply type.</typeparam>
    /// <param name="key">The message key.</param>
    /// <param name="request">The request object.</param>
    /// <param name="timeout">Optional timeout override.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The deserialized reply.</returns>
    /// <exception cref="TimeoutException">Thrown when no reply is received within the timeout.</exception>
    /// <exception cref="RequestReplyException">Thrown when the responder returns an error reply.</exception>
    public async Task<TReply> RequestAsync<TRequest, TReply>(
        string key,
        TRequest request,
        TimeSpan? timeout = null,
        CancellationToken ct = default)
    {
        var keySerializer = ResolveSerializer<string>();
        var requestSerializer = ResolveSerializer<TRequest>();
        var replyDeserializer = ResolveDeserializer<TReply>();

        var keyBytes = keySerializer.Serialize(key, _requestTopic) ?? [];
        var requestBytes = requestSerializer.Serialize(request, _requestTopic)
            ?? throw new SerializationException(SerializationDirection.Serialize, typeof(TRequest), _requestTopic);

        var reply = await RequestAsync(keyBytes, requestBytes, timeout, ct).ConfigureAwait(false);

        if (reply.IsError)
            throw new RequestReplyException(reply.ErrorMessage ?? "Server returned an error", reply.CorrelationId);

        return replyDeserializer.Deserialize(reply.Value, _replyTopic);
    }

    /// <summary>
    /// Starts consuming from the request topic and dispatching to the handler.
    /// The handler receives each request and returns the raw response bytes.
    /// Returns a disposable that stops the responder when disposed.
    /// </summary>
    /// <param name="handler">Function that processes a request and returns the response bytes.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A disposable that stops the responder.</returns>
    public async Task<IAsyncDisposable> RespondAsync(
        Func<RequestMessage, Task<byte[]>> handler,
        CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(handler);

        if (_config.AutoCreateTopics)
        {
            await EnsureTopicExistsAsync(_requestTopic, ct).ConfigureAwait(false);
        }

        var responderCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var responderTask = RunResponderAsync(handler, responderCts.Token);

        return new ResponderHandle(responderCts, responderTask);
    }

    /// <summary>
    /// Starts consuming from the request topic with typed serialization.
    /// </summary>
    /// <typeparam name="TRequest">The request type (deserialized from the request payload).</typeparam>
    /// <typeparam name="TReply">The reply type (serialized into the response payload).</typeparam>
    /// <param name="handler">Function that processes a typed request and returns a typed reply.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>A disposable that stops the responder.</returns>
    public Task<IAsyncDisposable> RespondAsync<TRequest, TReply>(
        Func<TRequest, Task<TReply>> handler,
        CancellationToken ct = default)
    {
        var requestDeserializer = ResolveDeserializer<TRequest>();
        var replySerializer = ResolveSerializer<TReply>();

        return RespondAsync(async requestMsg =>
        {
            var typedRequest = requestDeserializer.Deserialize(requestMsg.Value, _requestTopic);
            var typedReply = await handler(typedRequest).ConfigureAwait(false);
            return replySerializer.Serialize(typedReply, _replyTopic) ?? [];
        }, ct);
    }

    private async Task RunReplyListenerAsync(CancellationToken ct)
    {
        // Start consuming from the beginning of the reply topic
        var offset = 0L;
        try
        {
            offset = await _client.Messaging.GetLatestOffsetAsync(_replyTopic, 0, ct).ConfigureAwait(false);
        }
        catch
        {
            // Topic may not have messages yet, start from 0
        }

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await _client.Messaging.ReceiveAsync(
                    _replyTopic, 0, offset, maxBytes: 1024 * 1024, maxWaitMs: 1000, ct).ConfigureAwait(false);

                foreach (var msg in result.Messages)
                {
                    offset = msg.Offset + 1;

                    try
                    {
                        var (correlationId, payload, isError, errorMessage) =
                            RequestReplyEnvelope.UnwrapReply(msg.Value);

                        var reply = new ReplyMessage(
                            correlationId,
                            payload,
                            DateTimeOffset.FromUnixTimeMilliseconds(msg.Timestamp),
                            isError,
                            errorMessage);

                        if (_pendingRequests.TryRemove(correlationId, out var tcs))
                        {
                            tcs.TrySetResult(reply);
                        }
                    }
                    catch (Exception ex)
                    {
                        // Malformed reply envelope - skip
                        System.Diagnostics.Debug.WriteLine($"Failed to unwrap reply: {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch
            {
                // Transient error - retry after brief pause
                try { await Task.Delay(100, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    private async Task RunResponderAsync(Func<RequestMessage, Task<byte[]>> handler, CancellationToken ct)
    {
        var offset = 0L;
        try
        {
            offset = await _client.Messaging.GetLatestOffsetAsync(_requestTopic, 0, ct).ConfigureAwait(false);
        }
        catch
        {
            // Topic may not have messages yet, start from 0
        }

        while (!ct.IsCancellationRequested)
        {
            try
            {
                var result = await _client.Messaging.ReceiveAsync(
                    _requestTopic, 0, offset, maxBytes: 1024 * 1024, maxWaitMs: 1000, ct).ConfigureAwait(false);

                foreach (var msg in result.Messages)
                {
                    offset = msg.Offset + 1;

                    try
                    {
                        var (correlationId, replyTopic, payload) =
                            RequestReplyEnvelope.UnwrapRequest(msg.Value);

                        var requestMessage = new RequestMessage(
                            correlationId,
                            replyTopic,
                            msg.Key ?? [],
                            payload,
                            DateTimeOffset.FromUnixTimeMilliseconds(msg.Timestamp),
                            null);

                        byte[] responsePayload;
                        bool isError = false;
                        string? errorMessage = null;

                        try
                        {
                            responsePayload = await handler(requestMessage).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            responsePayload = [];
                            isError = true;
                            errorMessage = ex.Message;
                        }

                        var replyEnvelope = RequestReplyEnvelope.WrapReply(
                            correlationId, responsePayload, isError, errorMessage);

                        await _client.Messaging.SendAsync(
                            replyTopic, 0, null, replyEnvelope, ct).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        // Malformed request or send failure - skip
                        System.Diagnostics.Debug.WriteLine($"Failed to process request: {ex.Message}");
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch
            {
                // Transient error - retry after brief pause
                try { await Task.Delay(100, ct).ConfigureAwait(false); }
                catch (OperationCanceledException) { break; }
            }
        }
    }

    private async Task EnsureTopicExistsAsync(string topic, CancellationToken ct)
    {
        try
        {
            var topics = await _client.Topics.ListAsync(ct).ConfigureAwait(false);
            if (topics.Any(t => t.Name == topic))
                return;

            await _client.Topics.CreateAsync(topic, partitions: 1, cancellationToken: ct).ConfigureAwait(false);
        }
        catch
        {
            // Topic creation may fail if it already exists (race condition) - that's fine
        }
    }

    private static ISerializer<T> ResolveSerializer<T>()
    {
        return TypedSendBuilder<T, T>.GetDefaultSerializer<T>();
    }

    private static IDeserializer<T> ResolveDeserializer<T>()
    {
        var type = typeof(T);

        if (type == typeof(string))
            return (IDeserializer<T>)(object)Serializers.StringDeserializer;

        if (type == typeof(byte[]))
            return (IDeserializer<T>)(object)Serializers.ByteArrayDeserializer;

        if (type == typeof(int))
            return (IDeserializer<T>)(object)Serializers.Int32Deserializer;

        if (type == typeof(long))
            return (IDeserializer<T>)(object)Serializers.Int64Deserializer;

        if (type == typeof(Guid))
            return (IDeserializer<T>)(object)Serializers.GuidDeserializer;

        return Serializers.JsonDeserializer<T>();
    }

    /// <inheritdoc/>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        // Cancel all pending requests
        foreach (var kvp in _pendingRequests)
        {
            kvp.Value.TrySetCanceled();
        }
        _pendingRequests.Clear();

        // Stop the reply listener
        if (_replyListenerCts != null)
        {
            await _replyListenerCts.CancelAsync().ConfigureAwait(false);
            _replyListenerCts.Dispose();
        }

        if (_replyListenerTask != null)
        {
            try { await _replyListenerTask.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
            catch { }
        }
    }

    /// <summary>
    /// Handle returned by <see cref="RespondAsync"/> that stops the responder on disposal.
    /// </summary>
    private sealed class ResponderHandle(CancellationTokenSource cts, Task responderTask) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            await cts.CancelAsync().ConfigureAwait(false);
            cts.Dispose();

            try { await responderTask.ConfigureAwait(false); }
            catch (OperationCanceledException) { }
            catch { }
        }
    }
}
