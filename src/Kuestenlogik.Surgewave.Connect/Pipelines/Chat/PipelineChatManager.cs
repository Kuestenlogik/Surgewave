using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Native;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Connect.Pipelines.Chat;

/// <summary>
/// Manages chat sessions for pipelines. Produces user messages to the pipeline's
/// signal topic and consumes responses from the pipeline's chat output topic.
/// </summary>
public sealed class PipelineChatManager : IAsyncDisposable
{
    private const string ChatTopicPrefix = "_pipeline-chat-";
    private const string ChatSignalSuffix = "-signal";
    private const string ChatResponseSuffix = "-response";

    private readonly ConcurrentDictionary<string, ChatSession> _sessions = new();
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _consumers = new();
    private readonly ConcurrentDictionary<string, long> _consumerOffsets = new();
    private readonly ConnectWorkerConfig _config;
    private readonly ILogger<PipelineChatManager> _logger;
    private SurgewaveNativeClient? _client;

    public TimeSpan ResponseTimeout { get; set; } = TimeSpan.FromSeconds(60);
    public TimeSpan SessionIdleTimeout { get; set; } = TimeSpan.FromMinutes(30);

    public PipelineChatManager(ConnectWorkerConfig config, ILogger<PipelineChatManager> logger)
    {
        _config = config;
        _logger = logger;
    }

    /// <summary>
    /// Get the signal topic name for a pipeline (where user messages are produced).
    /// </summary>
    public static string GetSignalTopicName(string pipelineId)
        => $"{ChatTopicPrefix}{pipelineId}{ChatSignalSuffix}";

    /// <summary>
    /// Get the response topic name for a pipeline (where pipeline responses are produced).
    /// </summary>
    public static string GetResponseTopicName(string pipelineId)
        => $"{ChatTopicPrefix}{pipelineId}{ChatResponseSuffix}";

    /// <summary>
    /// Create or retrieve a chat session for a pipeline.
    /// </summary>
    public ChatSession GetOrCreateSession(string pipelineId, string? sessionId = null)
    {
        sessionId ??= Guid.NewGuid().ToString("N");
        var key = $"{pipelineId}:{sessionId}";

        return _sessions.GetOrAdd(key, _ => new ChatSession(sessionId, pipelineId));
    }

    /// <summary>
    /// Get an existing session.
    /// </summary>
    public ChatSession? GetSession(string pipelineId, string sessionId)
    {
        var key = $"{pipelineId}:{sessionId}";
        return _sessions.TryGetValue(key, out var session) ? session : null;
    }

    /// <summary>
    /// List all active sessions for a pipeline.
    /// </summary>
    public IReadOnlyList<ChatSession> ListSessions(string pipelineId)
    {
        return _sessions.Values
            .Where(s => s.PipelineId == pipelineId)
            .OrderByDescending(s => s.LastActivityAt)
            .ToList();
    }

    /// <summary>
    /// Send a chat message to a pipeline and wait for the response.
    /// </summary>
    public async Task<ChatMessage> SendMessageAsync(
        string pipelineId,
        string sessionId,
        string message,
        CancellationToken cancellationToken = default)
    {
        var session = GetOrCreateSession(pipelineId, sessionId);
        var messageId = session.AddUserMessage(message);

        // Ensure we're consuming responses for this pipeline
        await EnsureResponseConsumerAsync(pipelineId, cancellationToken);

        // Produce the message to the signal topic
        await ProduceSignalAsync(pipelineId, sessionId, messageId, message, cancellationToken);

        _logger.LogDebug("Sent chat message {MessageId} to pipeline {PipelineId}", messageId, pipelineId);

        // Wait for the pipeline to produce a response
        return await session.WaitForResponseAsync(messageId, ResponseTimeout, cancellationToken);
    }

    /// <summary>
    /// Send a chat message without waiting for a response (fire-and-forget).
    /// Returns the message ID for later correlation.
    /// </summary>
    public async Task<string> SendMessageFireAndForgetAsync(
        string pipelineId,
        string sessionId,
        string message,
        CancellationToken cancellationToken = default)
    {
        var session = GetOrCreateSession(pipelineId, sessionId);
        var messageId = session.AddUserMessage(message);

        await EnsureResponseConsumerAsync(pipelineId, cancellationToken);
        await ProduceSignalAsync(pipelineId, sessionId, messageId, message, cancellationToken);

        return messageId;
    }

    /// <summary>
    /// Send a chat message and stream the response back as a series of token events.
    /// Currently simulates token-by-token streaming by splitting the full response into
    /// word-sized chunks, providing a streaming UX while the infrastructure catches up.
    /// </summary>
    public async IAsyncEnumerable<ChatStreamEvent> StreamChatAsync(
        string pipelineId,
        string sessionId,
        string message,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ChatMessage? response = null;
        string? errorMessage = null;

        try
        {
            response = await SendMessageAsync(pipelineId, sessionId, message, ct);
        }
        catch (Exception ex)
        {
            errorMessage = ex.Message;
        }

        if (errorMessage is not null)
        {
            yield return new ChatStreamEvent { Type = "error", Error = errorMessage };
            yield break;
        }

        if (response!.Role == "system" && response.Metadata?.ContainsKey("error") == true)
        {
            yield return new ChatStreamEvent { Type = "error", Error = response.Content };
            yield break;
        }

        if (response.Role == "system" && response.Metadata?.ContainsKey("timeout") == true)
        {
            yield return new ChatStreamEvent { Type = "error", Error = response.Content };
            yield break;
        }

        // Simulate token-by-token streaming by splitting into word-sized chunks
        var words = response.Content.Split(' ');
        for (var i = 0; i < words.Length; i++)
        {
            ct.ThrowIfCancellationRequested();

            var token = i == 0 ? words[i] : " " + words[i];
            yield return new ChatStreamEvent { Type = "token", Token = token };

            if (i < words.Length - 1)
            {
                await Task.Delay(25, ct);
            }
        }

        yield return new ChatStreamEvent
        {
            Type = "done",
            SessionId = sessionId,
            MessageId = response.Id,
            FullContent = response.Content
        };
    }

    /// <summary>
    /// Remove a session and clean up.
    /// </summary>
    public bool RemoveSession(string pipelineId, string sessionId)
    {
        var key = $"{pipelineId}:{sessionId}";
        return _sessions.TryRemove(key, out _);
    }

    /// <summary>
    /// Clean up idle sessions.
    /// </summary>
    public int CleanupIdleSessions()
    {
        var now = DateTimeOffset.UtcNow;
        var expired = _sessions
            .Where(kv => (now - kv.Value.LastActivityAt) > SessionIdleTimeout)
            .Select(kv => kv.Key)
            .ToList();

        foreach (var key in expired)
        {
            _sessions.TryRemove(key, out _);
        }

        return expired.Count;
    }

    private async Task EnsureConnectedAsync(CancellationToken cancellationToken)
    {
        if (_client is { IsConnected: true })
            return;

        var parts = _config.BootstrapServers.Split(':');
        var host = parts[0];
        var port = parts.Length > 1 ? int.Parse(parts[1]) : 9092;

        _client = new SurgewaveNativeClient(host, port);
        await _client.ConnectAsync(cancellationToken);
    }

    private async Task ProduceSignalAsync(
        string pipelineId,
        string sessionId,
        string messageId,
        string content,
        CancellationToken cancellationToken)
    {
        await EnsureConnectedAsync(cancellationToken);

        var signalTopic = GetSignalTopicName(pipelineId);
        var payload = JsonSerializer.Serialize(new
        {
            session_id = sessionId,
            message_id = messageId,
            content,
            timestamp = DateTimeOffset.UtcNow
        });

        await _client!.Messaging.SendAsync(signalTopic, 0, sessionId, payload, cancellationToken);
    }

    private async Task EnsureResponseConsumerAsync(string pipelineId, CancellationToken cancellationToken)
    {
        if (_consumers.ContainsKey(pipelineId))
            return;

        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        if (!_consumers.TryAdd(pipelineId, cts))
            return;

        await EnsureConnectedAsync(cancellationToken);

        var responseTopic = GetResponseTopicName(pipelineId);

        // Ensure topic exists
        try
        {
            await _client!.Topics.Create(responseTopic)
                .WithPartitions(1)
                .WithReplicationFactor(1)
                .WithConfig("cleanup.policy", "delete")
                .WithConfig("retention.ms", "3600000") // 1 hour
                .ExecuteAsync(cancellationToken);
        }
        catch (Exception ex) when (ex.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase))
        {
            // Topic already exists, that's fine
        }

        // Start a background consumer polling loop
        _ = Task.Run(() => ConsumeResponsesAsync(pipelineId, responseTopic, cts.Token), cts.Token);
    }

    private async Task ConsumeResponsesAsync(string pipelineId, string responseTopic, CancellationToken cancellationToken)
    {
        var offsetKey = $"{pipelineId}:0";
        _consumerOffsets.TryAdd(offsetKey, 0);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var offset = _consumerOffsets.GetOrAdd(offsetKey, 0);

                try
                {
                    var result = await _client!.Messaging.ReceiveAsync(
                        responseTopic, 0, offset,
                        maxBytes: 256 * 1024,
                        maxWaitMs: 1000,
                        cancellationToken: cancellationToken);

                    foreach (var msg in result.Messages)
                    {
                        try
                        {
                            var value = Encoding.UTF8.GetString(msg.Value);
                            using var doc = JsonDocument.Parse(value);

                            var sessionId = doc.RootElement.TryGetProperty("session_id", out var sid) ? sid.GetString() : null;
                            var messageId = doc.RootElement.TryGetProperty("message_id", out var mid) ? mid.GetString() : null;
                            var content = doc.RootElement.TryGetProperty("content", out var c) ? c.GetString() : value;

                            if (sessionId is not null && messageId is not null && content is not null)
                            {
                                var session = GetSession(pipelineId, sessionId);
                                session?.CompleteResponse(messageId, content);

                                _logger.LogDebug("Received chat response for session {SessionId}, message {MessageId}",
                                    sessionId, messageId);
                            }
                        }
                        catch (JsonException ex)
                        {
                            _logger.LogWarning(ex, "Failed to parse chat response from topic {Topic}", responseTopic);
                        }

                        _consumerOffsets[offsetKey] = msg.Offset + 1;
                    }

                    if (result.Messages.Count == 0)
                    {
                        // No new messages, wait a bit before polling again
                        await Task.Delay(100, cancellationToken);
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogWarning(ex, "Error polling responses for pipeline {PipelineId}, retrying...", pipelineId);
                    await Task.Delay(500, cancellationToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Chat response consumer for pipeline {PipelineId} failed", pipelineId);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var (_, cts) in _consumers)
        {
            await cts.CancelAsync();
            cts.Dispose();
        }

        _consumers.Clear();
        _sessions.Clear();
        _consumerOffsets.Clear();

        if (_client != null)
        {
            await _client.DisposeAsync();
            _client = null;
        }
    }
}
