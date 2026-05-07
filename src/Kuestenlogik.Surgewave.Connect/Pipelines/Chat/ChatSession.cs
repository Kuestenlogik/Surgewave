using System.Collections.Concurrent;

namespace Kuestenlogik.Surgewave.Connect.Pipelines.Chat;

/// <summary>
/// A chat session tied to a pipeline. Tracks conversation history and
/// correlates user messages with pipeline responses.
/// </summary>
public sealed class ChatSession
{
    private readonly ConcurrentQueue<ChatMessage> _history = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource<ChatMessage>> _pending = new();

    public string SessionId { get; }
    public string PipelineId { get; }
    public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
    public DateTimeOffset LastActivityAt { get; private set; } = DateTimeOffset.UtcNow;

    public ChatSession(string sessionId, string pipelineId)
    {
        SessionId = sessionId;
        PipelineId = pipelineId;
    }

    /// <summary>
    /// Add a user message to the session and create a pending response slot.
    /// Returns the message ID used for correlation.
    /// </summary>
    public string AddUserMessage(string content, Dictionary<string, string>? metadata = null)
    {
        var msg = new ChatMessage
        {
            Id = Guid.NewGuid().ToString("N"),
            Role = "user",
            Content = content,
            Metadata = metadata
        };

        _history.Enqueue(msg);
        _pending[msg.Id] = new TaskCompletionSource<ChatMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        LastActivityAt = DateTimeOffset.UtcNow;
        return msg.Id;
    }

    /// <summary>
    /// Complete a pending request with an assistant response.
    /// Called when the pipeline produces output correlated with the message ID.
    /// </summary>
    public void CompleteResponse(string messageId, string content, Dictionary<string, string>? metadata = null)
    {
        var response = new ChatMessage
        {
            Id = Guid.NewGuid().ToString("N"),
            Role = "assistant",
            Content = content,
            Metadata = metadata
        };

        _history.Enqueue(response);
        LastActivityAt = DateTimeOffset.UtcNow;

        if (_pending.TryRemove(messageId, out var tcs))
        {
            tcs.TrySetResult(response);
        }
    }

    /// <summary>
    /// Fail a pending request.
    /// </summary>
    public void FailResponse(string messageId, string error)
    {
        if (_pending.TryRemove(messageId, out var tcs))
        {
            var errorMsg = new ChatMessage
            {
                Id = Guid.NewGuid().ToString("N"),
                Role = "system",
                Content = error,
                Metadata = new Dictionary<string, string> { ["error"] = "true" }
            };
            _history.Enqueue(errorMsg);
            tcs.TrySetResult(errorMsg);
        }
    }

    /// <summary>
    /// Wait for the response to a specific message.
    /// </summary>
    public async Task<ChatMessage> WaitForResponseAsync(string messageId, TimeSpan timeout, CancellationToken cancellationToken)
    {
        if (!_pending.TryGetValue(messageId, out var tcs))
        {
            throw new InvalidOperationException($"No pending request with ID '{messageId}'");
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);

        try
        {
            return await tcs.Task.WaitAsync(cts.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            _pending.TryRemove(messageId, out _);
            var timeoutMsg = new ChatMessage
            {
                Id = Guid.NewGuid().ToString("N"),
                Role = "system",
                Content = "Response timed out",
                Metadata = new Dictionary<string, string> { ["timeout"] = "true" }
            };
            _history.Enqueue(timeoutMsg);
            return timeoutMsg;
        }
    }

    /// <summary>
    /// Get conversation history.
    /// </summary>
    public IReadOnlyList<ChatMessage> History => [.. _history];

    /// <summary>
    /// Number of pending (unanswered) requests.
    /// </summary>
    public int PendingCount => _pending.Count;

    /// <summary>
    /// Total messages in history.
    /// </summary>
    public int MessageCount => _history.Count;
}
