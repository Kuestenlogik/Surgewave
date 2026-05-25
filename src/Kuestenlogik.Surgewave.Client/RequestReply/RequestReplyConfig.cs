namespace Kuestenlogik.Surgewave.Client.RequestReply;

/// <summary>
/// Configuration for request-reply operations.
/// Controls timeouts, topic naming, and auto-creation behavior.
/// </summary>
public sealed class RequestReplyConfig
{
    /// <summary>
    /// Default timeout for request-reply operations.
    /// If no response is received within this duration, a <see cref="TimeoutException"/> is thrown.
    /// </summary>
    public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Prefix used for auto-generated reply topics.
    /// The full reply topic name follows the pattern: {ReplyTopicPrefix}.{clientId}.
    /// </summary>
    public string ReplyTopicPrefix { get; set; } = "__reply";

    /// <summary>
    /// Whether to automatically create request and reply topics if they do not exist.
    /// </summary>
    public bool AutoCreateTopics { get; set; } = true;
}
