namespace Kuestenlogik.Surgewave.Client.RequestReply;

/// <summary>
/// Represents an incoming request message on the server side.
/// Contains the correlation ID and reply topic needed to route the response.
/// </summary>
public sealed record RequestMessage(
    string CorrelationId,
    string ReplyTopic,
    byte[] Key,
    byte[] Value,
    DateTimeOffset Timestamp,
    IReadOnlyDictionary<string, string>? Headers);
