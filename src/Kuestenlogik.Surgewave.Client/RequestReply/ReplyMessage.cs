namespace Kuestenlogik.Surgewave.Client.RequestReply;

/// <summary>
/// Represents a reply message received by the requesting client.
/// Contains the correlated response value or error information.
/// </summary>
public sealed record ReplyMessage(
    string CorrelationId,
    byte[] Value,
    DateTimeOffset Timestamp,
    bool IsError,
    string? ErrorMessage);
