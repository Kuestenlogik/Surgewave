namespace Kuestenlogik.Surgewave.Client.RequestReply;

/// <summary>
/// Exception thrown when a request-reply operation fails due to a server-side error.
/// The responder processed the request but returned an error response.
/// </summary>
public sealed class RequestReplyException : SurgewaveClientException
{
    /// <summary>
    /// The correlation ID of the failed request.
    /// </summary>
    public string CorrelationId { get; }

    /// <summary>
    /// Creates a new request-reply exception.
    /// </summary>
    public RequestReplyException(string message, string correlationId)
        : base(message)
    {
        CorrelationId = correlationId;
    }

    /// <summary>
    /// Creates a new request-reply exception with an inner exception.
    /// </summary>
    public RequestReplyException(string message, string correlationId, Exception innerException)
        : base(message, innerException)
    {
        CorrelationId = correlationId;
    }
}
