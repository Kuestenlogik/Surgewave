using System.Net;

namespace Kuestenlogik.Surgewave.Pipelines.Publishing;

/// <summary>Thrown when the broker rejects a pipeline operation or cannot be reached.</summary>
public sealed class PipelinePublishException : Exception
{
    public PipelinePublishException(string message)
        : base(message)
    {
    }

    public PipelinePublishException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public PipelinePublishException(string message, HttpStatusCode statusCode)
        : base(message)
    {
        StatusCode = statusCode;
    }

    /// <summary>The HTTP status returned by the broker, when the failure was an API response.</summary>
    public HttpStatusCode? StatusCode { get; }
}
