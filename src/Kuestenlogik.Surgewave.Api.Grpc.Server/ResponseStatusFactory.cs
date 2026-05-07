using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// Factory for creating common ResponseStatus patterns.
/// </summary>
public static class ResponseStatusFactory
{
    /// <summary>
    /// Success response status.
    /// </summary>
    public static ResponseStatus Success { get; } = new() { ErrorCode = ErrorCode.None };

    /// <summary>
    /// Creates an error response with the specified error code and message.
    /// </summary>
    public static ResponseStatus Error(ErrorCode code, string message) => new()
    {
        ErrorCode = code,
        ErrorMessage = message
    };

    /// <summary>
    /// Creates an unknown error response from an exception.
    /// </summary>
    public static ResponseStatus FromException(Exception ex) => new()
    {
        ErrorCode = ErrorCode.Unknown,
        ErrorMessage = ex.Message
    };

    /// <summary>
    /// Creates a not found error for topics/partitions.
    /// </summary>
    public static ResponseStatus TopicNotFound(string topic, int? partition = null) => new()
    {
        ErrorCode = ErrorCode.UnknownTopicOrPartition,
        ErrorMessage = partition.HasValue
            ? $"Topic '{topic}' partition {partition} not found"
            : $"Topic '{topic}' not found"
    };

    /// <summary>
    /// Creates a not found error for generic resources.
    /// </summary>
    public static ResponseStatus NotFound(string resourceType, string name) => new()
    {
        ErrorCode = ErrorCode.Unknown,
        ErrorMessage = $"{resourceType} '{name}' not found"
    };

    /// <summary>
    /// Creates a coordinator not available error.
    /// </summary>
    public static ResponseStatus CoordinatorNotAvailable(string? message = null) => new()
    {
        ErrorCode = ErrorCode.CoordinatorNotAvailable,
        ErrorMessage = message ?? "Coordinator not available"
    };
}
