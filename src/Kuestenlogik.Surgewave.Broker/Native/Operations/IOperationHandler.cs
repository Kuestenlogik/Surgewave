using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Broker.Native.Operations;

/// <summary>
/// Interface for operation results that include an error code to be sent in the response header.
/// </summary>
public interface IOperationResult
{
    SurgewaveErrorCode ErrorCode { get; }
}

/// <summary>
/// Interface for broker operation handlers that process a request and produce a response.
/// </summary>
/// <typeparam name="TRequest">The request payload type.</typeparam>
/// <typeparam name="TResponse">The response payload type.</typeparam>
public interface IOperationHandler<TRequest, TResponse>
    where TRequest : struct
    where TResponse : struct
{
    /// <summary>
    /// The operation code this handler processes.
    /// </summary>
    SurgewaveOpCode OpCode { get; }

    /// <summary>
    /// Parse the request from the payload.
    /// </summary>
    TRequest ParseRequest(ref SurgewavePayloadReader reader);

    /// <summary>
    /// Validate the request. Throw SurgewaveOperationException if invalid.
    /// </summary>
    void ValidateRequest(in TRequest request);

    /// <summary>
    /// Execute the operation and return the response.
    /// </summary>
    Task<TResponse> ExecuteAsync(TRequest request, CancellationToken cancellationToken);

    /// <summary>
    /// Write the response to the writer.
    /// </summary>
    void WriteResponse(IPayloadWriter writer, in TResponse response);
}

/// <summary>
/// Interface for broker operation handlers that process a request with no response payload.
/// </summary>
/// <typeparam name="TRequest">The request payload type.</typeparam>
public interface IVoidOperationHandler<TRequest>
    where TRequest : struct
{
    /// <summary>
    /// The operation code this handler processes.
    /// </summary>
    SurgewaveOpCode OpCode { get; }

    /// <summary>
    /// Parse the request from the payload.
    /// </summary>
    TRequest ParseRequest(ref SurgewavePayloadReader reader);

    /// <summary>
    /// Validate the request. Throw SurgewaveOperationException if invalid.
    /// </summary>
    void ValidateRequest(in TRequest request);

    /// <summary>
    /// Execute the operation.
    /// </summary>
    Task ExecuteAsync(TRequest request, CancellationToken cancellationToken);
}

/// <summary>
/// Interface for broker operation handlers that have no request payload.
/// </summary>
/// <typeparam name="TResponse">The response payload type.</typeparam>
public interface INoRequestOperationHandler<TResponse>
    where TResponse : struct
{
    /// <summary>
    /// The operation code this handler processes.
    /// </summary>
    SurgewaveOpCode OpCode { get; }

    /// <summary>
    /// Execute the operation and return the response.
    /// </summary>
    Task<TResponse> ExecuteAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Write the response to the writer.
    /// </summary>
    void WriteResponse(IPayloadWriter writer, in TResponse response);
}
