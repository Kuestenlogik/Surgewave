using Kuestenlogik.Surgewave.Broker.Native.Handlers;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Broker.Native.Operations;

/// <summary>
/// Executes broker operations with a unified template method pattern.
/// Handles parsing, validation, execution, response writing, and error handling.
/// </summary>
public sealed class OperationExecutor
{
    /// <summary>
    /// Execute an operation handler that has both request and response.
    /// </summary>
    public async Task ExecuteAsync<TRequest, TResponse>(
        IOperationHandler<TRequest, TResponse> handler,
        NativeRequestContext context,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
        where TRequest : struct
        where TResponse : struct
    {
        try
        {
            // Parse request
            var reader = new SurgewavePayloadReader(payload.Span);
            var request = handler.ParseRequest(ref reader);

            // Validate request
            handler.ValidateRequest(in request);

            // Execute operation
            var response = await handler.ExecuteAsync(request, cancellationToken);

            // Write and send response
            using var writer = new BigEndianWriter();
            handler.WriteResponse(writer, in response);

            // Use error code from result if available, otherwise None
            var errorCode = GetErrorCode(response);

            await context.SendResponseAsync(
                context.Header.RequestId,
                handler.OpCode,
                errorCode,
                writer.AsMemory(),
                cancellationToken);
        }
        catch (SurgewaveOperationException ex)
        {
            await context.SendErrorAsync(
                context.Header.RequestId,
                handler.OpCode,
                ex.ErrorCode,
                ex.Message,
                cancellationToken);
        }
        catch (InvalidOperationException ex)
        {
            await context.SendErrorAsync(
                context.Header.RequestId,
                handler.OpCode,
                SurgewaveErrorCode.InvalidRequest,
                ex.Message,
                cancellationToken);
        }
    }

    private static SurgewaveErrorCode GetErrorCode<T>(T response) where T : struct
        => response is IOperationResult opResult ? opResult.ErrorCode : SurgewaveErrorCode.None;

    /// <summary>
    /// Execute a void operation handler (no response payload).
    /// </summary>
    public async Task ExecuteVoidAsync<TRequest>(
        IVoidOperationHandler<TRequest> handler,
        NativeRequestContext context,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
        where TRequest : struct
    {
        try
        {
            // Parse request
            var reader = new SurgewavePayloadReader(payload.Span);
            var request = handler.ParseRequest(ref reader);

            // Validate request
            handler.ValidateRequest(in request);

            // Execute operation
            await handler.ExecuteAsync(request, cancellationToken);

            // Send success response
            await context.SendResponseAsync(
                context.Header.RequestId,
                handler.OpCode,
                SurgewaveErrorCode.None,
                Array.Empty<byte>(),
                cancellationToken);
        }
        catch (SurgewaveOperationException ex)
        {
            await context.SendErrorAsync(
                context.Header.RequestId,
                handler.OpCode,
                ex.ErrorCode,
                ex.Message,
                cancellationToken);
        }
        catch (InvalidOperationException ex)
        {
            await context.SendErrorAsync(
                context.Header.RequestId,
                handler.OpCode,
                SurgewaveErrorCode.InvalidRequest,
                ex.Message,
                cancellationToken);
        }
    }

    /// <summary>
    /// Execute a no-request operation handler (response only).
    /// </summary>
    public async Task ExecuteNoRequestAsync<TResponse>(
        INoRequestOperationHandler<TResponse> handler,
        NativeRequestContext context,
        CancellationToken cancellationToken)
        where TResponse : struct
    {
        try
        {
            // Execute operation
            var response = await handler.ExecuteAsync(cancellationToken);

            // Write and send response
            using var writer = new BigEndianWriter();
            handler.WriteResponse(writer, in response);

            // Use error code from result if available, otherwise None
            var errorCode = GetErrorCode(response);

            await context.SendResponseAsync(
                context.Header.RequestId,
                handler.OpCode,
                errorCode,
                writer.AsMemory(),
                cancellationToken);
        }
        catch (SurgewaveOperationException ex)
        {
            await context.SendErrorAsync(
                context.Header.RequestId,
                handler.OpCode,
                ex.ErrorCode,
                ex.Message,
                cancellationToken);
        }
        catch (InvalidOperationException ex)
        {
            await context.SendErrorAsync(
                context.Header.RequestId,
                handler.OpCode,
                SurgewaveErrorCode.InvalidRequest,
                ex.Message,
                cancellationToken);
        }
    }
}
