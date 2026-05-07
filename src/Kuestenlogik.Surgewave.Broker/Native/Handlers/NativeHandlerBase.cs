using Kuestenlogik.Surgewave.Broker.Native.Operations;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Base class for native protocol handlers using a registry pattern.
/// Eliminates boilerplate switch statements by registering operation factories.
/// </summary>
public abstract class NativeHandlerBase : INativeRequestHandler
{
    private readonly Dictionary<SurgewaveOpCode, Func<NativeRequestContext, ReadOnlyMemory<byte>, CancellationToken, Task>> _handlers = new();
    private readonly OperationExecutor _executor = new();

    /// <summary>
    /// The operation codes this handler can process (derived from registered operations).
    /// </summary>
    public IEnumerable<SurgewaveOpCode> SupportedOpCodes => _handlers.Keys;

    /// <summary>
    /// Register an operation handler that has both request and response.
    /// </summary>
    protected void Register<TRequest, TResponse>(
        SurgewaveOpCode opCode,
        Func<NativeRequestContext, IOperationHandler<TRequest, TResponse>> factory)
        where TRequest : struct
        where TResponse : struct
    {
        _handlers[opCode] = (ctx, payload, ct) => _executor.ExecuteAsync(factory(ctx), ctx, payload, ct);
    }

    /// <summary>
    /// Register a void operation handler (no response payload).
    /// </summary>
    protected void RegisterVoid<TRequest>(
        SurgewaveOpCode opCode,
        Func<NativeRequestContext, IVoidOperationHandler<TRequest>> factory)
        where TRequest : struct
    {
        _handlers[opCode] = (ctx, payload, ct) => _executor.ExecuteVoidAsync(factory(ctx), ctx, payload, ct);
    }

    /// <summary>
    /// Register a no-request operation handler (response only).
    /// </summary>
    protected void RegisterNoRequest<TResponse>(
        SurgewaveOpCode opCode,
        Func<NativeRequestContext, INoRequestOperationHandler<TResponse>> factory)
        where TResponse : struct
    {
        _handlers[opCode] = (ctx, _, ct) => _executor.ExecuteNoRequestAsync(factory(ctx), ctx, ct);
    }

    /// <summary>
    /// Handle a native protocol request by delegating to the registered operation.
    /// </summary>
    public Task HandleAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var preCheck = PreExecuteCheck(context, cancellationToken);
        if (preCheck != null)
            return preCheck;

        return _handlers.TryGetValue(context.Header.OpCode, out var handler)
            ? handler(context, payload, cancellationToken)
            : Task.CompletedTask;
    }

    /// <summary>
    /// Override to perform pre-execution checks (e.g., coordinator availability).
    /// Return a task that sends an error if the check fails, or null to continue.
    /// </summary>
    protected virtual Task? PreExecuteCheck(NativeRequestContext context, CancellationToken cancellationToken) => null;
}
