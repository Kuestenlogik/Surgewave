using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Client.Native.Commands;

/// <summary>
/// Base class for commands that have no request payload.
/// Derived classes only need to implement ReadResponse.
/// </summary>
/// <typeparam name="TResult">The result type returned by the command.</typeparam>
public abstract class NoRequestCommand<TResult> : ISurgewaveCommand<TResult>
{
    /// <inheritdoc />
    public abstract SurgewaveOpCode OpCode { get; }

    /// <inheritdoc />
    public void WriteRequest(ref SurgewavePayloadWriter writer) { }

    /// <inheritdoc />
    public int EstimateRequestSize() => 0;

    /// <inheritdoc />
    public abstract TResult ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header);
}

/// <summary>
/// Base class for void commands.
/// Derived classes only need to implement WriteRequest, EstimateRequestSize, and optionally override ValidateResponse.
/// </summary>
public abstract class VoidCommand : ISurgewaveVoidCommand
{
    /// <inheritdoc />
    public abstract SurgewaveOpCode OpCode { get; }

    /// <inheritdoc />
    public abstract void WriteRequest(ref SurgewavePayloadWriter writer);

    /// <inheritdoc />
    public abstract int EstimateRequestSize();

    /// <inheritdoc />
    public Unit ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        ValidateResponse(ref reader, header);
        return Unit.Value;
    }

    /// <inheritdoc />
    public virtual void ValidateResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header) { }
}
