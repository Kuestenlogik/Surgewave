using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Client.Native.Commands;

/// <summary>
/// Represents a Surgewave protocol command that can be executed against a broker.
/// </summary>
/// <typeparam name="TResult">The result type returned by the command.</typeparam>
public interface ISurgewaveCommand<TResult>
{
    /// <summary>
    /// The operation code for this command.
    /// </summary>
    SurgewaveOpCode OpCode { get; }

    /// <summary>
    /// Write the request payload to the writer.
    /// </summary>
    void WriteRequest(ref SurgewavePayloadWriter writer);

    /// <summary>
    /// Estimate the size needed for the request payload.
    /// </summary>
    int EstimateRequestSize();

    /// <summary>
    /// Read and parse the response payload, returning the result.
    /// </summary>
    TResult ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header);
}

/// <summary>
/// Represents a Surgewave command that returns no value (void operations).
/// </summary>
public interface ISurgewaveVoidCommand : ISurgewaveCommand<Unit>
{
    /// <summary>
    /// Validate the response. Override to check response-specific error codes.
    /// </summary>
    void ValidateResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header);
}
