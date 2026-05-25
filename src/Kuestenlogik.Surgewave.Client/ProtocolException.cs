using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client;

/// <summary>
/// Exception thrown when a native protocol operation fails.
/// </summary>
public class ProtocolException : SurgewaveClientException
{
    /// <summary>
    /// The operation that failed.
    /// </summary>
    public SurgewaveOpCode Operation { get; }

    /// <summary>
    /// The error code returned by the broker.
    /// </summary>
    public SurgewaveErrorCode ErrorCode { get; }

    public ProtocolException() { }

    public ProtocolException(string message) : base(message) { }

    public ProtocolException(string message, Exception innerException) : base(message, innerException) { }

    public ProtocolException(SurgewaveOpCode operation, SurgewaveErrorCode errorCode)
        : base($"{GetOperationName(operation)} failed: {errorCode}")
    {
        Operation = operation;
        ErrorCode = errorCode;
    }

    public ProtocolException(string message, SurgewaveOpCode operation, SurgewaveErrorCode errorCode)
        : base(message)
    {
        Operation = operation;
        ErrorCode = errorCode;
    }

    private static string GetOperationName(SurgewaveOpCode opCode) => opCode switch
    {
        SurgewaveOpCode.Produce => "Send",
        SurgewaveOpCode.Fetch => "Receive",
        SurgewaveOpCode.ListOffsets => "ListOffsets",
        SurgewaveOpCode.CreateTopic => "CreateTopic",
        SurgewaveOpCode.DeleteTopic => "DeleteTopic",
        SurgewaveOpCode.GetMetadata => "GetMetadata",
        _ => opCode.ToString()
    };
}
