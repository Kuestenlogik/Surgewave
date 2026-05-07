using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Commands;

/// <summary>
/// Exception thrown when a Surgewave protocol operation fails.
/// </summary>
public sealed class SurgewaveProtocolException : Exception
{
    /// <summary>
    /// The error code returned by the broker.
    /// </summary>
    public SurgewaveErrorCode ErrorCode { get; }

    /// <summary>
    /// The operation that failed.
    /// </summary>
    public SurgewaveOpCode OpCode { get; }

    /// <summary>
    /// Additional error message from the broker, if any.
    /// </summary>
    public string? BrokerMessage { get; }

    public SurgewaveProtocolException()
        : base("Surgewave protocol error")
    {
    }

    public SurgewaveProtocolException(string message)
        : base(message)
    {
    }

    public SurgewaveProtocolException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public SurgewaveProtocolException(SurgewaveErrorCode errorCode, SurgewaveOpCode opCode, string? brokerMessage = null)
        : base(FormatMessage(errorCode, opCode, brokerMessage))
    {
        ErrorCode = errorCode;
        OpCode = opCode;
        BrokerMessage = brokerMessage;
    }

    public SurgewaveProtocolException(SurgewaveErrorCode errorCode, SurgewaveOpCode opCode, string? brokerMessage, Exception innerException)
        : base(FormatMessage(errorCode, opCode, brokerMessage), innerException)
    {
        ErrorCode = errorCode;
        OpCode = opCode;
        BrokerMessage = brokerMessage;
    }

    private static string FormatMessage(SurgewaveErrorCode errorCode, SurgewaveOpCode opCode, string? brokerMessage)
    {
        var message = $"Surgewave protocol error: {errorCode} for operation {opCode}";
        if (!string.IsNullOrEmpty(brokerMessage))
            message += $" - {brokerMessage}";
        return message;
    }
}
