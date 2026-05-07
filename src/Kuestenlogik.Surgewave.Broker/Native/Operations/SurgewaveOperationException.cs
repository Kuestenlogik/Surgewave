using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Broker.Native.Operations;

/// <summary>
/// Exception thrown by operation handlers to indicate an error with a specific error code.
/// </summary>
public sealed class SurgewaveOperationException : Exception
{
    /// <summary>
    /// The Surgewave error code for this operation failure.
    /// </summary>
    public SurgewaveErrorCode ErrorCode { get; }

    public SurgewaveOperationException() : base() => ErrorCode = SurgewaveErrorCode.UnknownError;

    public SurgewaveOperationException(string message) : base(message) => ErrorCode = SurgewaveErrorCode.UnknownError;

    public SurgewaveOperationException(string message, Exception innerException) : base(message, innerException)
        => ErrorCode = SurgewaveErrorCode.UnknownError;

    public SurgewaveOperationException(SurgewaveErrorCode errorCode, string message) : base(message)
        => ErrorCode = errorCode;

    public SurgewaveOperationException(SurgewaveErrorCode errorCode, string message, Exception innerException)
        : base(message, innerException)
        => ErrorCode = errorCode;
}
