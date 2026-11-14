namespace Kuestenlogik.Surgewave.Protocol.Native;

/// <summary>
/// A payload that does not hold what its reader was to read from it: it ends before the field does, or names a
/// length it does not have.
/// </summary>
/// <remarks>
/// Thrown by <see cref="SurgewavePayloadReader"/>, which used to run into the end of its buffer and throw whatever
/// the runtime throws there — an <see cref="IndexOutOfRangeException"/>, an <see cref="ArgumentOutOfRangeException"/>
/// — which nothing above it could tell from a fault of its own. A broker answered a request it could not read as
/// one it had failed to carry out (#290).
/// </remarks>
public sealed class SurgewavePayloadException : Exception
{
    public SurgewavePayloadException()
        : base("The payload does not hold what was to be read from it.")
    {
    }

    public SurgewavePayloadException(string message)
        : base(message)
    {
    }

    public SurgewavePayloadException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    internal static SurgewavePayloadException At(int position, int wanted, int remaining) => new(
        wanted < 0
            ? $"The payload names a length of {wanted} at byte {position}."
            : $"The payload ends at byte {position + remaining}; {wanted} bytes were to be read from byte {position}.");
}
