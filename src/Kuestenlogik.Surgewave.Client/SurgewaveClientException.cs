namespace Kuestenlogik.Surgewave.Client;

/// <summary>
/// Base exception for all Surgewave client errors.
/// </summary>
public class SurgewaveClientException : Exception
{
    public SurgewaveClientException() { }
    public SurgewaveClientException(string message) : base(message) { }
    public SurgewaveClientException(string message, Exception innerException) : base(message, innerException) { }
}
