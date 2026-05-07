namespace Kuestenlogik.Surgewave.Client;

/// <summary>
/// Exception thrown when a broker returns an empty or invalid response.
/// </summary>
public class BrokerResponseException : SurgewaveClientException
{
    /// <summary>
    /// The API that received the invalid response.
    /// </summary>
    public string? ApiName { get; }

    public BrokerResponseException() { }

    public BrokerResponseException(string message) : base(message) { }

    public BrokerResponseException(string message, Exception innerException) : base(message, innerException) { }

    public BrokerResponseException(string message, string? apiName)
        : base(message)
    {
        ApiName = apiName;
    }
}
