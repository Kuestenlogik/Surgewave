using Kuestenlogik.Surgewave.Client.Diagnostics;

namespace Kuestenlogik.Surgewave.Client;

/// <summary>
/// Exception thrown when connection to the broker fails.
/// </summary>
public class BrokerConnectionException : SurgewaveClientException, IRecoverableException
{
    /// <summary>
    /// The broker host that failed to connect.
    /// </summary>
    public string? Host { get; }

    /// <summary>
    /// The broker port that failed to connect.
    /// </summary>
    public int? Port { get; }

    /// <summary>
    /// Gets a suggestion for how to recover from this error.
    /// </summary>
    public string? RecoverySuggestion => Diagnostics.RecoverySuggestion.ForConnectionError(Host, Port);

    /// <summary>Initializes a new instance of <see cref="BrokerConnectionException"/>.</summary>
    public BrokerConnectionException() { }

    /// <summary>Initializes a new instance of <see cref="BrokerConnectionException"/> with a message.</summary>
    /// <param name="message">The error message.</param>
    public BrokerConnectionException(string message) : base(message) { }

    /// <summary>Initializes a new instance of <see cref="BrokerConnectionException"/> with a message and inner exception.</summary>
    /// <param name="message">The error message.</param>
    /// <param name="innerException">The inner exception.</param>
    public BrokerConnectionException(string message, Exception innerException) : base(message, innerException) { }

    /// <summary>Initializes a new instance of <see cref="BrokerConnectionException"/> with broker details.</summary>
    /// <param name="message">The error message.</param>
    /// <param name="host">The broker host that failed to connect.</param>
    /// <param name="port">The broker port that failed to connect.</param>
    /// <param name="innerException">The inner exception, if any.</param>
    public BrokerConnectionException(string message, string? host, int? port, Exception? innerException = null)
        : base(message, innerException!)
    {
        Host = host;
        Port = port;
    }
}
