namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Tracks the authentication state of a client connection
/// </summary>
public sealed class ConnectionState
{
    /// <summary>
    /// The IP address of the connected client (for ACL host-based authorization)
    /// </summary>
    public string ClientHost { get; }

    /// <summary>
    /// Creates a new connection state with the specified client host
    /// </summary>
    /// <param name="clientHost">The IP address of the connected client</param>
    public ConnectionState(string clientHost)
    {
        ClientHost = clientHost;
    }

    /// <summary>
    /// Whether the connection has been authenticated
    /// </summary>
    public bool IsAuthenticated { get; private set; }

    /// <summary>
    /// The authenticated username (null if not authenticated)
    /// </summary>
    public string? AuthenticatedUser { get; private set; }

    /// <summary>
    /// The SASL mechanism used for authentication
    /// </summary>
    public string? SaslMechanism { get; private set; }

    /// <summary>
    /// The mechanism negotiated during handshake (before authentication)
    /// </summary>
    public string? NegotiatedMechanism { get; private set; }

    /// <summary>
    /// Time when authentication completed
    /// </summary>
    public DateTime? AuthenticatedAt { get; private set; }

    /// <summary>
    /// SCRAM session state for multi-step authentication
    /// </summary>
    public ScramSession? ScramSession { get; private set; }

    /// <summary>
    /// Mark this connection as awaiting authentication with the specified mechanism
    /// </summary>
    public void SetNegotiatedMechanism(string mechanism)
    {
        NegotiatedMechanism = mechanism;

        // Initialize SCRAM session if needed
        if (mechanism.StartsWith("SCRAM-", StringComparison.OrdinalIgnoreCase))
        {
            ScramSession = new ScramSession();
        }
    }

    /// <summary>
    /// Mark this connection as authenticated
    /// </summary>
    public void SetAuthenticated(string username, string mechanism)
    {
        IsAuthenticated = true;
        AuthenticatedUser = username;
        SaslMechanism = mechanism;
        AuthenticatedAt = DateTime.UtcNow;
        ScramSession = null; // Clear session after successful auth
    }

    /// <summary>
    /// Reset authentication state (e.g., for re-authentication)
    /// </summary>
    public void Reset()
    {
        IsAuthenticated = false;
        AuthenticatedUser = null;
        SaslMechanism = null;
        NegotiatedMechanism = null;
        AuthenticatedAt = null;
        ScramSession = null;
    }
}
