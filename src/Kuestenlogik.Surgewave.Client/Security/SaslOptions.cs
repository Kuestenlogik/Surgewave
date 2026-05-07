namespace Kuestenlogik.Surgewave.Client.Security;

/// <summary>
/// SASL credentials sent to the broker during the SASL handshake
/// (Kafka <c>SaslHandshake</c> + <c>SaslAuthenticate</c> request pair).
/// The handshake runs after the TCP/TLS connect and before the first
/// produce / fetch frame — every subsequent connection on the same
/// transport is authenticated.
/// </summary>
public sealed record SaslOptions
{
    /// <summary>
    /// The SASL mechanism advertised to the broker. Surgewave's first
    /// slice implements <see cref="SaslMechanism.Plain"/>; the SCRAM
    /// and OAUTHBEARER values throw on use until their respective
    /// implementation slices land.
    /// </summary>
    public required SaslMechanism Mechanism { get; init; }

    /// <summary>Username (or client id for OAUTHBEARER).</summary>
    public required string Username { get; init; }

    /// <summary>Password (or bearer token for OAUTHBEARER).</summary>
    public required string Password { get; init; }
}
