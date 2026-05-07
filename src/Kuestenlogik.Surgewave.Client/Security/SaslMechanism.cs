namespace Kuestenlogik.Surgewave.Client.Security;

/// <summary>
/// SASL authentication mechanism negotiated during the Kafka SASL
/// handshake. Surgewave's hand-rolled Kafka-wire client implements PLAIN
/// in the first slice; the SCRAM and OAUTHBEARER values are reserved
/// for follow-up implementations so the public enum stays stable.
/// </summary>
public enum SaslMechanism
{
    /// <summary>
    /// SASL PLAIN — username + password in clear text. Pair with TLS
    /// in production; on its own only safe inside a trusted network.
    /// </summary>
    Plain,

    /// <summary>
    /// SASL SCRAM-SHA-256 (RFC 5802). Reserved for a follow-up slice —
    /// using it before that slice lands throws at handshake time.
    /// </summary>
    ScramSha256,

    /// <summary>
    /// SASL SCRAM-SHA-512 (RFC 5802). Reserved for a follow-up slice.
    /// </summary>
    ScramSha512,

    /// <summary>
    /// SASL OAUTHBEARER — bearer token presented to the broker.
    /// Reserved for a follow-up slice.
    /// </summary>
    OAuthBearer,
}
