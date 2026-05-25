using System.Collections;

namespace Confluent.Kafka;

/// <summary>
/// Base configuration class for Kafka clients.
/// </summary>
public class ClientConfig : IEnumerable<KeyValuePair<string, string>>
{
    private readonly Dictionary<string, string> _properties = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Creates an empty configuration.
    /// </summary>
    public ClientConfig() { }

    /// <summary>
    /// Creates a configuration from existing properties.
    /// </summary>
    public ClientConfig(IEnumerable<KeyValuePair<string, string>> config)
    {
        foreach (var (key, value) in config)
        {
            _properties[key] = value;
        }
    }

    /// <summary>
    /// Gets or sets a configuration property.
    /// </summary>
    protected string? GetOrDefault(string key) =>
        _properties.TryGetValue(key, out var value) ? value : null;

    /// <summary>
    /// Sets a configuration property.
    /// </summary>
    protected void Set(string key, string? value)
    {
        if (value is null)
            _properties.Remove(key);
        else
            _properties[key] = value;
    }

    /// <summary>
    /// Bootstrap servers for initial cluster connection.
    /// </summary>
    public string? BootstrapServers
    {
        get => GetOrDefault("bootstrap.servers");
        set => Set("bootstrap.servers", value);
    }

    /// <summary>
    /// Client identifier for logging and debugging.
    /// </summary>
    public string? ClientId
    {
        get => GetOrDefault("client.id");
        set => Set("client.id", value);
    }

    /// <summary>
    /// SASL mechanism for authentication.
    /// </summary>
    public SaslMechanism? SaslMechanism
    {
        get => GetOrDefault("sasl.mechanism") switch
        {
            "PLAIN" => Confluent.Kafka.SaslMechanism.Plain,
            "SCRAM-SHA-256" => Confluent.Kafka.SaslMechanism.ScramSha256,
            "SCRAM-SHA-512" => Confluent.Kafka.SaslMechanism.ScramSha512,
            "OAUTHBEARER" => Confluent.Kafka.SaslMechanism.OAuthBearer,
            _ => null
        };
        set => Set("sasl.mechanism", value switch
        {
            Confluent.Kafka.SaslMechanism.Plain => "PLAIN",
            Confluent.Kafka.SaslMechanism.ScramSha256 => "SCRAM-SHA-256",
            Confluent.Kafka.SaslMechanism.ScramSha512 => "SCRAM-SHA-512",
            Confluent.Kafka.SaslMechanism.OAuthBearer => "OAUTHBEARER",
            _ => null
        });
    }

    /// <summary>
    /// SASL username for authentication.
    /// </summary>
    public string? SaslUsername
    {
        get => GetOrDefault("sasl.username");
        set => Set("sasl.username", value);
    }

    /// <summary>
    /// SASL password for authentication.
    /// </summary>
    public string? SaslPassword
    {
        get => GetOrDefault("sasl.password");
        set => Set("sasl.password", value);
    }

    /// <summary>
    /// Security protocol.
    /// </summary>
    public SecurityProtocol? SecurityProtocol
    {
        get => GetOrDefault("security.protocol") switch
        {
            "plaintext" => Confluent.Kafka.SecurityProtocol.Plaintext,
            "ssl" => Confluent.Kafka.SecurityProtocol.Ssl,
            "sasl_plaintext" => Confluent.Kafka.SecurityProtocol.SaslPlaintext,
            "sasl_ssl" => Confluent.Kafka.SecurityProtocol.SaslSsl,
            _ => null
        };
        set => Set("security.protocol", value switch
        {
            Confluent.Kafka.SecurityProtocol.Plaintext => "plaintext",
            Confluent.Kafka.SecurityProtocol.Ssl => "ssl",
            Confluent.Kafka.SecurityProtocol.SaslPlaintext => "sasl_plaintext",
            Confluent.Kafka.SecurityProtocol.SaslSsl => "sasl_ssl",
            _ => null
        });
    }

    /// <summary>
    /// SSL CA certificate location.
    /// </summary>
    public string? SslCaLocation
    {
        get => GetOrDefault("ssl.ca.location");
        set => Set("ssl.ca.location", value);
    }

    /// <summary>
    /// Debug contexts to enable (comma-separated).
    /// </summary>
    public string? Debug
    {
        get => GetOrDefault("debug");
        set => Set("debug", value);
    }

    /// <summary>
    /// Connection idle timeout in milliseconds.
    /// </summary>
    public int? ConnectionsMaxIdleMs
    {
        get => int.TryParse(GetOrDefault("connections.max.idle.ms"), out var v) ? v : null;
        set => Set("connections.max.idle.ms", value?.ToString());
    }

    /// <summary>
    /// Surgewave-specific: Protocol to use. Can be "surgewave", "kafka", or "auto".
    /// </summary>
    public string? SurgewaveProtocol
    {
        get => GetOrDefault("surgewave.protocol");
        set => Set("surgewave.protocol", value);
    }

    /// <inheritdoc/>
    public IEnumerator<KeyValuePair<string, string>> GetEnumerator() =>
        _properties.GetEnumerator();

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>
    /// Gets or sets a property by key.
    /// </summary>
    public string? this[string key]
    {
        get => GetOrDefault(key);
        set => Set(key, value);
    }
}

/// <summary>
/// SASL authentication mechanism.
/// </summary>
public enum SaslMechanism
{
    /// <summary>PLAIN mechanism.</summary>
    Plain,

    /// <summary>SCRAM-SHA-256 mechanism.</summary>
    ScramSha256,

    /// <summary>SCRAM-SHA-512 mechanism.</summary>
    ScramSha512,

    /// <summary>OAUTHBEARER mechanism.</summary>
    OAuthBearer
}

/// <summary>
/// Security protocol for broker connections.
/// </summary>
public enum SecurityProtocol
{
    /// <summary>Plain text (no encryption).</summary>
    Plaintext,

    /// <summary>SSL encryption.</summary>
    Ssl,

    /// <summary>SASL authentication without encryption.</summary>
    SaslPlaintext,

    /// <summary>SASL authentication with SSL encryption.</summary>
    SaslSsl
}
