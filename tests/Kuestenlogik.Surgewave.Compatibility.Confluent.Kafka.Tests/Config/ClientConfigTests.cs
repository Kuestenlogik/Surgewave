namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Config;

/// <summary>
/// Pins the dictionary-backed semantics of ClientConfig: canonical librdkafka key names,
/// case-insensitive lookup, null-removal, copy construction, enumeration, and the
/// SASL mechanism / security protocol wire-string mappings.
/// </summary>
public class ClientConfigTests
{
    [Fact]
    public void Indexer_UnknownKey_ReturnsNull()
    {
        var config = new ClientConfig();
        Assert.Null(config["does.not.exist"]);
    }

    [Fact]
    public void Indexer_SetAndGet_RoundTrips()
    {
        var config = new ClientConfig
        {
            ["custom.setting"] = "custom-value"
        };

        Assert.Equal("custom-value", config["custom.setting"]);
    }

    [Fact]
    public void Indexer_KeysAreCaseInsensitive()
    {
        var config = new ClientConfig
        {
            ["BOOTSTRAP.SERVERS"] = "localhost:9092"
        };

        Assert.Equal("localhost:9092", config.BootstrapServers);
        Assert.Equal("localhost:9092", config["bootstrap.servers"]);
    }

    [Fact]
    public void Setter_NullValue_RemovesProperty()
    {
        var config = new ClientConfig
        {
            BootstrapServers = "localhost:9092"
        };

        config.BootstrapServers = null;

        Assert.Null(config["bootstrap.servers"]);
        Assert.Empty(config);
    }

    [Fact]
    public void CopyConstructor_CopiesAllProperties()
    {
        var source = new Dictionary<string, string>
        {
            ["bootstrap.servers"] = "broker:9092",
            ["client.id"] = "client-1"
        };

        var config = new ClientConfig(source);

        Assert.Equal("broker:9092", config.BootstrapServers);
        Assert.Equal("client-1", config.ClientId);
    }

    [Fact]
    public void Enumeration_YieldsAllStoredPairs()
    {
        var config = new ClientConfig
        {
            BootstrapServers = "broker:9092",
            ClientId = "client-1"
        };

        var pairs = config.ToDictionary(p => p.Key, p => p.Value);

        Assert.Equal(2, pairs.Count);
        Assert.Equal("broker:9092", pairs["bootstrap.servers"]);
        Assert.Equal("client-1", pairs["client.id"]);
    }

    [Fact]
    public void TypedProperties_WriteCanonicalLibrdkafkaKeys()
    {
        var config = new ClientConfig
        {
            BootstrapServers = "broker:9092",
            ClientId = "client-1",
            SaslUsername = "user",
            SaslPassword = "secret",
            SslCaLocation = "/certs/ca.pem",
            Debug = "broker,topic",
            ConnectionsMaxIdleMs = 540000,
            SurgewaveProtocol = "auto"
        };

        Assert.Equal("broker:9092", config["bootstrap.servers"]);
        Assert.Equal("client-1", config["client.id"]);
        Assert.Equal("user", config["sasl.username"]);
        Assert.Equal("secret", config["sasl.password"]);
        Assert.Equal("/certs/ca.pem", config["ssl.ca.location"]);
        Assert.Equal("broker,topic", config["debug"]);
        Assert.Equal("540000", config["connections.max.idle.ms"]);
        Assert.Equal("auto", config["surgewave.protocol"]);
    }

    [Theory]
    [InlineData(SaslMechanism.Plain, "PLAIN")]
    [InlineData(SaslMechanism.ScramSha256, "SCRAM-SHA-256")]
    [InlineData(SaslMechanism.ScramSha512, "SCRAM-SHA-512")]
    [InlineData(SaslMechanism.OAuthBearer, "OAUTHBEARER")]
    public void SaslMechanism_Setter_WritesWireString(SaslMechanism mechanism, string expected)
    {
        var config = new ClientConfig { SaslMechanism = mechanism };
        Assert.Equal(expected, config["sasl.mechanism"]);
    }

    [Theory]
    [InlineData("PLAIN", SaslMechanism.Plain)]
    [InlineData("SCRAM-SHA-256", SaslMechanism.ScramSha256)]
    [InlineData("SCRAM-SHA-512", SaslMechanism.ScramSha512)]
    [InlineData("OAUTHBEARER", SaslMechanism.OAuthBearer)]
    public void SaslMechanism_Getter_ParsesWireString(string raw, SaslMechanism expected)
    {
        var config = new ClientConfig { ["sasl.mechanism"] = raw };
        Assert.Equal(expected, config.SaslMechanism);
    }

    [Fact]
    public void SaslMechanism_UnknownWireString_ReturnsNull()
    {
        var config = new ClientConfig { ["sasl.mechanism"] = "GSSAPI" };
        Assert.Null(config.SaslMechanism);
    }

    [Fact]
    public void SaslMechanism_SetNull_RemovesKey()
    {
        var config = new ClientConfig { SaslMechanism = SaslMechanism.Plain };

        config.SaslMechanism = null;

        Assert.Null(config["sasl.mechanism"]);
    }

    [Theory]
    [InlineData(SecurityProtocol.Plaintext, "plaintext")]
    [InlineData(SecurityProtocol.Ssl, "ssl")]
    [InlineData(SecurityProtocol.SaslPlaintext, "sasl_plaintext")]
    [InlineData(SecurityProtocol.SaslSsl, "sasl_ssl")]
    public void SecurityProtocol_Setter_WritesWireString(SecurityProtocol protocol, string expected)
    {
        var config = new ClientConfig { SecurityProtocol = protocol };
        Assert.Equal(expected, config["security.protocol"]);
    }

    [Theory]
    [InlineData("plaintext", SecurityProtocol.Plaintext)]
    [InlineData("ssl", SecurityProtocol.Ssl)]
    [InlineData("sasl_plaintext", SecurityProtocol.SaslPlaintext)]
    [InlineData("sasl_ssl", SecurityProtocol.SaslSsl)]
    public void SecurityProtocol_Getter_ParsesWireString(string raw, SecurityProtocol expected)
    {
        var config = new ClientConfig { ["security.protocol"] = raw };
        Assert.Equal(expected, config.SecurityProtocol);
    }

    [Fact]
    public void SecurityProtocol_UnknownWireString_ReturnsNull()
    {
        var config = new ClientConfig { ["security.protocol"] = "carrier-pigeon" };
        Assert.Null(config.SecurityProtocol);
    }

    [Fact]
    public void ConnectionsMaxIdleMs_NonNumericRawValue_ReturnsNull()
    {
        var config = new ClientConfig { ["connections.max.idle.ms"] = "forever" };
        Assert.Null(config.ConnectionsMaxIdleMs);
    }
}
