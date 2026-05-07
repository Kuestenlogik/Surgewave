using Kuestenlogik.Surgewave.Client;
using Kuestenlogik.Surgewave.Client.Security;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// Unit tests for the public SSL / SASL configuration shape exposed by
/// <see cref="SurgewaveClientBuilder"/>. The builder threads the options
/// onto the Kafka-protocol client and rejects them on the native
/// protocol where TLS / SASL aren't wired yet.
/// </summary>
public class SecurityOptionsTests
{
    private const string DummyCertPem =
        "-----BEGIN CERTIFICATE-----\nABC\n-----END CERTIFICATE-----";
    private const string DummyKeyPem =
        "-----BEGIN PRIVATE KEY-----\nDEF\n-----END PRIVATE KEY-----";

    [Fact]
    public void SslOptions_RecordEquality_WorksOnPemContent()
    {
        var a = new SslOptions { CertificatePem = DummyCertPem, PrivateKeyPem = DummyKeyPem };
        var b = new SslOptions { CertificatePem = DummyCertPem, PrivateKeyPem = DummyKeyPem };
        Assert.Equal(a, b);
    }

    [Fact]
    public void SslOptions_DefaultsAreNullExceptRequired()
    {
        var ssl = new SslOptions
        {
            CertificatePem = DummyCertPem,
            PrivateKeyPem = DummyKeyPem,
        };
        Assert.Null(ssl.Passphrase);
        Assert.Null(ssl.CaCertificatePem);
        Assert.False(ssl.AllowSelfSigned);
    }

    [Fact]
    public void SaslOptions_RoundTripsAllFields()
    {
        var sasl = new SaslOptions
        {
            Mechanism = SaslMechanism.Plain,
            Username = "alice",
            Password = "s3cret",
        };
        Assert.Equal(SaslMechanism.Plain, sasl.Mechanism);
        Assert.Equal("alice", sasl.Username);
        Assert.Equal("s3cret", sasl.Password);
    }

    [Fact]
    public async Task NativeProtocol_RejectsSslWithClearError()
    {
        // Surgewave-native TLS isn't implemented yet; the builder must
        // bail loudly rather than silently fall back to a plaintext
        // connection.
        var builder = SurgewaveClient.Create("localhost:19090")
            .UseSurgewaveProtocol()
            .WithSslPem(DummyCertPem, DummyKeyPem);

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => builder.BuildAsync());
        Assert.Contains("Surgewave-native", ex.Message, StringComparison.Ordinal);
        Assert.Contains("UseKafkaProtocol", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NativeProtocol_RejectsSaslWithClearError()
    {
        var builder = SurgewaveClient.Create("localhost:19090")
            .UseSurgewaveProtocol()
            .WithSasl(SaslMechanism.Plain, "alice", "secret");

        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => builder.BuildAsync());
        Assert.Contains("SASL", ex.Message, StringComparison.Ordinal);
        Assert.Contains("UseKafkaProtocol", ex.Message, StringComparison.Ordinal);
    }
}
