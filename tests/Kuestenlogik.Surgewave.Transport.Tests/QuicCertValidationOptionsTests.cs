using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Kuestenlogik.Surgewave.Transport;
using Kuestenlogik.Surgewave.Transport.Quic;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

public class QuicCertValidationOptionsTests
{
    [Fact]
    public void TransportOptions_TrustAllCertificates_OverridesStatic()
    {
        var options = new TransportOptions
        {
            Host = "localhost",
            Port = 9094,
            TrustAllCertificates = true
        };

        Assert.True(options.TrustAllCertificates);
    }

    [Fact]
    public void TransportOptions_CustomValidationCallback_IsSet()
    {
        RemoteCertificateValidationCallback callback = (_, _, _, errors) =>
            errors == SslPolicyErrors.None;

        var options = new TransportOptions
        {
            Host = "localhost",
            Port = 9094,
            CertificateValidation = callback
        };

        Assert.Same(callback, options.CertificateValidation);
    }

    [Fact]
    public void TransportOptions_ClientCertificate_IsSet()
    {
        if (!OperatingSystem.IsWindows() && !OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS()) return;

        using var cert = CreateSelfSigned("CN=test-client");
        var options = new TransportOptions
        {
            Host = "localhost",
            Port = 9094,
            ClientCertificate = cert
        };

        Assert.Same(cert, options.ClientCertificate);
    }

    [Fact]
    public void QuicPeerTransportOptions_InstanceMtls_Config()
    {
        var options = new QuicPeerTransportOptions
        {
            BrokerCertificatePath = "/path/broker.pfx",
            BrokerCertificatePassword = "secret",
            CaCertificatePath = "/path/ca.crt",
            TrustAllCertificates = false
        };

        Assert.Equal("/path/broker.pfx", options.BrokerCertificatePath);
        Assert.Equal("secret", options.BrokerCertificatePassword);
        Assert.Equal("/path/ca.crt", options.CaCertificatePath);
        Assert.False(options.TrustAllCertificates);
    }

    [Fact]
    public void QuicPeerTransportOptions_CustomCallback_IsSet()
    {
        RemoteCertificateValidationCallback callback = (_, _, _, errors) =>
            errors == SslPolicyErrors.None;

        var options = new QuicPeerTransportOptions
        {
            CertificateValidation = callback
        };

        Assert.Same(callback, options.CertificateValidation);
    }

    [Fact]
    public void QuicPeerTransport_DefaultConstructor_StillWorks()
    {
        if (!OperatingSystem.IsWindows() && !OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS()) return;

        var transport = new QuicPeerTransport();
        Assert.Equal("quic", transport.Name);
    }

    [Fact]
    public void QuicPeerTransport_OptionsConstructor_StillWorks()
    {
        if (!OperatingSystem.IsWindows() && !OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS()) return;

        var options = new QuicPeerTransportOptions
        {
            TrustAllCertificates = true
        };
        var transport = new QuicPeerTransport(options);
        Assert.Equal("quic", transport.Name);
    }

    [Fact]
    public void TransportOptions_Defaults_AreNull()
    {
        var options = new TransportOptions
        {
            Host = "localhost",
            Port = 9094
        };

        Assert.Null(options.CertificateValidation);
        Assert.Null(options.ClientCertificate);
        Assert.Null(options.TrustAllCertificates);
    }

    private static X509Certificate2 CreateSelfSigned(string subject)
    {
        using var rsa = System.Security.Cryptography.RSA.Create(2048);
        var request = new CertificateRequest(
            new X500DistinguishedName(subject),
            rsa,
            System.Security.Cryptography.HashAlgorithmName.SHA256,
            System.Security.Cryptography.RSASignaturePadding.Pkcs1);
        var cert = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(1));
        return X509CertificateLoader.LoadPkcs12(cert.Export(X509ContentType.Pkcs12), password: null);
    }
}
