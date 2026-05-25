using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Kuestenlogik.Surgewave.Transport.Quic;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// Tests for <see cref="QuicPeerTransport.VerifyAgainstCa"/> — the chain
/// validation logic that underpins inter-broker mTLS over QUIC.
/// </summary>
public class QuicMtlsValidationTests
{
    [Fact]
    public void ValidCert_SignedByTrustedCa_ReturnsTrue()
    {
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            using var ca = CreateCaCertificate("CN=test-ca");
            using var broker = CreateSignedCertificate("CN=broker-1", ca);
            Assert.True(QuicPeerTransport.VerifyAgainstCa(broker, ca));
        }
    }

    [Fact]
    public void SelfSignedCert_NotSignedByCa_ReturnsFalse()
    {
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            using var ca = CreateCaCertificate("CN=test-ca");
            using var selfSigned = CreateSelfSignedCertificate("CN=rogue-broker");
            Assert.False(QuicPeerTransport.VerifyAgainstCa(selfSigned, ca));
        }
    }

    [Fact]
    public void CertSignedByDifferentCa_ReturnsFalse()
    {
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            using var trustedCa = CreateCaCertificate("CN=trusted-ca");
            using var foreignCa = CreateCaCertificate("CN=foreign-ca");
            using var foreignBroker = CreateSignedCertificate("CN=foreign-broker", foreignCa);
            Assert.False(QuicPeerTransport.VerifyAgainstCa(foreignBroker, trustedCa));
        }
    }

    [Fact]
    public void ExpiredCert_SignedByTrustedCa_ReturnsFalse()
    {
        if (OperatingSystem.IsWindows() || OperatingSystem.IsLinux() || OperatingSystem.IsMacOS())
        {
            using var ca = CreateCaCertificate("CN=test-ca");
            using var expired = CreateSignedCertificate("CN=expired-broker", ca,
                notBefore: DateTimeOffset.UtcNow.AddYears(-2),
                notAfter: DateTimeOffset.UtcNow.AddYears(-1));
            Assert.False(QuicPeerTransport.VerifyAgainstCa(expired, ca));
        }
    }

    [Fact]
    public void HasMutualTlsConfig_BothPaths_ReturnsTrue()
    {
        if (!OperatingSystem.IsWindows() && !OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS()) return;
        var originalCert = QuicPeerTransport.BrokerCertificatePath;
        var originalCa = QuicPeerTransport.CaCertificatePath;
        try
        {
            QuicPeerTransport.BrokerCertificatePath = "/fake/broker.pfx";
            QuicPeerTransport.CaCertificatePath = "/fake/ca.crt";

            Assert.True(QuicPeerTransport.HasMutualTlsConfig);
        }
        finally
        {
            QuicPeerTransport.BrokerCertificatePath = originalCert;
            QuicPeerTransport.CaCertificatePath = originalCa;
        }
    }

    [Fact]
    public void HasMutualTlsConfig_MissingCa_ReturnsFalse()
    {
        if (!OperatingSystem.IsWindows() && !OperatingSystem.IsLinux() && !OperatingSystem.IsMacOS()) return;
        var originalCert = QuicPeerTransport.BrokerCertificatePath;
        var originalCa = QuicPeerTransport.CaCertificatePath;
        try
        {
            QuicPeerTransport.BrokerCertificatePath = "/fake/broker.pfx";
            QuicPeerTransport.CaCertificatePath = "";

            Assert.False(QuicPeerTransport.HasMutualTlsConfig);
        }
        finally
        {
            QuicPeerTransport.BrokerCertificatePath = originalCert;
            QuicPeerTransport.CaCertificatePath = originalCa;
        }
    }

    private static X509Certificate2 CreateCaCertificate(string subject)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            new X500DistinguishedName(subject),
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(
            certificateAuthority: true, hasPathLengthConstraint: false, pathLengthConstraint: 0, critical: true));
        var cert = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddYears(-5),
            DateTimeOffset.UtcNow.AddYears(5));
        return X509CertificateLoader.LoadPkcs12(cert.Export(X509ContentType.Pkcs12), password: null);
    }

    private static X509Certificate2 CreateSignedCertificate(
        string subject,
        X509Certificate2 issuer,
        DateTimeOffset? notBefore = null,
        DateTimeOffset? notAfter = null)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            new X500DistinguishedName(subject),
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid("1.3.6.1.5.5.7.3.1"), new Oid("1.3.6.1.5.5.7.3.2")], false));

        var serial = new byte[16];
        Random.Shared.NextBytes(serial);

        using var signed = request.Create(
            issuer,
            notBefore ?? DateTimeOffset.UtcNow.AddDays(-1),
            notAfter ?? DateTimeOffset.UtcNow.AddYears(1),
            serial);

        var withKey = signed.CopyWithPrivateKey(rsa);
        return X509CertificateLoader.LoadPkcs12(withKey.Export(X509ContentType.Pkcs12), password: null);
    }

    private static X509Certificate2 CreateSelfSignedCertificate(string subject)
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            new X500DistinguishedName(subject),
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);
        var cert = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(1));
        return X509CertificateLoader.LoadPkcs12(cert.Export(X509ContentType.Pkcs12), password: null);
    }
}
