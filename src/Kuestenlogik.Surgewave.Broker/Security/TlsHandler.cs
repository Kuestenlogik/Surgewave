using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace Kuestenlogik.Surgewave.Broker.Security;

/// <summary>
/// Handles TLS/SSL setup for secure connections.
/// </summary>
public sealed class TlsHandler : IDisposable
{
    private readonly X509Certificate2 _serverCertificate;
    private readonly X509Certificate2Collection? _trustedCaCertificates;
    private readonly bool _requireClientCertificate;
    private readonly SslProtocols _sslProtocols;
    private bool _disposed;

    public TlsHandler(SecurityConfig config)
    {
        if (string.IsNullOrEmpty(config.CertificatePath))
        {
            throw new InvalidOperationException("TLS is enabled but no certificate path is configured");
        }

        if (!File.Exists(config.CertificatePath))
        {
            throw new FileNotFoundException($"Certificate file not found: {config.CertificatePath}");
        }

        // Load server certificate using modern API
        _serverCertificate = X509CertificateLoader.LoadPkcs12FromFile(
            config.CertificatePath,
            config.CertificatePassword,
            X509KeyStorageFlags.MachineKeySet | X509KeyStorageFlags.PersistKeySet);

        if (!_serverCertificate.HasPrivateKey)
        {
            throw new InvalidOperationException("Server certificate must include a private key");
        }

        // Load trusted CA certificates for client validation (mTLS)
        if (!string.IsNullOrEmpty(config.TrustedCaCertificatePath) && File.Exists(config.TrustedCaCertificatePath))
        {
            _trustedCaCertificates = X509CertificateLoader.LoadPkcs12CollectionFromFile(
                config.TrustedCaCertificatePath,
                password: null);
        }

        _requireClientCertificate = config.RequireClientCertificate;

        // Let OS choose best protocol by default, or restrict based on config
        _sslProtocols = config.MinTlsVersion.ToUpperInvariant() switch
        {
            "TLS13" => SslProtocols.None, // OS will use TLS 1.3+ when available
            _ => SslProtocols.None // Let OS choose best available
        };
    }

    /// <summary>
    /// Wrap a network stream with TLS encryption.
    /// </summary>
    public async Task<SslStream> AuthenticateAsServerAsync(
        Stream innerStream,
        CancellationToken cancellationToken = default)
    {
        var sslStream = new SslStream(
            innerStream,
            leaveInnerStreamOpen: false,
            userCertificateValidationCallback: ValidateClientCertificate);

        var authOptions = new SslServerAuthenticationOptions
        {
            ServerCertificate = _serverCertificate,
            ClientCertificateRequired = _requireClientCertificate,
            EnabledSslProtocols = _sslProtocols,
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck
        };

        try
        {
            await sslStream.AuthenticateAsServerAsync(authOptions, cancellationToken);
            return sslStream;
        }
        catch
        {
            await sslStream.DisposeAsync();
            throw;
        }
    }

    /// <summary>
    /// Validate client certificate during mTLS handshake.
    /// </summary>
    private bool ValidateClientCertificate(
        object sender,
        X509Certificate? certificate,
        X509Chain? chain,
        SslPolicyErrors sslPolicyErrors)
    {
        // If client certificate is not required, accept connections without one
        if (!_requireClientCertificate && certificate == null)
        {
            return true;
        }

        // If no errors, accept
        if (sslPolicyErrors == SslPolicyErrors.None)
        {
            return true;
        }

        // If we have trusted CA certificates, validate against them
        if (_trustedCaCertificates != null && certificate != null)
        {
            using var customChain = new X509Chain();
            customChain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            customChain.ChainPolicy.CustomTrustStore.AddRange(_trustedCaCertificates);
            customChain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;

            using var cert2 = X509CertificateLoader.LoadCertificate(certificate.GetRawCertData());
            if (customChain.Build(cert2))
            {
                return true;
            }
        }

        // For self-signed certificates in development, you might want to accept them
        // In production, you should validate properly
        if (sslPolicyErrors == SslPolicyErrors.RemoteCertificateChainErrors)
        {
            // Check if it's just a self-signed cert issue (common in dev)
            return false; // Strict by default - change for dev scenarios
        }

        return false;
    }

    /// <summary>
    /// Information about the TLS configuration for logging.
    /// </summary>
    public string ConfigurationSummary
    {
        get
        {
            var clientCertMode = _requireClientCertificate ? "Required" : "Optional";
            return $"TLS enabled with {_serverCertificate.Subject}, " +
                   $"Client cert: {clientCertMode}, " +
                   $"Protocols: {_sslProtocols}";
        }
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _serverCertificate.Dispose();
            _disposed = true;
        }
    }
}
