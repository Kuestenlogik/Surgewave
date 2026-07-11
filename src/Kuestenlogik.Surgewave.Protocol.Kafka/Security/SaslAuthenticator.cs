using System.Security.Cryptography;
using System.Text;
using Kuestenlogik.Surgewave.Broker.Security.OAuthBearer;

namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// Handles SASL authentication for Kafka connections.
/// Supports PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and OAUTHBEARER mechanisms.
/// </summary>
public sealed class SaslAuthenticator
{
    private readonly CredentialStore _credentialStore;
    private readonly HashSet<string> _enabledMechanisms;
    private readonly ScramAuthenticator? _scramSha256;
    private readonly ScramAuthenticator? _scramSha512;
    private readonly OAuthBearerAuthenticator? _oauthBearer;

    public const string MechanismPlain = "PLAIN";
    public const string MechanismScramSha256 = "SCRAM-SHA-256";
    public const string MechanismScramSha512 = "SCRAM-SHA-512";
    public const string MechanismOAuthBearer = "OAUTHBEARER";

    public SaslAuthenticator(
        CredentialStore credentialStore,
        IEnumerable<string>? enabledMechanisms = null,
        ScramCredentialStore? scramSha256Store = null,
        ScramCredentialStore? scramSha512Store = null,
        OAuthBearerAuthenticator? oauthBearer = null)
    {
        _credentialStore = credentialStore;
        _enabledMechanisms = enabledMechanisms?.ToHashSet(StringComparer.OrdinalIgnoreCase)
            ?? new HashSet<string>(StringComparer.OrdinalIgnoreCase) { MechanismPlain };

        // Initialize SCRAM authenticators if stores are provided and mechanism is enabled
        if (scramSha256Store != null && _enabledMechanisms.Contains(MechanismScramSha256))
        {
            _scramSha256 = new ScramAuthenticator(scramSha256Store, MechanismScramSha256);
        }

        if (scramSha512Store != null && _enabledMechanisms.Contains(MechanismScramSha512))
        {
            _scramSha512 = new ScramAuthenticator(scramSha512Store, MechanismScramSha512);
        }

        // OAUTHBEARER (KIP-936) — only enabled when both the mechanism is in the
        // allow-list and a validator was wired up. The validator pulls JWKS from
        // an OIDC discovery doc or a direct JWKS URL.
        if (oauthBearer != null && _enabledMechanisms.Contains(MechanismOAuthBearer))
        {
            _oauthBearer = oauthBearer;
        }
    }

    /// <summary>
    /// Get the list of supported SASL mechanisms
    /// </summary>
    public string[] EnabledMechanisms => [.. _enabledMechanisms];

    /// <summary>
    /// Check if a mechanism is supported
    /// </summary>
    public bool IsMechanismSupported(string mechanism)
    {
        return _enabledMechanisms.Contains(mechanism);
    }

    /// <summary>
    /// Check if a mechanism requires multi-step authentication
    /// </summary>
    public bool IsMultiStepMechanism(string mechanism)
    {
        return mechanism.ToUpperInvariant().StartsWith("SCRAM-", StringComparison.Ordinal);
    }

    /// <summary>
    /// Authenticate using the specified mechanism and auth bytes (single-step)
    /// </summary>
    /// <returns>Authentication result with username if successful</returns>
    public SaslAuthenticationResult Authenticate(string mechanism, byte[] authBytes)
    {
        if (!IsMechanismSupported(mechanism))
        {
            return SaslAuthenticationResult.Failed("Unsupported SASL mechanism");
        }

        return mechanism.ToUpperInvariant() switch
        {
            MechanismPlain => AuthenticatePlain(authBytes),
            MechanismOAuthBearer => AuthenticateOAuthBearerSync(authBytes),
            _ => SaslAuthenticationResult.Failed($"Mechanism {mechanism} requires multi-step authentication")
        };
    }

    /// <summary>
    /// OAUTHBEARER authentication (KIP-936). The validator runs async (it may need
    /// to refresh JWKS over HTTP); we expose a synchronous facade for the existing
    /// dispatch surface and block on the task. The HTTP path is cached, so the
    /// blocking is bounded to a single network round-trip on first use of a
    /// rotated JWKS.
    /// </summary>
    private SaslAuthenticationResult AuthenticateOAuthBearerSync(byte[] authBytes)
    {
        if (_oauthBearer is null)
        {
            return SaslAuthenticationResult.Failed("OAUTHBEARER not configured");
        }

        try
        {
            var (result, _) = _oauthBearer.AuthenticateAsync(authBytes, CancellationToken.None)
                .ConfigureAwait(false).GetAwaiter().GetResult();
            return result;
        }
        catch (Exception ex)
        {
            return SaslAuthenticationResult.Failed($"OAUTHBEARER error: {ex.Message}");
        }
    }

    /// <summary>
    /// Process SCRAM authentication step
    /// </summary>
    public SaslAuthenticationResult AuthenticateScram(string mechanism, byte[] authBytes, ScramSession session)
    {
        if (!IsMechanismSupported(mechanism))
        {
            return SaslAuthenticationResult.Failed("Unsupported SASL mechanism");
        }

        var authenticator = mechanism.ToUpperInvariant() switch
        {
            MechanismScramSha256 => _scramSha256,
            MechanismScramSha512 => _scramSha512,
            _ => null
        };

        if (authenticator == null)
        {
            return SaslAuthenticationResult.Failed($"SCRAM mechanism {mechanism} not configured");
        }

        ScramStepResult result;
        if (session.State == ScramState.Initial)
        {
            result = authenticator.ProcessClientFirst(authBytes, session);
        }
        else if (session.State == ScramState.ServerFirstSent)
        {
            result = authenticator.ProcessClientFinal(authBytes, session);
        }
        else
        {
            return SaslAuthenticationResult.Failed("Invalid SCRAM state");
        }

        if (result.IsComplete)
        {
            if (result.IsSuccess)
            {
                return SaslAuthenticationResult.Success(result.Username!, result.ResponseData);
            }
            return SaslAuthenticationResult.Failed(result.ErrorMessage ?? "Authentication failed");
        }

        // Continue - need more steps
        return SaslAuthenticationResult.Continue(result.ResponseData!);
    }

    /// <summary>
    /// SASL/PLAIN authentication
    /// Format: [authzid] NUL authcid NUL passwd
    /// authzid is authorization identity (usually empty)
    /// authcid is authentication identity (username)
    /// passwd is the password
    /// </summary>
    private SaslAuthenticationResult AuthenticatePlain(byte[] authBytes)
    {
        try
        {
            // Parse PLAIN format: [authzid] NUL username NUL password
            var parts = SplitByNull(authBytes);

            if (parts.Count < 2)
            {
                return SaslAuthenticationResult.Failed("Invalid PLAIN authentication data");
            }

            string username;
            string password;

            if (parts.Count == 2)
            {
                // No authzid: username NUL password
                username = parts[0];
                password = parts[1];
            }
            else
            {
                // With authzid: authzid NUL username NUL password
                // authzid is typically empty or same as username
                username = parts[1];
                password = parts[2];
            }

            if (string.IsNullOrEmpty(username))
            {
                return SaslAuthenticationResult.Failed("Username cannot be empty");
            }

            if (!_credentialStore.ValidateCredentials(username, password))
            {
                return SaslAuthenticationResult.Failed("Authentication failed");
            }

            return SaslAuthenticationResult.Success(username);
        }
        catch (Exception ex)
        {
            return SaslAuthenticationResult.Failed($"Authentication error: {ex.Message}");
        }
    }

    /// <summary>
    /// Split byte array by NULL bytes into UTF-8 strings
    /// </summary>
    private static List<string> SplitByNull(byte[] data)
    {
        var parts = new List<string>();
        var start = 0;

        for (int i = 0; i < data.Length; i++)
        {
            if (data[i] == 0)
            {
                if (i > start)
                {
                    parts.Add(Encoding.UTF8.GetString(data, start, i - start));
                }
                else
                {
                    parts.Add(string.Empty);
                }
                start = i + 1;
            }
        }

        // Add the last part (password)
        if (start < data.Length)
        {
            parts.Add(Encoding.UTF8.GetString(data, start, data.Length - start));
        }

        return parts;
    }
}

/// <summary>
/// Result of SASL authentication attempt
/// </summary>
public sealed class SaslAuthenticationResult
{
    public bool IsSuccess { get; private init; }
    public bool IsComplete { get; private init; }
    public string? Username { get; private init; }
    public string? ErrorMessage { get; private init; }
    public byte[]? ResponseData { get; private init; }

    /// <summary>
    /// Authentication succeeded
    /// </summary>
    public static SaslAuthenticationResult Success(string username, byte[]? responseData = null) => new()
    {
        IsSuccess = true,
        IsComplete = true,
        Username = username,
        ResponseData = responseData
    };

    /// <summary>
    /// Authentication failed
    /// </summary>
    public static SaslAuthenticationResult Failed(string errorMessage) => new()
    {
        IsSuccess = false,
        IsComplete = true,
        ErrorMessage = errorMessage
    };

    /// <summary>
    /// Authentication requires more steps (SCRAM challenge-response)
    /// </summary>
    public static SaslAuthenticationResult Continue(byte[] responseData) => new()
    {
        IsSuccess = false,
        IsComplete = false,
        ResponseData = responseData
    };
}
