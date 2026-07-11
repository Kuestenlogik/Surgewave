using System.Security.Cryptography;
using System.Text;
namespace Kuestenlogik.Surgewave.Protocol.Kafka;

/// <summary>
/// SCRAM (Salted Challenge Response Authentication Mechanism) authenticator
/// supporting SCRAM-SHA-256 and SCRAM-SHA-512.
///
/// RFC 5802: Salted Challenge Response Authentication Mechanism
/// RFC 7677: SCRAM-SHA-256 and SCRAM-SHA-256-PLUS
/// </summary>
public sealed class ScramAuthenticator
{
    private readonly ScramCredentialStore _credentialStore;
    private readonly HashAlgorithmName _hashAlgorithm;
    private readonly string _mechanism;

    public ScramAuthenticator(ScramCredentialStore credentialStore, string mechanism)
    {
        _credentialStore = credentialStore;
        _mechanism = mechanism.ToUpperInvariant();

        _hashAlgorithm = _mechanism switch
        {
            "SCRAM-SHA-256" => HashAlgorithmName.SHA256,
            "SCRAM-SHA-512" => HashAlgorithmName.SHA512,
            _ => throw new ArgumentException($"Unsupported SCRAM mechanism: {mechanism}")
        };
    }

    /// <summary>
    /// Process client-first-message and generate server-first-message.
    /// Client sends: n,,n=user,r=clientNonce
    /// Server returns: r=clientNonce+serverNonce,s=salt,i=iterations
    /// </summary>
    public ScramStepResult ProcessClientFirst(byte[] clientFirstMessage, ScramSession session)
    {
        try
        {
            var message = Encoding.UTF8.GetString(clientFirstMessage);

            // Parse: gs2-header,client-first-message-bare
            // gs2-header: n,, (no channel binding)
            // client-first-message-bare: n=username,r=client-nonce[,extensions]
            var parts = message.Split(',');

            if (parts.Length < 4)
            {
                return ScramStepResult.Failed("Invalid client-first-message format");
            }

            // parts[0] = "n" (no channel binding)
            // parts[1] = "" (no authzid)
            // parts[2] = "n=username"
            // parts[3] = "r=client-nonce"

            var gs2Header = $"{parts[0]},{parts[1]},";
            var clientFirstBare = string.Join(",", parts.Skip(2));

            // Parse username
            if (!parts[2].StartsWith("n=", StringComparison.Ordinal))
            {
                return ScramStepResult.Failed("Missing username in client-first-message");
            }
            var username = SaslPrepUsername(parts[2][2..]);

            // Parse client nonce
            if (!parts[3].StartsWith("r=", StringComparison.Ordinal))
            {
                return ScramStepResult.Failed("Missing nonce in client-first-message");
            }
            var clientNonce = parts[3][2..];

            // Get stored credentials
            if (!_credentialStore.TryGetCredential(username, out var credential))
            {
                // Return a fake response to prevent username enumeration
                credential = _credentialStore.FakeCredential;
                session.IsFakeUser = true;
            }

            // Generate server nonce
            var serverNonce = GenerateNonce();
            var combinedNonce = clientNonce + serverNonce;

            // Build server-first-message
            var serverFirst = $"r={combinedNonce},s={Convert.ToBase64String(credential.Salt)},i={credential.Iterations}";

            // Store session state
            session.Username = username;
            session.ClientFirstBare = clientFirstBare;
            session.ServerFirst = serverFirst;
            session.CombinedNonce = combinedNonce;
            session.StoredKey = credential.StoredKey;
            session.ServerKey = credential.ServerKey;
            session.State = ScramState.ServerFirstSent;

            return ScramStepResult.Continue(Encoding.UTF8.GetBytes(serverFirst));
        }
        catch (Exception ex)
        {
            return ScramStepResult.Failed($"Error processing client-first-message: {ex.Message}");
        }
    }

    /// <summary>
    /// Process client-final-message and generate server-final-message.
    /// Client sends: c=biws,r=combinedNonce,p=clientProof
    /// Server returns: v=serverSignature (if successful)
    /// </summary>
    public ScramStepResult ProcessClientFinal(byte[] clientFinalMessage, ScramSession session)
    {
        try
        {
            if (session.State != ScramState.ServerFirstSent)
            {
                return ScramStepResult.Failed("Invalid SCRAM state");
            }

            var message = Encoding.UTF8.GetString(clientFinalMessage);

            // Parse: c=channelBinding,r=nonce,p=proof
            var parts = message.Split(',');
            if (parts.Length < 3)
            {
                return ScramStepResult.Failed("Invalid client-final-message format");
            }

            // Verify channel binding (c=biws for no binding)
            if (!parts[0].StartsWith("c=", StringComparison.Ordinal))
            {
                return ScramStepResult.Failed("Missing channel binding");
            }
            var channelBinding = parts[0][2..];
            if (channelBinding != "biws") // Base64("n,,")
            {
                return ScramStepResult.Failed("Unsupported channel binding");
            }

            // Verify nonce
            if (!parts[1].StartsWith("r=", StringComparison.Ordinal))
            {
                return ScramStepResult.Failed("Missing nonce in client-final-message");
            }
            var nonce = parts[1][2..];
            if (nonce != session.CombinedNonce)
            {
                return ScramStepResult.Failed("Nonce mismatch");
            }

            // Get client proof
            var proofPart = parts.FirstOrDefault(p => p.StartsWith("p=", StringComparison.Ordinal));
            if (proofPart == null)
            {
                return ScramStepResult.Failed("Missing proof in client-final-message");
            }
            var clientProof = Convert.FromBase64String(proofPart[2..]);

            // Build client-final-message-without-proof
            var clientFinalWithoutProof = string.Join(",", parts.Where(p => !p.StartsWith("p=", StringComparison.Ordinal)));

            // Compute auth message
            var authMessage = $"{session.ClientFirstBare},{session.ServerFirst},{clientFinalWithoutProof}";

            // Verify client proof
            var clientSignature = ComputeHmac(session.StoredKey, Encoding.UTF8.GetBytes(authMessage));
            var computedClientKey = XorBytes(clientProof, clientSignature);
            var computedStoredKey = ComputeHash(computedClientKey);

            if (session.IsFakeUser || !CryptographicOperations.FixedTimeEquals(computedStoredKey, session.StoredKey))
            {
                return ScramStepResult.Failed("Authentication failed");
            }

            // Compute server signature for verification
            var serverSignature = ComputeHmac(session.ServerKey, Encoding.UTF8.GetBytes(authMessage));
            var serverFinal = $"v={Convert.ToBase64String(serverSignature)}";

            session.State = ScramState.Completed;

            return ScramStepResult.Success(Encoding.UTF8.GetBytes(serverFinal), session.Username!);
        }
        catch (Exception ex)
        {
            return ScramStepResult.Failed($"Error processing client-final-message: {ex.Message}");
        }
    }

    private byte[] ComputeHmac(byte[] key, byte[] data)
    {
        return _hashAlgorithm.Name switch
        {
            "SHA256" => HMACSHA256.HashData(key, data),
            "SHA512" => HMACSHA512.HashData(key, data),
            _ => throw new InvalidOperationException($"Unsupported hash algorithm: {_hashAlgorithm.Name}")
        };
    }

    private byte[] ComputeHash(byte[] data)
    {
        return _hashAlgorithm.Name switch
        {
            "SHA256" => SHA256.HashData(data),
            "SHA512" => SHA512.HashData(data),
            _ => throw new InvalidOperationException($"Unsupported hash algorithm: {_hashAlgorithm.Name}")
        };
    }

    private static byte[] XorBytes(byte[] a, byte[] b)
    {
        if (a.Length != b.Length)
        {
            throw new ArgumentException("Arrays must be same length");
        }

        var result = new byte[a.Length];
        for (int i = 0; i < a.Length; i++)
        {
            result[i] = (byte)(a[i] ^ b[i]);
        }
        return result;
    }

    private static string GenerateNonce()
    {
        var bytes = new byte[24];
        RandomNumberGenerator.Fill(bytes);
        return Convert.ToBase64String(bytes);
    }

    private static string SaslPrepUsername(string username)
    {
        // Basic SASLprep - in production should implement RFC 4013
        return username.Normalize(NormalizationForm.FormKC);
    }
}

/// <summary>
/// Result of a SCRAM authentication step
/// </summary>
public sealed class ScramStepResult
{
    public bool IsComplete { get; private init; }
    public bool IsSuccess { get; private init; }
    public byte[]? ResponseData { get; private init; }
    public string? Username { get; private init; }
    public string? ErrorMessage { get; private init; }

    public static ScramStepResult Continue(byte[] responseData) => new()
    {
        IsComplete = false,
        IsSuccess = false,
        ResponseData = responseData
    };

    public static ScramStepResult Success(byte[] responseData, string username) => new()
    {
        IsComplete = true,
        IsSuccess = true,
        ResponseData = responseData,
        Username = username
    };

    public static ScramStepResult Failed(string errorMessage) => new()
    {
        IsComplete = true,
        IsSuccess = false,
        ErrorMessage = errorMessage
    };
}
