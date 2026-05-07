using System.Security.Cryptography;

namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Built-in <see cref="ISppSigner"/> using ECDSA P-256. Signs the package's SHA256 hash,
/// producing a detached <c>.sig</c> file with a base64-encoded signature. Trust is modelled
/// as a flat directory of <c>.pub</c> files (SubjectPublicKeyInfo PEM).
/// </summary>
/// <remarks>
/// This is the zero-dependency default — no PKI, no cert chains, no timestamping. For
/// enterprise scenarios (X.509, HSM, revocation, RFC-3161) use a Charter-backed signer
/// instead.
/// </remarks>
public sealed class BuiltinEcdsaSigner : ISppSigner
{
    internal const string SignatureExtension = ".sig";
    internal const string PublicKeyExtension = ".pub";

    private readonly string? _privateKeyPath;
    private readonly string? _trustedKeysDir;

    public string Name => "builtin-ecdsa";

    /// <summary>
    /// Creates a signer. At least one of <paramref name="privateKeyPath"/> (for signing)
    /// or <paramref name="trustedKeysDir"/> (for verification) must be supplied.
    /// </summary>
    public BuiltinEcdsaSigner(string? privateKeyPath = null, string? trustedKeysDir = null)
    {
        if (string.IsNullOrEmpty(privateKeyPath) && string.IsNullOrEmpty(trustedKeysDir))
            throw new ArgumentException(
                "BuiltinEcdsaSigner needs either a privateKeyPath (for signing) or a trustedKeysDir (for verification).");

        _privateKeyPath = privateKeyPath;
        _trustedKeysDir = trustedKeysDir;
    }

    /// <summary>
    /// Generates a new ECDSA P-256 key pair. Private key in EC PRIVATE KEY PEM form,
    /// public key in SubjectPublicKeyInfo PEM form.
    /// </summary>
    public static (string privateKeyPath, string publicKeyPath) GenerateKeyPair(string outputDir, string name)
    {
        Directory.CreateDirectory(outputDir);
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);

        var privateKeyPath = Path.Combine(outputDir, $"{name}.key");
        var publicKeyPath = Path.Combine(outputDir, $"{name}{PublicKeyExtension}");

        File.WriteAllText(privateKeyPath, key.ExportECPrivateKeyPem());
        File.WriteAllText(publicKeyPath, key.ExportSubjectPublicKeyInfoPem());

        return (privateKeyPath, publicKeyPath);
    }

    public bool HasSignature(string packagePath)
        => File.Exists(packagePath + SignatureExtension);

    public async Task<string> SignAsync(string packagePath, CancellationToken ct = default)
    {
        if (string.IsNullOrEmpty(_privateKeyPath))
            throw new InvalidOperationException("BuiltinEcdsaSigner was constructed without a private key path — signing not available.");
        if (!File.Exists(_privateKeyPath))
            throw new FileNotFoundException($"Private key not found: {_privateKeyPath}", _privateKeyPath);

        var hash = await ComputeSha256Async(packagePath, ct);
        var pem = await File.ReadAllTextAsync(_privateKeyPath, ct);

        using var key = ECDsa.Create();
        key.ImportFromPem(pem);

        var signature = key.SignHash(hash);
        var sigPath = packagePath + SignatureExtension;
        await File.WriteAllTextAsync(sigPath, Convert.ToBase64String(signature), ct);
        return sigPath;
    }

    public async Task<SignatureVerification> VerifyAsync(string packagePath, CancellationToken ct = default)
    {
        if (!HasSignature(packagePath))
            return SignatureVerification.Unsigned;

        if (string.IsNullOrEmpty(_trustedKeysDir))
            throw new InvalidOperationException("BuiltinEcdsaSigner was constructed without a trusted-keys directory — verification not available.");
        if (!Directory.Exists(_trustedKeysDir))
            return SignatureVerification.Invalid($"Trusted keys directory not found: {_trustedKeysDir}");

        var hash = await ComputeSha256Async(packagePath, ct);
        var signatureBase64 = (await File.ReadAllTextAsync(packagePath + SignatureExtension, ct)).Trim();
        byte[] signature;
        try
        {
            signature = Convert.FromBase64String(signatureBase64);
        }
        catch (FormatException ex)
        {
            return SignatureVerification.Invalid($"Signature file is not valid base64: {ex.Message}");
        }

        foreach (var keyFile in Directory.GetFiles(_trustedKeysDir, $"*{PublicKeyExtension}"))
        {
            try
            {
                var pem = await File.ReadAllTextAsync(keyFile, ct);
                using var key = ECDsa.Create();
                key.ImportFromPem(pem);

                if (key.VerifyHash(hash, signature))
                    return SignatureVerification.Valid(Path.GetFileNameWithoutExtension(keyFile));
            }
            catch
            {
                // Skip malformed key files; they shouldn't block other trusted keys.
            }
        }

        return SignatureVerification.Invalid("No trusted key matches the signature");
    }

    private static async Task<byte[]> ComputeSha256Async(string filePath, CancellationToken ct)
    {
        await using var stream = File.OpenRead(filePath);
        return await SHA256.HashDataAsync(stream, ct);
    }
}
