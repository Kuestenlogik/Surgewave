using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// File-system facade over the BuiltinEcdsaSigner trust store — the directory
/// of <c>*.pub</c> SubjectPublicKeyInfo PEM files that the verifier enumerates
/// at <c>VerifyAsync</c> time. The service handles validation (PEM parses as
/// ECDSA), name-sanitisation (no path traversal), atomic writes, and SHA-256
/// fingerprint computation so the Broker REST surface can stay thin.
///
/// The trust store directory is created on first use. Operations are
/// synchronous-friendly; the few async methods accept cancellation for the
/// streaming-upload path.
/// </summary>
public sealed partial class TrustStoreService
{
    public const string PublicKeyExtension = ".pub";

    private static readonly Regex KeyNamePattern = MyKeyNameRegex();

    [GeneratedRegex(@"^[A-Za-z0-9._\-]{1,128}$", RegexOptions.CultureInvariant)]
    private static partial Regex MyKeyNameRegex();

    private readonly string _trustedKeysDir;

    public TrustStoreService(string trustedKeysDir)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(trustedKeysDir);
        _trustedKeysDir = trustedKeysDir;
    }

    public string TrustedKeysDir => _trustedKeysDir;

    /// <summary>
    /// Enumerates every <c>*.pub</c> file in the trust store. Malformed files
    /// are reported with a null <see cref="TrustedKeyInfo.Fingerprint"/> so the
    /// UI can flag them — they're silently skipped at verification time today
    /// and otherwise invisible to the operator.
    /// </summary>
    public IReadOnlyList<TrustedKeyInfo> List()
    {
        if (!Directory.Exists(_trustedKeysDir)) return [];
        var entries = new List<TrustedKeyInfo>();
        foreach (var path in Directory.EnumerateFiles(_trustedKeysDir, $"*{PublicKeyExtension}"))
        {
            var fi = new FileInfo(path);
            var name = Path.GetFileNameWithoutExtension(path);
            string? fp = null;
            try
            {
                var pem = File.ReadAllText(path);
                fp = ComputeFingerprint(pem);
            }
            catch { /* leave fp null — UI shows "invalid PEM" */ }
            entries.Add(new TrustedKeyInfo(name, fp, fi.LastWriteTimeUtc, fi.Length));
        }
        return entries.OrderBy(e => e.Name, StringComparer.OrdinalIgnoreCase).ToList();
    }

    /// <summary>
    /// Validates the PEM content as ECDSA P-256 SubjectPublicKeyInfo and
    /// writes it to <c>{keyName}.pub</c>. Throws on invalid name, invalid PEM,
    /// or if a key with that name already exists (force-overwrite would be a
    /// silent revocation/rotation — make it explicit by deleting first).
    /// </summary>
    public async Task<TrustedKeyInfo> UploadAsync(string keyName, Stream pemContent, CancellationToken ct = default)
    {
        ValidateKeyName(keyName);
        ArgumentNullException.ThrowIfNull(pemContent);

        using var reader = new StreamReader(pemContent, Encoding.UTF8, leaveOpen: true);
        var pem = await reader.ReadToEndAsync(ct).ConfigureAwait(false);

        // Validate: must parse as ECDSA SubjectPublicKeyInfo. Throws on garbage.
        using (var probe = ECDsa.Create())
        {
            probe.ImportFromPem(pem);
        }

        Directory.CreateDirectory(_trustedKeysDir);
        var targetPath = Path.Combine(_trustedKeysDir, keyName + PublicKeyExtension);
        if (File.Exists(targetPath))
        {
            throw new InvalidOperationException(
                $"A trusted key named '{keyName}' already exists. Delete it explicitly before re-adding to avoid silent key rotation.");
        }

        await File.WriteAllTextAsync(targetPath, pem, ct).ConfigureAwait(false);
        var fi = new FileInfo(targetPath);
        return new TrustedKeyInfo(keyName, ComputeFingerprint(pem), fi.LastWriteTimeUtc, fi.Length);
    }

    /// <summary>
    /// Removes <c>{keyName}.pub</c>. Returns <c>true</c> if a key was deleted,
    /// <c>false</c> if no such key exists. Idempotent.
    /// </summary>
    public bool Delete(string keyName)
    {
        ValidateKeyName(keyName);
        var path = Path.Combine(_trustedKeysDir, keyName + PublicKeyExtension);
        if (!File.Exists(path)) return false;
        File.Delete(path);
        return true;
    }

    /// <summary>
    /// Generates a new ECDSA P-256 key pair. Writes the public key into the
    /// trust store as <c>{keyName}.pub</c>; the private key PEM is returned in
    /// the result so the caller can stream it back to the operator's browser
    /// for one-time download. The service never persists the private key.
    /// </summary>
    public GeneratedKeyPair Generate(string keyName)
    {
        ValidateKeyName(keyName);
        Directory.CreateDirectory(_trustedKeysDir);
        var targetPath = Path.Combine(_trustedKeysDir, keyName + PublicKeyExtension);
        if (File.Exists(targetPath))
        {
            throw new InvalidOperationException(
                $"A trusted key named '{keyName}' already exists. Delete it first or pick another name.");
        }

        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var publicPem = key.ExportSubjectPublicKeyInfoPem();
        var privatePem = key.ExportECPrivateKeyPem();
        File.WriteAllText(targetPath, publicPem);
        return new GeneratedKeyPair(keyName, privatePem, publicPem, ComputeFingerprint(publicPem));
    }

    private static void ValidateKeyName(string keyName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(keyName);
        if (!KeyNamePattern.IsMatch(keyName))
        {
            throw new ArgumentException(
                "Key name must be 1–128 chars of [A-Za-z0-9._-]. No path separators or extension.",
                nameof(keyName));
        }
    }

    private static string ComputeFingerprint(string pem)
    {
        // Strip PEM armour + whitespace, base64-decode the body, SHA-256 it,
        // format as colon-separated lowercase hex — same shape openssl uses.
        var body = ExtractBase64Body(pem);
        var der = Convert.FromBase64String(body);
        var hash = SHA256.HashData(der);
        return string.Join(':', hash.Select(b => b.ToString("x2", System.Globalization.CultureInfo.InvariantCulture)));
    }

    private static string ExtractBase64Body(string pem)
    {
        var sb = new StringBuilder(pem.Length);
        var inside = false;
        foreach (var line in pem.Split('\n'))
        {
            var trimmed = line.Trim();
            if (trimmed.StartsWith("-----BEGIN", StringComparison.Ordinal)) { inside = true; continue; }
            if (trimmed.StartsWith("-----END", StringComparison.Ordinal)) { inside = false; continue; }
            if (inside) sb.Append(trimmed);
        }
        return sb.ToString();
    }
}

public sealed record TrustedKeyInfo(string Name, string? Fingerprint, DateTimeOffset LastModifiedUtc, long SizeBytes);

public sealed record GeneratedKeyPair(string KeyName, string PrivateKeyPem, string PublicKeyPem, string Fingerprint);
