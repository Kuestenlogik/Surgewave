using System.Security.Cryptography;

namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Computes and verifies SHA256 checksums for plugin packages.
/// </summary>
public static class PackageChecksumCalculator
{
    /// <summary>
    /// Computes the SHA256 hash of a file.
    /// </summary>
    public static async Task<string> ComputeAsync(string filePath, CancellationToken cancellationToken = default)
    {
        await using var stream = File.OpenRead(filePath);
        var hash = await SHA256.HashDataAsync(stream, cancellationToken);
        return Convert.ToHexStringLower(hash);
    }

    /// <summary>
    /// Verifies a file's SHA256 hash against an expected value.
    /// </summary>
    public static async Task<ChecksumVerificationResult> VerifyAsync(
        string filePath, string expectedHash, CancellationToken cancellationToken = default)
    {
        var computed = await ComputeAsync(filePath, cancellationToken);
        var isValid = string.Equals(computed, expectedHash, StringComparison.OrdinalIgnoreCase);
        return new ChecksumVerificationResult(isValid, expectedHash, computed);
    }
}

/// <summary>
/// Result of a checksum verification.
/// </summary>
public sealed record ChecksumVerificationResult(bool IsValid, string ExpectedHash, string ComputedHash);
