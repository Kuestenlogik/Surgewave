using System.Security.Cryptography;
using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

public sealed class PackageChecksumCalculatorTests : IDisposable
{
    private readonly string _tempDir;

    public PackageChecksumCalculatorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sw-checksum-tests-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    [Fact]
    public async Task Compute_ReturnsLowerHexSha256()
    {
        var file = WriteFile("hello.txt", "hello world");
        var expected = Convert.ToHexStringLower(SHA256.HashData("hello world"u8));

        var hash = await PackageChecksumCalculator.ComputeAsync(file);

        Assert.Equal(expected, hash);
        Assert.Equal(64, hash.Length);
        Assert.Matches("^[0-9a-f]+$", hash);
    }

    [Fact]
    public async Task Compute_EmptyFile_ReturnsSha256OfEmpty()
    {
        var file = WriteFile("empty.bin", "");

        var hash = await PackageChecksumCalculator.ComputeAsync(file);

        // Well-known SHA256 of zero bytes
        Assert.Equal("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", hash);
    }

    [Fact]
    public async Task Verify_MatchingHash_IsValid()
    {
        var file = WriteFile("a.bin", "payload-A");
        var expected = await PackageChecksumCalculator.ComputeAsync(file);

        var result = await PackageChecksumCalculator.VerifyAsync(file, expected);

        Assert.True(result.IsValid);
        Assert.Equal(expected, result.ExpectedHash);
        Assert.Equal(expected, result.ComputedHash);
    }

    [Fact]
    public async Task Verify_DifferentHash_IsInvalid()
    {
        var file = WriteFile("a.bin", "payload-A");
        var wrong = new string('0', 64);

        var result = await PackageChecksumCalculator.VerifyAsync(file, wrong);

        Assert.False(result.IsValid);
        Assert.Equal(wrong, result.ExpectedHash);
        Assert.NotEqual(wrong, result.ComputedHash);
    }

    [Fact]
    public async Task Verify_CaseInsensitive_AcceptsUppercaseExpected()
    {
        var file = WriteFile("a.bin", "payload-A");
        var lower = await PackageChecksumCalculator.ComputeAsync(file);

        var result = await PackageChecksumCalculator.VerifyAsync(file, lower.ToUpperInvariant());

        Assert.True(result.IsValid);
    }

    [Fact]
    public async Task Compute_DifferentContent_ProducesDifferentHashes()
    {
        var fileA = WriteFile("a.bin", "content-A");
        var fileB = WriteFile("b.bin", "content-B");

        var hashA = await PackageChecksumCalculator.ComputeAsync(fileA);
        var hashB = await PackageChecksumCalculator.ComputeAsync(fileB);

        Assert.NotEqual(hashA, hashB);
    }

    [Fact]
    public async Task Compute_SameContent_ProducesSameHash()
    {
        var fileA = WriteFile("a.bin", "identical");
        var fileB = WriteFile("b.bin", "identical");

        var hashA = await PackageChecksumCalculator.ComputeAsync(fileA);
        var hashB = await PackageChecksumCalculator.ComputeAsync(fileB);

        Assert.Equal(hashA, hashB);
    }

    private string WriteFile(string name, string content)
    {
        var path = Path.Combine(_tempDir, name);
        File.WriteAllText(path, content);
        return path;
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); }
            catch { /* best effort */ }
        }
    }
}
