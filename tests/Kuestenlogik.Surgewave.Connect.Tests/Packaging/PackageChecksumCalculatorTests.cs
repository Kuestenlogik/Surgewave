namespace Kuestenlogik.Surgewave.Connect.Tests.Packaging;

using System.Security.Cryptography;
using System.Text;
using Kuestenlogik.Surgewave.Plugins.Packaging;

public class PackageChecksumCalculatorTests
{
    private string CreateTempFile(string content)
    {
        var path = Path.GetTempFileName();
        File.WriteAllText(path, content);
        return path;
    }

    [Fact]
    public async Task ComputeAsync_ReturnsCorrectHash()
    {
        var path = CreateTempFile("hello world");
        try
        {
            var hash = await PackageChecksumCalculator.ComputeAsync(path);

            // Known SHA256 of "hello world"
            var expected = Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes("hello world")));
            Assert.Equal(expected, hash);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task ComputeAsync_ReturnsLowercaseHex()
    {
        var path = CreateTempFile("test data");
        try
        {
            var hash = await PackageChecksumCalculator.ComputeAsync(path);

            Assert.Equal(hash, hash.ToLowerInvariant());
            Assert.Matches("^[0-9a-f]{64}$", hash);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task VerifyAsync_MatchingHash_ReturnsValid()
    {
        var path = CreateTempFile("verify me");
        try
        {
            var expectedHash = await PackageChecksumCalculator.ComputeAsync(path);
            var result = await PackageChecksumCalculator.VerifyAsync(path, expectedHash);

            Assert.True(result.IsValid);
            Assert.Equal(expectedHash, result.ComputedHash);
            Assert.Equal(expectedHash, result.ExpectedHash);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task VerifyAsync_MismatchingHash_ReturnsInvalid()
    {
        var path = CreateTempFile("some content");
        try
        {
            var result = await PackageChecksumCalculator.VerifyAsync(path, "0000000000000000000000000000000000000000000000000000000000000000");

            Assert.False(result.IsValid);
            Assert.NotEqual(result.ComputedHash, result.ExpectedHash);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task VerifyAsync_CaseInsensitiveComparison()
    {
        var path = CreateTempFile("case test");
        try
        {
            var hash = await PackageChecksumCalculator.ComputeAsync(path);
            var upperHash = hash.ToUpperInvariant();

            var result = await PackageChecksumCalculator.VerifyAsync(path, upperHash);

            Assert.True(result.IsValid);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
