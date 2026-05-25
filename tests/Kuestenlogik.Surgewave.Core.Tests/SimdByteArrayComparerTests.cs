using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Unit tests for SIMD-optimized byte array comparer.
/// Verifies correctness of equality and hash code operations.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class SimdByteArrayComparerTests
{
    private readonly ITestOutputHelper _output;
    private readonly SimdByteArrayComparer _comparer = SimdByteArrayComparer.Instance;

    public SimdByteArrayComparerTests(ITestOutputHelper output)
    {
        _output = output;
        _output.WriteLine($"Implementation: {SimdByteArrayComparer.Implementation}");
        _output.WriteLine($"Hardware Accelerated: {SimdByteArrayComparer.IsHardwareAccelerated}");
    }

    [Fact]
    public void Equals_NullArrays_ReturnsTrue()
    {
        Assert.True(_comparer.Equals(null, null));
    }

    [Fact]
    public void Equals_OneNull_ReturnsFalse()
    {
        var data = new byte[] { 1, 2, 3 };
        Assert.False(_comparer.Equals(null, data));
        Assert.False(_comparer.Equals(data, null));
    }

    [Fact]
    public void Equals_EmptyArrays_ReturnsTrue()
    {
        Assert.True(_comparer.Equals(Array.Empty<byte>(), Array.Empty<byte>()));
    }

    [Fact]
    public void Equals_SameReference_ReturnsTrue()
    {
        var data = new byte[] { 1, 2, 3, 4, 5 };
        Assert.True(_comparer.Equals(data, data));
    }

    [Fact]
    public void Equals_DifferentLengths_ReturnsFalse()
    {
        var a = new byte[] { 1, 2, 3 };
        var b = new byte[] { 1, 2, 3, 4 };
        Assert.False(_comparer.Equals(a, b));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(31)]
    [InlineData(32)]
    [InlineData(33)]
    [InlineData(64)]
    [InlineData(100)]
    [InlineData(256)]
    public void Equals_IdenticalArrays_ReturnsTrue(int size)
    {
        var a = new byte[size];
        var b = new byte[size];
        for (int i = 0; i < size; i++)
        {
            a[i] = (byte)(i % 256);
            b[i] = (byte)(i % 256);
        }

        Assert.True(_comparer.Equals(a, b));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(31)]
    [InlineData(32)]
    [InlineData(33)]
    [InlineData(64)]
    [InlineData(100)]
    [InlineData(256)]
    public void Equals_DifferAtEnd_ReturnsFalse(int size)
    {
        var a = new byte[size];
        var b = new byte[size];
        for (int i = 0; i < size; i++)
        {
            a[i] = (byte)(i % 256);
            b[i] = (byte)(i % 256);
        }
        b[size - 1] ^= 0xFF; // Flip last byte

        Assert.False(_comparer.Equals(a, b));
    }

    [Theory]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(32)]
    [InlineData(64)]
    public void Equals_DifferAtStart_ReturnsFalse(int size)
    {
        var a = new byte[size];
        var b = new byte[size];
        for (int i = 0; i < size; i++)
        {
            a[i] = (byte)(i % 256);
            b[i] = (byte)(i % 256);
        }
        b[0] ^= 0xFF; // Flip first byte

        Assert.False(_comparer.Equals(a, b));
    }

    [Theory]
    [InlineData(16)]
    [InlineData(32)]
    [InlineData(64)]
    public void Equals_DifferInMiddle_ReturnsFalse(int size)
    {
        var a = new byte[size];
        var b = new byte[size];
        for (int i = 0; i < size; i++)
        {
            a[i] = (byte)(i % 256);
            b[i] = (byte)(i % 256);
        }
        b[size / 2] ^= 0xFF; // Flip middle byte

        Assert.False(_comparer.Equals(a, b));
    }

    [Fact]
    public void GetHashCode_NullArray_ReturnsZero()
    {
        Assert.Equal(0, _comparer.GetHashCode(null!));
    }

    [Fact]
    public void GetHashCode_EmptyArray_ReturnsZero()
    {
        Assert.Equal(0, _comparer.GetHashCode(Array.Empty<byte>()));
    }

    [Fact]
    public void GetHashCode_Deterministic()
    {
        var data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        var hash1 = _comparer.GetHashCode(data);
        var hash2 = _comparer.GetHashCode(data);
        Assert.Equal(hash1, hash2);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(15)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(31)]
    [InlineData(32)]
    [InlineData(33)]
    [InlineData(64)]
    [InlineData(100)]
    public void GetHashCode_IdenticalArrays_SameHash(int size)
    {
        var a = new byte[size];
        var b = new byte[size];
        for (int i = 0; i < size; i++)
        {
            a[i] = (byte)(i % 256);
            b[i] = (byte)(i % 256);
        }

        Assert.Equal(_comparer.GetHashCode(a), _comparer.GetHashCode(b));
    }

    [Theory]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(32)]
    [InlineData(64)]
    public void GetHashCode_DifferentArrays_DifferentHash(int size)
    {
        var a = new byte[size];
        var b = new byte[size];
        for (int i = 0; i < size; i++)
        {
            a[i] = (byte)(i % 256);
            b[i] = (byte)((i + 1) % 256);
        }

        // Note: hash collisions are possible, but highly unlikely for different data
        Assert.NotEqual(_comparer.GetHashCode(a), _comparer.GetHashCode(b));
    }

    [Fact]
    public void Dictionary_WorksCorrectly()
    {
        var dict = new Dictionary<byte[], string>(SimdByteArrayComparer.Instance);

        var key1 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        var key2 = new byte[] { 1, 2, 3, 4, 5, 6, 7, 9 }; // Different last byte
        var key1Copy = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }; // Same as key1

        dict[key1] = "value1";
        dict[key2] = "value2";

        Assert.Equal("value1", dict[key1]);
        Assert.Equal("value2", dict[key2]);
        Assert.Equal("value1", dict[key1Copy]); // Should find key1's value
        Assert.Equal(2, dict.Count);
    }

    [Fact]
    public void Dictionary_LargeKeys_WorksCorrectly()
    {
        var dict = new Dictionary<byte[], int>(SimdByteArrayComparer.Instance);
        var random = new Random(42);

        // Add 1000 random 64-byte keys
        var keys = new List<byte[]>();
        for (int i = 0; i < 1000; i++)
        {
            var key = new byte[64];
            random.NextBytes(key);
            keys.Add(key);
            dict[key] = i;
        }

        // Verify all keys can be found
        for (int i = 0; i < keys.Count; i++)
        {
            Assert.Equal(i, dict[keys[i]]);
        }
    }

    [Fact]
    public void Implementation_ReportsCorrectly()
    {
        var impl = SimdByteArrayComparer.Implementation;
        Assert.NotNull(impl);
        Assert.NotEmpty(impl);
        _output.WriteLine($"Active implementation: {impl}");
    }
}
