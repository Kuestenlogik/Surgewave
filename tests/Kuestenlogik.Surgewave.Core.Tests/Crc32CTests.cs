using System.Text;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Unit tests for CRC32-C (Castagnoli) implementation.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class Crc32CTests
{
    private readonly ITestOutputHelper _output;

    public Crc32CTests(ITestOutputHelper output)
    {
        _output = output;
        _output.WriteLine($"CRC32C Implementation: {Crc32C.Implementation}");
        _output.WriteLine($"Hardware Accelerated: {Crc32C.IsHardwareAccelerated}");
    }

    [Fact]
    public void Compute_EmptyArray_ReturnsZero()
    {
        var result = Crc32C.Compute(Array.Empty<byte>());
        Assert.Equal(0x00000000u, result);
    }

    [Fact]
    public void Compute_SingleZeroByte_ReturnsExpected()
    {
        var result = Crc32C.Compute(new byte[] { 0x00 });
        Assert.Equal(0x527d5351u, result);
    }

    [Fact]
    public void Compute_123456789String_ReturnsKnownValue()
    {
        var data = Encoding.ASCII.GetBytes("123456789");
        var result = Crc32C.Compute(data);
        Assert.Equal(0xe3069283u, result);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(100)]
    [InlineData(1000)]
    public void Compute_VariousSizes_ProducesDeterministicResults(int size)
    {
        var data = new byte[size];
        for (int i = 0; i < size; i++)
            data[i] = (byte)(i % 256);

        var result1 = Crc32C.Compute(data);
        var result2 = Crc32C.Compute(data);

        Assert.Equal(result1, result2);
        Assert.NotEqual(0u, result1);
    }

    [Fact]
    public void Compute_SpanAndReadOnlySpan_ProduceSameResult()
    {
        var data = Encoding.ASCII.GetBytes("Test data for span comparison");

        var spanResult = Crc32C.Compute(data.AsSpan());
        var readOnlySpanResult = Crc32C.Compute((ReadOnlySpan<byte>)data);

        Assert.Equal(spanResult, readOnlySpanResult);
    }

    [Fact]
    public void IsHardwareAccelerated_ReportsCorrectly()
    {
        _output.WriteLine($"Hardware accelerated: {Crc32C.IsHardwareAccelerated}");
        _output.WriteLine($"Implementation: {Crc32C.Implementation}");

        Assert.NotNull(Crc32C.Implementation);
        Assert.NotEmpty(Crc32C.Implementation);
    }

    [Fact]
    public void Compute_AlignedAndUnalignedData_ProduceCorrectResults()
    {
        var fullBuffer = new byte[100];
        for (int i = 0; i < fullBuffer.Length; i++)
            fullBuffer[i] = (byte)(i * 37 + 17);

        var result1 = Crc32C.Compute(fullBuffer.AsSpan(0, 64));
        var result2 = Crc32C.Compute(fullBuffer.AsSpan(1, 64));
        var result3 = Crc32C.Compute(fullBuffer.AsSpan(3, 64));

        Assert.NotEqual(0u, result1);
        Assert.NotEqual(0u, result2);
        Assert.NotEqual(0u, result3);
        Assert.NotEqual(result1, result2);
        Assert.NotEqual(result2, result3);
    }
}
