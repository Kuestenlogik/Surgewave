namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Serialization;

using System.Text;

public class DeserializersTests
{
    [Fact]
    public void Utf8_DeserializesString()
    {
        var bytes = Encoding.UTF8.GetBytes("Hello, World!");
        var result = Deserializers.Utf8.Deserialize(bytes, false, default);
        Assert.Equal("Hello, World!", result);
    }

    [Fact]
    public void Utf8_NullReturnsEmpty()
    {
        var result = Deserializers.Utf8.Deserialize(ReadOnlySpan<byte>.Empty, true, default);
        Assert.Equal(string.Empty, result);
    }

    [Fact]
    public void ByteArray_ReturnsArray()
    {
        var input = new byte[] { 1, 2, 3, 4, 5 };
        var result = Deserializers.ByteArray.Deserialize(input, false, default);
        Assert.Equal(input, result);
    }

    [Fact]
    public void Int32_DeserializesCorrectly()
    {
        // Big-endian format for 12345
        var value = 12345;
        var bytes = BitConverter.GetBytes(value).Reverse().ToArray();
        var result = Deserializers.Int32.Deserialize(bytes, false, default);
        Assert.Equal(12345, result);
    }

    [Fact]
    public void Int64_DeserializesCorrectly()
    {
        var value = 1234567890123L;
        var bytes = BitConverter.GetBytes(value).Reverse().ToArray();
        var result = Deserializers.Int64.Deserialize(bytes, false, default);
        Assert.Equal(1234567890123L, result);
    }

    [Fact]
    public void Single_DeserializesCorrectly()
    {
        var value = 3.14f;
        var bytes = BitConverter.GetBytes(value).Reverse().ToArray();
        var result = Deserializers.Single.Deserialize(bytes, false, default);
        Assert.Equal(3.14f, result);
    }

    [Fact]
    public void Double_DeserializesCorrectly()
    {
        var value = 3.14159265359;
        var bytes = BitConverter.GetBytes(value).Reverse().ToArray();
        var result = Deserializers.Double.Deserialize(bytes, false, default);
        Assert.Equal(3.14159265359, result);
    }

    [Fact]
    public void Null_ReturnsNullInstance()
    {
        var result = Deserializers.Null.Deserialize(ReadOnlySpan<byte>.Empty, true, default);
        Assert.Same(Null.Instance, result);
    }

    [Fact]
    public void Ignore_ReturnsIgnore()
    {
        var result = Deserializers.Ignore.Deserialize(new byte[] { 1, 2, 3 }, false, default);
        Assert.IsType<Ignore>(result);
    }
}
