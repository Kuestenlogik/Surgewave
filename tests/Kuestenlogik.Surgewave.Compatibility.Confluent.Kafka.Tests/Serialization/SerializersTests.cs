namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Serialization;

using System.Text;

public class SerializersTests
{
    [Fact]
    public void Utf8_SerializesString()
    {
        var bytes = Serializers.Utf8.Serialize("Hello, World!", default);
        Assert.Equal("Hello, World!", Encoding.UTF8.GetString(bytes!));
    }

    [Fact]
    public void Utf8_NullReturnsNull()
    {
        var bytes = Serializers.Utf8.Serialize(null!, default);
        Assert.Null(bytes);
    }

    [Fact]
    public void ByteArray_ReturnsInput()
    {
        var input = new byte[] { 1, 2, 3, 4, 5 };
        var bytes = Serializers.ByteArray.Serialize(input, default);
        Assert.Same(input, bytes);
    }

    [Fact]
    public void Int32_SerializesCorrectly()
    {
        var bytes = Serializers.Int32.Serialize(12345, default);
        Assert.NotNull(bytes);
        Assert.Equal(4, bytes!.Length);

        // Big-endian format
        var value = BitConverter.ToInt32(bytes.Reverse().ToArray());
        Assert.Equal(12345, value);
    }

    [Fact]
    public void Int64_SerializesCorrectly()
    {
        var bytes = Serializers.Int64.Serialize(1234567890123L, default);
        Assert.NotNull(bytes);
        Assert.Equal(8, bytes!.Length);
    }

    [Fact]
    public void Single_SerializesCorrectly()
    {
        var bytes = Serializers.Single.Serialize(3.14f, default);
        Assert.NotNull(bytes);
        Assert.Equal(4, bytes!.Length);
    }

    [Fact]
    public void Double_SerializesCorrectly()
    {
        var bytes = Serializers.Double.Serialize(3.14159265359, default);
        Assert.NotNull(bytes);
        Assert.Equal(8, bytes!.Length);
    }

    [Fact]
    public void Null_ReturnsNull()
    {
        var bytes = Serializers.Null.Serialize(null!, default);
        Assert.Null(bytes);
    }
}
