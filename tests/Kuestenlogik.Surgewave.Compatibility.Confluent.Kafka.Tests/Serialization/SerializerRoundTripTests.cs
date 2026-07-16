namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Serialization;

/// <summary>
/// Pins serializer/deserializer pairing: values must round-trip losslessly,
/// numeric encodings must be big-endian (Kafka wire order), and deserializers
/// must tolerate null or truncated input by returning defaults.
/// </summary>
public class SerializerRoundTripTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(-1)]
    [InlineData(int.MinValue)]
    [InlineData(int.MaxValue)]
    public void Int32_RoundTrips(int value)
    {
        var bytes = Serializers.Int32.Serialize(value, default);

        Assert.NotNull(bytes);
        Assert.Equal(value, Deserializers.Int32.Deserialize(bytes, false, default));
    }

    [Fact]
    public void Int32_EncodesBigEndian()
    {
        var bytes = Serializers.Int32.Serialize(0x01020304, default);

        Assert.NotNull(bytes);
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, bytes);
    }

    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    [InlineData(long.MinValue)]
    [InlineData(long.MaxValue)]
    public void Int64_RoundTrips(long value)
    {
        var bytes = Serializers.Int64.Serialize(value, default);

        Assert.NotNull(bytes);
        Assert.Equal(value, Deserializers.Int64.Deserialize(bytes, false, default));
    }

    [Fact]
    public void Int64_EncodesBigEndian()
    {
        var bytes = Serializers.Int64.Serialize(0x0102030405060708L, default);

        Assert.NotNull(bytes);
        Assert.Equal(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }, bytes);
    }

    [Theory]
    [InlineData(0f)]
    [InlineData(3.5f)]
    [InlineData(-0.25f)]
    [InlineData(float.MaxValue)]
    public void Single_RoundTrips(float value)
    {
        var bytes = Serializers.Single.Serialize(value, default);

        Assert.NotNull(bytes);
        Assert.Equal(value, Deserializers.Single.Deserialize(bytes, false, default));
    }

    [Theory]
    [InlineData(0d)]
    [InlineData(2.5d)]
    [InlineData(-0.125d)]
    [InlineData(double.MaxValue)]
    public void Double_RoundTrips(double value)
    {
        var bytes = Serializers.Double.Serialize(value, default);

        Assert.NotNull(bytes);
        Assert.Equal(value, Deserializers.Double.Deserialize(bytes, false, default));
    }

    [Fact]
    public void Utf8_RoundTripsUnicode()
    {
        const string value = "wave üßé \U0001F30A";

        var bytes = Serializers.Utf8.Serialize(value, default);

        Assert.NotNull(bytes);
        Assert.Equal(value, Deserializers.Utf8.Deserialize(bytes, false, default));
    }

    [Fact]
    public void ByteArray_RoundTripsContent()
    {
        var value = new byte[] { 0, 1, 2, 254, 255 };

        var bytes = Serializers.ByteArray.Serialize(value, default);

        Assert.NotNull(bytes);
        Assert.Equal(value, Deserializers.ByteArray.Deserialize(bytes, false, default));
    }

    [Fact]
    public void NumericDeserializers_TruncatedBuffer_ReturnZero()
    {
        var threeBytes = new byte[] { 1, 2, 3 };
        var sevenBytes = new byte[] { 1, 2, 3, 4, 5, 6, 7 };

        Assert.Equal(0, Deserializers.Int32.Deserialize(threeBytes, false, default));
        Assert.Equal(0L, Deserializers.Int64.Deserialize(sevenBytes, false, default));
        Assert.Equal(0f, Deserializers.Single.Deserialize(threeBytes, false, default));
        Assert.Equal(0d, Deserializers.Double.Deserialize(sevenBytes, false, default));
    }

    [Fact]
    public void NumericDeserializers_NullInput_ReturnZero()
    {
        Assert.Equal(0, Deserializers.Int32.Deserialize(default, true, default));
        Assert.Equal(0L, Deserializers.Int64.Deserialize(default, true, default));
        Assert.Equal(0f, Deserializers.Single.Deserialize(default, true, default));
        Assert.Equal(0d, Deserializers.Double.Deserialize(default, true, default));
    }

    [Fact]
    public void ByteArrayDeserializer_NullInput_ReturnsEmptyArray()
    {
        var result = Deserializers.ByteArray.Deserialize(default, true, default);

        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public void Utf8Deserializer_EmptyNonNullInput_ReturnsEmptyString()
    {
        var result = Deserializers.Utf8.Deserialize(default, false, default);
        Assert.Equal(string.Empty, result);
    }
}
