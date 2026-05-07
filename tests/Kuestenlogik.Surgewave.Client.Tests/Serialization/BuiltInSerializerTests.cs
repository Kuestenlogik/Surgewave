using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Client.Serialization;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests.Serialization;

/// <summary>
/// Tests for the built-in serializers and deserializers (String, Int32, Int64, Guid, ByteArray, Json, Null).
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class BuiltInSerializerTests
{
    private const string Topic = "test-topic";

    #region Null Serializer

    [Fact]
    public void NullSerializer_Serialize_ReturnsNull()
    {
        var result = Serializers.Null.Serialize(null, Topic);
        Assert.Null(result);
    }

    [Fact]
    public void NullSerializer_Serialize_Instance_ReturnsNull()
    {
        var result = Serializers.Null.Serialize(Null.Instance, Topic);
        Assert.Null(result);
    }

    [Fact]
    public void Null_Instance_IsSingleton()
    {
        Assert.Same(Null.Instance, Null.Instance);
    }

    #endregion

    #region String Serializer

    [Fact]
    public void StringSerializer_RoundTrip()
    {
        var original = "Hello, World!";
        var bytes = Serializers.String.Serialize(original, Topic);
        Assert.NotNull(bytes);

        var deserialized = Serializers.StringDeserializer.Deserialize(bytes, Topic);
        Assert.Equal(original, deserialized);
    }

    [Fact]
    public void StringSerializer_NullInput_ReturnsNull()
    {
        var result = Serializers.String.Serialize(null, Topic);
        Assert.Null(result);
    }

    [Fact]
    public void StringDeserializer_EmptyBytes_ReturnsEmptyString()
    {
        var result = Serializers.StringDeserializer.Deserialize(ReadOnlySpan<byte>.Empty, Topic);
        Assert.Equal(string.Empty, result);
    }

    [Fact]
    public void StringSerializer_EmptyString_RoundTrip()
    {
        var bytes = Serializers.String.Serialize("", Topic);
        Assert.NotNull(bytes);
        Assert.Empty(bytes);

        var deserialized = Serializers.StringDeserializer.Deserialize(bytes, Topic);
        Assert.Equal("", deserialized);
    }

    [Fact]
    public void StringSerializer_UnicodeCharacters_RoundTrip()
    {
        var original = "Grüße aus dem Weltall \u00e4\u00f6\u00fc \u65e5\u672c\u8a9e";
        var bytes = Serializers.String.Serialize(original, Topic);
        Assert.NotNull(bytes);

        var deserialized = Serializers.StringDeserializer.Deserialize(bytes, Topic);
        Assert.Equal(original, deserialized);
    }

    [Fact]
    public void StringSerializer_UsesUtf8Encoding()
    {
        var input = "ABC";
        var bytes = Serializers.String.Serialize(input, Topic);
        Assert.NotNull(bytes);
        Assert.Equal(Encoding.UTF8.GetBytes(input), bytes);
    }

    #endregion

    #region Int32 Serializer

    [Fact]
    public void Int32Serializer_RoundTrip()
    {
        var original = 42;
        var bytes = Serializers.Int32.Serialize(original, Topic);
        Assert.NotNull(bytes);
        Assert.Equal(4, bytes.Length);

        var deserialized = Serializers.Int32Deserializer.Deserialize(bytes, Topic);
        Assert.Equal(original, deserialized);
    }

    [Fact]
    public void Int32Serializer_Zero_RoundTrip()
    {
        var bytes = Serializers.Int32.Serialize(0, Topic);
        var deserialized = Serializers.Int32Deserializer.Deserialize(bytes!, Topic);
        Assert.Equal(0, deserialized);
    }

    [Fact]
    public void Int32Serializer_MinValue_RoundTrip()
    {
        var bytes = Serializers.Int32.Serialize(int.MinValue, Topic);
        var deserialized = Serializers.Int32Deserializer.Deserialize(bytes!, Topic);
        Assert.Equal(int.MinValue, deserialized);
    }

    [Fact]
    public void Int32Serializer_MaxValue_RoundTrip()
    {
        var bytes = Serializers.Int32.Serialize(int.MaxValue, Topic);
        var deserialized = Serializers.Int32Deserializer.Deserialize(bytes!, Topic);
        Assert.Equal(int.MaxValue, deserialized);
    }

    [Fact]
    public void Int32Serializer_NegativeValue_RoundTrip()
    {
        var bytes = Serializers.Int32.Serialize(-12345, Topic);
        var deserialized = Serializers.Int32Deserializer.Deserialize(bytes!, Topic);
        Assert.Equal(-12345, deserialized);
    }

    [Fact]
    public void Int32Serializer_UsesBigEndian()
    {
        var bytes = Serializers.Int32.Serialize(1, Topic);
        Assert.NotNull(bytes);
        // Big-endian: MSB first => [0, 0, 0, 1]
        Assert.Equal(new byte[] { 0, 0, 0, 1 }, bytes);
    }

    #endregion

    #region Int64 Serializer

    [Fact]
    public void Int64Serializer_RoundTrip()
    {
        var original = 123456789L;
        var bytes = Serializers.Int64.Serialize(original, Topic);
        Assert.NotNull(bytes);
        Assert.Equal(8, bytes.Length);

        var deserialized = Serializers.Int64Deserializer.Deserialize(bytes, Topic);
        Assert.Equal(original, deserialized);
    }

    [Fact]
    public void Int64Serializer_Zero_RoundTrip()
    {
        var bytes = Serializers.Int64.Serialize(0L, Topic);
        var deserialized = Serializers.Int64Deserializer.Deserialize(bytes!, Topic);
        Assert.Equal(0L, deserialized);
    }

    [Fact]
    public void Int64Serializer_MinValue_RoundTrip()
    {
        var bytes = Serializers.Int64.Serialize(long.MinValue, Topic);
        var deserialized = Serializers.Int64Deserializer.Deserialize(bytes!, Topic);
        Assert.Equal(long.MinValue, deserialized);
    }

    [Fact]
    public void Int64Serializer_MaxValue_RoundTrip()
    {
        var bytes = Serializers.Int64.Serialize(long.MaxValue, Topic);
        var deserialized = Serializers.Int64Deserializer.Deserialize(bytes!, Topic);
        Assert.Equal(long.MaxValue, deserialized);
    }

    [Fact]
    public void Int64Serializer_UsesBigEndian()
    {
        var bytes = Serializers.Int64.Serialize(1L, Topic);
        Assert.NotNull(bytes);
        Assert.Equal(new byte[] { 0, 0, 0, 0, 0, 0, 0, 1 }, bytes);
    }

    #endregion

    #region Guid Serializer

    [Fact]
    public void GuidSerializer_RoundTrip()
    {
        var original = Guid.NewGuid();
        var bytes = Serializers.Guid.Serialize(original, Topic);
        Assert.NotNull(bytes);
        Assert.Equal(16, bytes.Length);

        var deserialized = Serializers.GuidDeserializer.Deserialize(bytes, Topic);
        Assert.Equal(original, deserialized);
    }

    [Fact]
    public void GuidSerializer_EmptyGuid_RoundTrip()
    {
        var bytes = Serializers.Guid.Serialize(Guid.Empty, Topic);
        var deserialized = Serializers.GuidDeserializer.Deserialize(bytes!, Topic);
        Assert.Equal(Guid.Empty, deserialized);
    }

    [Fact]
    public void GuidSerializer_MultipleGuids_AllUnique()
    {
        var guids = Enumerable.Range(0, 100).Select(_ => Guid.NewGuid()).ToList();
        var serialized = guids.Select(g => Serializers.Guid.Serialize(g, Topic)!).ToList();

        var deserialized = serialized.Select(b => Serializers.GuidDeserializer.Deserialize(b, Topic)).ToList();
        Assert.Equal(guids, deserialized);
    }

    #endregion

    #region ByteArray Serializer

    [Fact]
    public void ByteArraySerializer_RoundTrip()
    {
        var original = new byte[] { 1, 2, 3, 4, 5 };
        var bytes = Serializers.ByteArray.Serialize(original, Topic);
        Assert.Same(original, bytes); // pass-through, same reference

        var deserialized = Serializers.ByteArrayDeserializer.Deserialize(bytes!, Topic);
        Assert.Equal(original, deserialized);
    }

    [Fact]
    public void ByteArraySerializer_NullInput_ReturnsNull()
    {
        var result = Serializers.ByteArray.Serialize(null, Topic);
        Assert.Null(result);
    }

    [Fact]
    public void ByteArraySerializer_EmptyArray_RoundTrip()
    {
        var original = Array.Empty<byte>();
        var bytes = Serializers.ByteArray.Serialize(original, Topic);
        Assert.Same(original, bytes);

        var deserialized = Serializers.ByteArrayDeserializer.Deserialize(ReadOnlySpan<byte>.Empty, Topic);
        Assert.Empty(deserialized);
    }

    [Fact]
    public void ByteArrayDeserializer_CreatesNewArray()
    {
        var original = new byte[] { 10, 20, 30 };
        var deserialized = Serializers.ByteArrayDeserializer.Deserialize(original, Topic);

        // ByteArrayDeserializer.Deserialize calls data.ToArray(), so different reference
        Assert.Equal(original, deserialized);
        Assert.NotSame(original, deserialized);
    }

    #endregion

    #region Json Serializer

    [Fact]
    public void JsonSerializer_RoundTrip()
    {
        var serializer = Serializers.Json<TestRecord>();
        var deserializer = Serializers.JsonDeserializer<TestRecord>();

        var original = new TestRecord { Name = "test", Count = 42 };
        var bytes = serializer.Serialize(original, Topic);
        Assert.NotNull(bytes);

        var deserialized = deserializer.Deserialize(bytes, Topic);
        Assert.NotNull(deserialized);
        Assert.Equal("test", deserialized.Name);
        Assert.Equal(42, deserialized.Count);
    }

    [Fact]
    public void JsonSerializer_NullInput_ReturnsNull()
    {
        var serializer = Serializers.Json<TestRecord>();
        var result = serializer.Serialize(null, Topic);
        Assert.Null(result);
    }

    [Fact]
    public void JsonSerializer_WithCustomOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            WriteIndented = false
        };

        var serializer = Serializers.Json<TestRecord>(options);
        var original = new TestRecord { Name = "test", Count = 5 };
        var bytes = serializer.Serialize(original, Topic);
        Assert.NotNull(bytes);

        var json = Encoding.UTF8.GetString(bytes);
        Assert.Contains("\"name\"", json); // camelCase
        Assert.Contains("\"count\"", json);
    }

    [Fact]
    public void JsonDeserializer_CaseInsensitive()
    {
        var deserializer = Serializers.JsonDeserializer<TestRecord>();
        var json = """{"Name":"test","Count":10}"""u8;
        var result = deserializer.Deserialize(json, Topic);
        Assert.NotNull(result);
        Assert.Equal("test", result.Name);
        Assert.Equal(10, result.Count);
    }

    [Fact]
    public void JsonSerializer_ComplexObject_RoundTrip()
    {
        var serializer = Serializers.Json<ComplexRecord>();
        var deserializer = Serializers.JsonDeserializer<ComplexRecord>();

        var original = new ComplexRecord
        {
            Id = Guid.NewGuid(),
            Tags = ["tag1", "tag2"],
            Nested = new TestRecord { Name = "inner", Count = 99 }
        };

        var bytes = serializer.Serialize(original, Topic);
        var deserialized = deserializer.Deserialize(bytes!, Topic);

        Assert.Equal(original.Id, deserialized.Id);
        Assert.Equal(original.Tags, deserialized.Tags);
        Assert.Equal("inner", deserialized.Nested!.Name);
        Assert.Equal(99, deserialized.Nested.Count);
    }

    #endregion

    #region Singleton Properties

    [Fact]
    public void Serializers_ReturnSameInstance()
    {
        Assert.Same(Serializers.String, Serializers.String);
        Assert.Same(Serializers.Int32, Serializers.Int32);
        Assert.Same(Serializers.Int64, Serializers.Int64);
        Assert.Same(Serializers.Guid, Serializers.Guid);
        Assert.Same(Serializers.ByteArray, Serializers.ByteArray);
        Assert.Same(Serializers.Null, Serializers.Null);

        Assert.Same(Serializers.StringDeserializer, Serializers.StringDeserializer);
        Assert.Same(Serializers.Int32Deserializer, Serializers.Int32Deserializer);
        Assert.Same(Serializers.Int64Deserializer, Serializers.Int64Deserializer);
        Assert.Same(Serializers.GuidDeserializer, Serializers.GuidDeserializer);
        Assert.Same(Serializers.ByteArrayDeserializer, Serializers.ByteArrayDeserializer);
    }

    #endregion

    #region Test Types

    private sealed class TestRecord
    {
        public string Name { get; set; } = "";
        public int Count { get; set; }
    }

    private sealed class ComplexRecord
    {
        public Guid Id { get; set; }
        public List<string> Tags { get; set; } = [];
        public TestRecord? Nested { get; set; }
    }

    #endregion
}
