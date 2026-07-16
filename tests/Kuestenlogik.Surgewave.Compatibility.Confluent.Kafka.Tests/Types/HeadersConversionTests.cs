using System.Text;

namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

/// <summary>
/// Pins the Headers/dictionary conversion semantics (last value wins, null-safe
/// FromDictionary) and the Header value contract (null value reads as empty
/// bytes, null key rejected, "key: value" formatting).
/// </summary>
public class HeadersConversionTests
{
    [Fact]
    public void ToDictionary_LastValueWinsForDuplicateKeys()
    {
        var headers = new Headers
        {
            { "key1", Encoding.UTF8.GetBytes("first") },
            { "key1", Encoding.UTF8.GetBytes("last") },
            { "key2", Encoding.UTF8.GetBytes("other") }
        };

        var dict = headers.ToDictionary();

        Assert.Equal(2, dict.Count);
        Assert.Equal("last", Encoding.UTF8.GetString(dict["key1"]));
        Assert.Equal("other", Encoding.UTF8.GetString(dict["key2"]));
    }

    [Fact]
    public void ToDictionary_NullHeaderValue_BecomesEmptyBytes()
    {
        var headers = new Headers { { "key1", null } };

        var dict = headers.ToDictionary();

        Assert.Empty(dict["key1"]);
    }

    [Fact]
    public void FromDictionary_Null_ReturnsEmptyHeaders()
    {
        var headers = Headers.FromDictionary(null);

        Assert.NotNull(headers);
        Assert.Empty(headers);
    }

    [Fact]
    public void FromDictionary_RoundTripsContent()
    {
        var source = new Dictionary<string, byte[]>
        {
            ["a"] = Encoding.UTF8.GetBytes("1"),
            ["b"] = Encoding.UTF8.GetBytes("2")
        };

        var roundTripped = Headers.FromDictionary(source).ToDictionary();

        Assert.Equal(2, roundTripped.Count);
        Assert.Equal("1", Encoding.UTF8.GetString(roundTripped["a"]));
        Assert.Equal("2", Encoding.UTF8.GetString(roundTripped["b"]));
    }

    [Fact]
    public void GetLastHeader_ReturnsLastAddedForKey()
    {
        var headers = new Headers
        {
            { "key1", Encoding.UTF8.GetBytes("first") },
            { "key1", Encoding.UTF8.GetBytes("last") }
        };

        var header = headers.GetLastHeader("key1");

        Assert.NotNull(header);
        Assert.Equal("last", Encoding.UTF8.GetString(header.GetValueBytes()));
    }

    [Fact]
    public void GetLastHeader_MissingKey_ReturnsNull()
    {
        var headers = new Headers();
        Assert.Null(headers.GetLastHeader("missing"));
    }

    [Fact]
    public void Header_NullKey_Throws()
    {
        Assert.Throws<ArgumentNullException>("key", () => new Header(null!, []));
    }

    [Fact]
    public void Header_NullValue_GetValueBytesReturnsEmpty()
    {
        var header = new Header("key", null);
        Assert.Empty(header.GetValueBytes());
    }

    [Fact]
    public void Header_ToString_FormatsKeyAndValue()
    {
        var header = new Header("trace-id", Encoding.UTF8.GetBytes("abc"));
        Assert.Equal("trace-id: abc", header.ToString());
    }

    [Fact]
    public void Header_ToString_NullValue_ShowsNull()
    {
        var header = new Header("trace-id", null);
        Assert.Equal("trace-id: null", header.ToString());
    }
}
