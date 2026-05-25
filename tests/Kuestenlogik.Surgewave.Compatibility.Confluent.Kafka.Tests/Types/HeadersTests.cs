namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

using System.Text;

public class HeadersTests
{
    [Fact]
    public void Add_WithKeyAndValue_AddsHeader()
    {
        var headers = new Headers();
        headers.Add("key1", Encoding.UTF8.GetBytes("value1"));

        Assert.Single(headers);
        Assert.Equal("key1", headers[0].Key);
        Assert.Equal("value1", Encoding.UTF8.GetString(headers[0].GetValueBytes()));
    }

    [Fact]
    public void Add_WithHeader_AddsHeader()
    {
        var headers = new Headers();
        headers.Add(new Header("key1", Encoding.UTF8.GetBytes("value1")));

        Assert.Single(headers);
        Assert.Equal("key1", headers[0].Key);
    }

    [Fact]
    public void Indexer_ReturnsHeaderAtIndex()
    {
        var headers = new Headers
        {
            { "key1", Encoding.UTF8.GetBytes("value1") },
            { "key2", Encoding.UTF8.GetBytes("value2") }
        };

        Assert.Equal("key1", headers[0].Key);
        Assert.Equal("key2", headers[1].Key);
    }

    [Fact]
    public void Count_ReturnsNumberOfHeaders()
    {
        var headers = new Headers
        {
            { "key1", Encoding.UTF8.GetBytes("value1") },
            { "key2", Encoding.UTF8.GetBytes("value2") },
            { "key3", Encoding.UTF8.GetBytes("value3") }
        };

        Assert.Equal(3, headers.Count);
    }

    [Fact]
    public void TryGetLastBytes_ExistingKey_ReturnsTrue()
    {
        var headers = new Headers
        {
            { "key1", Encoding.UTF8.GetBytes("value1") }
        };

        var found = headers.TryGetLastBytes("key1", out var bytes);

        Assert.True(found);
        Assert.Equal("value1", Encoding.UTF8.GetString(bytes!));
    }

    [Fact]
    public void TryGetLastBytes_NonExistingKey_ReturnsFalse()
    {
        var headers = new Headers();

        var found = headers.TryGetLastBytes("missing", out var bytes);

        Assert.False(found);
        Assert.Null(bytes);
    }

    [Fact]
    public void TryGetLastBytes_MultipleValues_ReturnsLast()
    {
        var headers = new Headers
        {
            { "key1", Encoding.UTF8.GetBytes("first") },
            { "key1", Encoding.UTF8.GetBytes("second") },
            { "key1", Encoding.UTF8.GetBytes("last") }
        };

        var found = headers.TryGetLastBytes("key1", out var bytes);

        Assert.True(found);
        Assert.Equal("last", Encoding.UTF8.GetString(bytes!));
    }

    [Fact]
    public void Remove_ExistingKey_RemovesAllWithKey()
    {
        var headers = new Headers
        {
            { "key1", Encoding.UTF8.GetBytes("value1") },
            { "key1", Encoding.UTF8.GetBytes("value2") },
            { "key2", Encoding.UTF8.GetBytes("value3") }
        };

        headers.Remove("key1");

        Assert.Single(headers);
        Assert.Equal("key2", headers[0].Key);
    }

    [Fact]
    public void GetEnumerator_EnumeratesAllHeaders()
    {
        var headers = new Headers
        {
            { "key1", Encoding.UTF8.GetBytes("value1") },
            { "key2", Encoding.UTF8.GetBytes("value2") }
        };

        var keys = headers.Select(h => h.Key).ToList();

        Assert.Equal(new[] { "key1", "key2" }, keys);
    }
}
