using System.Collections;
using System.Text;

namespace Confluent.Kafka;

/// <summary>
/// A single header (key-value pair).
/// </summary>
public class Header
{
    /// <summary>
    /// Creates a new Header.
    /// </summary>
    public Header(string key, byte[]? value)
    {
        Key = key ?? throw new ArgumentNullException(nameof(key));
        _value = value;
    }

    /// <summary>
    /// The header key.
    /// </summary>
    public string Key { get; }

    private readonly byte[]? _value;

    /// <summary>
    /// Gets the header value.
    /// </summary>
    public byte[] GetValueBytes() => _value ?? [];

    /// <inheritdoc/>
    public override string ToString()
    {
        var valueStr = _value is null ? "null" : Encoding.UTF8.GetString(_value);
        return $"{Key}: {valueStr}";
    }
}

/// <summary>
/// A collection of message headers.
/// </summary>
public class Headers : IEnumerable<Header>
{
    private readonly List<Header> _headers = [];

    /// <summary>
    /// Creates an empty Headers collection.
    /// </summary>
    public Headers() { }

    /// <summary>
    /// Creates a Headers collection from existing headers.
    /// </summary>
    public Headers(IEnumerable<Header> headers)
    {
        _headers.AddRange(headers);
    }

    /// <summary>
    /// Number of headers.
    /// </summary>
    public int Count => _headers.Count;

    /// <summary>
    /// Adds a header.
    /// </summary>
    public void Add(string key, byte[]? value) => _headers.Add(new Header(key, value));

    /// <summary>
    /// Adds a header.
    /// </summary>
    public void Add(Header header) => _headers.Add(header);

    /// <summary>
    /// Gets the last header with the specified key.
    /// </summary>
    public Header? GetLastHeader(string key) => _headers.FindLast(h => h.Key == key);

    /// <summary>
    /// Tries to get the last header with the specified key.
    /// </summary>
    public bool TryGetLastBytes(string key, out byte[]? value)
    {
        var header = GetLastHeader(key);
        if (header is not null)
        {
            value = header.GetValueBytes();
            return true;
        }
        value = null;
        return false;
    }

    /// <summary>
    /// Removes all headers with the specified key.
    /// </summary>
    public void Remove(string key) => _headers.RemoveAll(h => h.Key == key);

    /// <summary>
    /// Gets the header at the specified index.
    /// </summary>
    public Header this[int index] => _headers[index];

    /// <inheritdoc/>
    public IEnumerator<Header> GetEnumerator() => _headers.GetEnumerator();

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>
    /// Converts to a dictionary (last value wins for duplicate keys).
    /// </summary>
    public IReadOnlyDictionary<string, byte[]> ToDictionary()
    {
        var dict = new Dictionary<string, byte[]>();
        foreach (var header in _headers)
        {
            dict[header.Key] = header.GetValueBytes();
        }
        return dict;
    }

    /// <summary>
    /// Creates Headers from a dictionary.
    /// </summary>
    public static Headers FromDictionary(IReadOnlyDictionary<string, byte[]>? dict)
    {
        var headers = new Headers();
        if (dict is not null)
        {
            foreach (var (key, value) in dict)
            {
                headers.Add(key, value);
            }
        }
        return headers;
    }
}
