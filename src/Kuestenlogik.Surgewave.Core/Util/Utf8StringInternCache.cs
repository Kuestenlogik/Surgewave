using System.Collections.Concurrent;
using System.Text;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// Interns strings decoded from UTF-8 wire bytes (topic names, client ids) so the same handful of
/// names does not allocate a fresh string per request. Keyed by a 32-bit hash of the raw bytes,
/// but — unlike the hand-rolled caches this replaces (#73) — every hit is VERIFIED against the
/// entry's stored UTF-8 bytes with a vectorized <see cref="MemoryExtensions.SequenceEqual{T}(ReadOnlySpan{T}, ReadOnlySpan{T})"/>,
/// so a 32-bit collision can never return the wrong string (the old caches routed produce traffic
/// to the wrong topic on collision). On a verified collision the newest string wins the slot; both
/// colliding names then stay correct, merely uncached.
/// </summary>
public sealed class Utf8StringInternCache
{
    private readonly ConcurrentDictionary<int, Entry> _cache = new();
    private readonly int _maxEntries;
    private readonly int _maxByteLength;

    /// <param name="maxEntries">Backstop for the entry count — beyond it, new names decode uncached.</param>
    /// <param name="maxByteLength">Longest byte sequence worth caching; longer inputs decode uncached.</param>
    public Utf8StringInternCache(int maxEntries = 10_000, int maxByteLength = 256)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxEntries);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxByteLength);
        _maxEntries = maxEntries;
        _maxByteLength = maxByteLength;
    }

    /// <summary>
    /// Returns the interned string for <paramref name="utf8"/>, decoding and (bounded) caching on miss.
    /// The hot hit path is one hash + one dictionary probe + one vectorized byte compare — no allocation.
    /// </summary>
    public string GetOrAdd(ReadOnlySpan<byte> utf8)
    {
        if (utf8.IsEmpty)
            return string.Empty;
        if (utf8.Length > _maxByteLength)
            return Encoding.UTF8.GetString(utf8);

        var hash = new HashCode();
        hash.AddBytes(utf8);
        var key = hash.ToHashCode();

        var found = _cache.TryGetValue(key, out var entry);
        if (found && utf8.SequenceEqual(entry.Utf8))
            return entry.Value;

        var value = Encoding.UTF8.GetString(utf8);
        if (found)
            _cache[key] = new Entry(utf8.ToArray(), value); // verified collision — newest wins the slot
        else if (_cache.Count < _maxEntries)
            _cache.TryAdd(key, new Entry(utf8.ToArray(), value));
        return value;
    }

    private readonly record struct Entry(byte[] Utf8, string Value);
}
