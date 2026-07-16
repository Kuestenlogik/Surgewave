using System.Text;
using Kuestenlogik.Surgewave.Core.Util;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// #73 — the byte-verifying string intern cache: a 32-bit hash collision must return the correct
/// (freshly decoded) string, never the colliding cache entry. The old hand-rolled caches trusted the
/// hash alone, which routed produce traffic to the WRONG topic on collision.
/// </summary>
public class Utf8StringInternCacheTests
{
    private static int HashOf(ReadOnlySpan<byte> utf8)
    {
        // Identical hashing to the cache (HashCode is process-seeded, so compute it in-process).
        var hash = new HashCode();
        hash.AddBytes(utf8);
        return hash.ToHashCode();
    }

    /// <summary>Brute-force two distinct short strings whose byte hashes collide (birthday bound ≈ 77K).</summary>
    private static (string A, string B) FindCollidingPair()
    {
        var seen = new Dictionary<int, string>();
        for (var i = 0; ; i++)
        {
            var candidate = "t" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            var hash = HashOf(Encoding.UTF8.GetBytes(candidate));
            if (seen.TryGetValue(hash, out var previous))
                return (previous, candidate);
            seen[hash] = candidate;
        }
    }

    [Fact]
    public void HashCollision_ReturnsTheCorrectString_NotTheCachedCollider()
    {
        var (a, b) = FindCollidingPair();
        Assert.NotEqual(a, b);

        var cache = new Utf8StringInternCache();

        // Prime the slot with A, then look up the COLLIDING B — the bug returned A here.
        Assert.Equal(a, cache.GetOrAdd(Encoding.UTF8.GetBytes(a)));
        Assert.Equal(b, cache.GetOrAdd(Encoding.UTF8.GetBytes(b)));

        // Both stay correct afterwards, in either order (newest-wins slot replacement).
        Assert.Equal(a, cache.GetOrAdd(Encoding.UTF8.GetBytes(a)));
        Assert.Equal(b, cache.GetOrAdd(Encoding.UTF8.GetBytes(b)));
    }

    [Fact]
    public void RepeatedLookup_ReturnsTheSameInternedInstance()
    {
        var cache = new Utf8StringInternCache();
        var bytes = Encoding.UTF8.GetBytes("orders-topic");

        var first = cache.GetOrAdd(bytes);
        var second = cache.GetOrAdd(bytes);

        Assert.Same(first, second); // hit path: no allocation, same instance
    }

    [Fact]
    public void EmptyInput_ReturnsEmptyString()
    {
        var cache = new Utf8StringInternCache();
        Assert.Same(string.Empty, cache.GetOrAdd(ReadOnlySpan<byte>.Empty));
    }

    [Fact]
    public void OverlongInput_DecodesCorrectlyWithoutCaching()
    {
        var cache = new Utf8StringInternCache(maxEntries: 16, maxByteLength: 8);
        var value = new string('x', 32);
        var bytes = Encoding.UTF8.GetBytes(value);

        Assert.Equal(value, cache.GetOrAdd(bytes));
        Assert.Equal(value, cache.GetOrAdd(bytes)); // uncached, but always correct
    }

    [Fact]
    public void NonAsciiTopicNames_RoundTripCorrectly()
    {
        var cache = new Utf8StringInternCache();
        var value = "bestellungen-müller-日本";
        var bytes = Encoding.UTF8.GetBytes(value);

        Assert.Equal(value, cache.GetOrAdd(bytes));
        Assert.Same(cache.GetOrAdd(bytes), cache.GetOrAdd(bytes));
    }
}
