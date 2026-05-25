using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Windows;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

/// <summary>
/// Tests for state stores.
/// </summary>
public sealed class StateStoreTests
{
    // KIP-985: descending iteration over the state store.

    [Fact]
    public void InMemoryKeyValueStore_ReverseAll_ReturnsKeysDescending()
    {
        var store = new InMemoryKeyValueStore<int, string>("rev-all", Comparer<int>.Default);
        store.Put(1, "one");
        store.Put(3, "three");
        store.Put(2, "two");

        var result = store.ReverseAll().Select(kv => kv.Key).ToList();

        Assert.Equal([3, 2, 1], result);
    }

    [Fact]
    public void InMemoryKeyValueStore_ReverseRange_ReturnsKeysWithinBoundsDescending()
    {
        var store = new InMemoryKeyValueStore<int, string>("rev-range", Comparer<int>.Default);
        for (int i = 1; i <= 10; i++) store.Put(i, $"v{i}");

        // KIP-985 contract: from is the larger key, to is the smaller key.
        var result = store.ReverseRange(from: 7, to: 3).Select(kv => kv.Key).ToList();

        Assert.Equal([7, 6, 5, 4, 3], result);
    }

    [Fact]
    public void InMemoryKeyValueStore_ReverseRange_AcceptsSwappedBounds()
    {
        // Even if a caller passes (from < to), the implementation should produce the
        // same descending stream — defensive programming for a slightly clumsy contract.
        var store = new InMemoryKeyValueStore<int, string>("rev-range-swap", Comparer<int>.Default);
        for (int i = 1; i <= 5; i++) store.Put(i, $"v{i}");

        var result = store.ReverseRange(from: 2, to: 4).Select(kv => kv.Key).ToList();

        Assert.Equal([4, 3, 2], result);
    }

    [Fact]
    public void InMemoryKeyValueStore_ReverseRange_WithoutComparer_Throws()
    {
        var store = new InMemoryKeyValueStore<int, string>("no-cmp"); // no comparer
        store.Put(1, "a");
        Assert.Throws<InvalidOperationException>(() => store.ReverseRange(2, 1).ToList());
    }

    [Fact]
    public void InMemoryKeyValueStore_PutsAndGets()
    {
        // Arrange
        var store = new InMemoryKeyValueStore<string, int>("test-store");

        // Act
        store.Put("key1", 100);
        store.Put("key2", 200);
        var value1 = store.Get("key1");
        var value2 = store.Get("key2");

        // Assert
        Assert.Equal(100, value1);
        Assert.Equal(200, value2);
    }

    [Fact]
    public void InMemoryKeyValueStore_Delete_RemovesKey()
    {
        // Arrange
        var store = new InMemoryKeyValueStore<string, int>("test-store");
        store.Put("key1", 100);

        // Act
        store.Delete("key1");
        var value = store.Get("key1");

        // Assert
        Assert.Equal(default, value);
    }

    [Fact]
    public void InMemoryKeyValueStore_All_ReturnsAllEntries()
    {
        // Arrange
        var store = new InMemoryKeyValueStore<string, int>("test-store");
        store.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);

        // Act
        var all = store.All().ToList();

        // Assert
        Assert.Equal(3, all.Count);
    }

    [Fact]
    public void InMemoryKeyValueStore_ApproximateNumEntries()
    {
        // Arrange
        var store = new InMemoryKeyValueStore<string, int>("test-store");
        store.Put("a", 1);
        store.Put("b", 2);
        store.Put("c", 3);

        // Act
        var count = store.ApproximateNumEntries;

        // Assert
        Assert.Equal(3, count);
    }

    [Fact]
    public void InMemoryKeyValueStore_PutIfAbsent()
    {
        // Arrange
        var store = new InMemoryKeyValueStore<string, int>("test-store");
        store.Put("key1", 100);

        // Act
        var result = store.PutIfAbsent("key1", 200);
        var value = store.Get("key1");

        // Assert
        Assert.Equal(100, result); // Returns existing value
        Assert.Equal(100, value); // Value unchanged
    }

    [Fact]
    public void InMemoryKeyValueStore_PutAll()
    {
        // Arrange
        var store = new InMemoryKeyValueStore<string, int>("test-store");
        var entries = new[]
        {
            new KeyValue<string, int>("a", 1),
            new KeyValue<string, int>("b", 2),
            new KeyValue<string, int>("c", 3)
        };

        // Act
        store.PutAll(entries);

        // Assert
        Assert.Equal(1, store.Get("a"));
        Assert.Equal(2, store.Get("b"));
        Assert.Equal(3, store.Get("c"));
    }

    [Fact]
    public void InMemoryWindowStore_PutsAndGets()
    {
        // Arrange
        var store = new InMemoryWindowStore<string, int>(
            "test-window-store",
            TimeSpan.FromSeconds(10),
            TimeSpan.FromHours(1));

        var windowStart = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Act
        store.Put("key1", 100, windowStart);
        var value = store.Fetch("key1", windowStart);

        // Assert
        Assert.Equal(100, value);
    }

    [Fact]
    public void InMemoryWindowStore_FetchRange()
    {
        // Arrange
        var store = new InMemoryWindowStore<string, int>(
            "test-window-store",
            TimeSpan.FromSeconds(10),
            TimeSpan.FromHours(1));

        var baseTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        store.Put("key1", 100, baseTime);
        store.Put("key1", 200, baseTime + 10000);
        store.Put("key1", 300, baseTime + 20000);

        // Act
        var values = store.Fetch("key1", baseTime, baseTime + 30000).ToList();

        // Assert
        Assert.True(values.Count >= 2);
    }

    [Fact]
    public void InMemorySessionStore_PutsAndGets()
    {
        // Arrange
        var store = new InMemorySessionStore<string, int>(
            "test-session-store",
            TimeSpan.FromMinutes(5));

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var window = new Window(now, now + 1000);

        // Act
        store.Put(new Windowed<string>("key1", window), 42);
        var sessions = store.FindSessions("key1", now - 1000, now + 2000).ToList();

        // Assert
        Assert.NotEmpty(sessions);
    }

    [Fact]
    public void InMemorySessionStore_Remove()
    {
        // Arrange
        var store = new InMemorySessionStore<string, int>(
            "test-session-store",
            TimeSpan.FromMinutes(5));

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var window = new Window(now, now + 1000);
        var windowed = new Windowed<string>("key1", window);

        store.Put(windowed, 42);

        // Act
        store.Remove(windowed);
        var sessions = store.FindSessions("key1", now - 1000, now + 2000).ToList();

        // Assert
        Assert.Empty(sessions);
    }

    [Fact]
    public void KeyValue_Properties()
    {
        // Arrange & Act
        var kv = new KeyValue<string, int>("key", 42);

        // Assert
        Assert.Equal("key", kv.Key);
        Assert.Equal(42, kv.Value);
    }

    [Fact]
    public void Windowed_Properties()
    {
        // Arrange
        var window = new Window(1000, 2000);
        var windowed = new Windowed<string>("key", window);

        // Assert
        Assert.Equal("key", windowed.Key);
        Assert.Equal(1000, windowed.Window.StartMs);
        Assert.Equal(2000, windowed.Window.EndMs);
    }

    [Fact]
    public void Window_Properties()
    {
        // Arrange
        var window = new Window(1000, 2000);

        // Assert
        Assert.Equal(1000, window.StartMs);
        Assert.Equal(2000, window.EndMs);
    }

    [Fact]
    public void InMemoryKeyValueStore_Name()
    {
        // Arrange
        var store = new InMemoryKeyValueStore<string, int>("my-store");

        // Assert
        Assert.Equal("my-store", store.Name);
        Assert.False(store.Persistent);
    }

    [Fact]
    public void InMemoryWindowStore_Name()
    {
        // Arrange
        var store = new InMemoryWindowStore<string, int>(
            "my-window-store",
            TimeSpan.FromSeconds(30),
            TimeSpan.FromHours(1));

        // Assert
        Assert.Equal("my-window-store", store.Name);
        Assert.False(store.Persistent);
    }

    [Fact]
    public void InMemorySessionStore_Name()
    {
        // Arrange
        var store = new InMemorySessionStore<string, int>(
            "my-session-store",
            TimeSpan.FromMinutes(5));

        // Assert
        Assert.Equal("my-session-store", store.Name);
        Assert.False(store.Persistent);
    }
}
