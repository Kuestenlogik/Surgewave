using System.Text.Json;

namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Executes typed queries against registered state stores and returns
/// JSON-friendly key/value pairs. Uses reflection to call the typed
/// store methods, following the same pattern as <see cref="RemoteQueryServer"/>.
/// </summary>
public sealed class StateStoreQueryExecutor
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly IStateStoreRegistry _registry;

    /// <summary>
    /// Initialises a new <see cref="StateStoreQueryExecutor"/> backed by the given registry.
    /// </summary>
    public StateStoreQueryExecutor(IStateStoreRegistry registry)
    {
        _registry = registry;
    }

    /// <summary>
    /// Returns a paginated list of all entries in the store.
    /// </summary>
    /// <param name="storeName">The store name.</param>
    /// <param name="offset">Number of entries to skip (zero-based).</param>
    /// <param name="limit">Maximum number of entries to return.</param>
    /// <returns>A list of key/value pairs serialised as objects.</returns>
    /// <exception cref="KeyNotFoundException">The store was not found.</exception>
    public IReadOnlyList<KeyValuePair<string, object?>> GetAll(string storeName, int offset, int limit)
    {
        var store = RequireStore(storeName);
        var entries = GetAllEntries(store);

        return entries
            .Skip(offset)
            .Take(limit)
            .ToList();
    }

    /// <summary>
    /// Returns the value for the given string-encoded key, or null if not found.
    /// </summary>
    /// <param name="storeName">The store name.</param>
    /// <param name="key">The key, serialised as a JSON string (or plain string).</param>
    public object? GetByKey(string storeName, string key)
    {
        var store = RequireStore(storeName);
        return LookupByKey(store, key);
    }

    /// <summary>
    /// Returns all entries whose keys fall within [from, to] (string-encoded).
    /// </summary>
    /// <param name="storeName">The store name.</param>
    /// <param name="from">The lower bound key (inclusive).</param>
    /// <param name="to">The upper bound key (inclusive).</param>
    public IReadOnlyList<KeyValuePair<string, object?>> GetRange(string storeName, string from, string to)
    {
        var store = RequireStore(storeName);
        return GetRangeEntries(store, from, to);
    }

    /// <summary>
    /// Returns the approximate entry count for the store.
    /// </summary>
    /// <param name="storeName">The store name.</param>
    public long GetCount(string storeName)
    {
        var store = RequireStore(storeName);
        return GetApproximateCount(store);
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    private IStateStore RequireStore(string storeName)
    {
        var store = _registry.GetStore(storeName);
        if (store == null)
            throw new KeyNotFoundException($"State store '{storeName}' not found.");
        return store;
    }

    private static IEnumerable<KeyValuePair<string, object?>> GetAllEntries(IStateStore store)
    {
        var kvInterface = FindKvInterface(store);
        if (kvInterface == null)
            yield break;

        var (keyType, valueType) = GetKvTypes(kvInterface);
        var allMethod = kvInterface.GetMethod("All");
        var entries = allMethod?.Invoke(store, null) as System.Collections.IEnumerable;
        if (entries == null)
            yield break;

        foreach (var entry in entries)
        {
            var (k, v) = ExtractKv(entry, keyType, valueType);
            yield return new KeyValuePair<string, object?>(k, v);
        }
    }

    private static IReadOnlyList<KeyValuePair<string, object?>> GetRangeEntries(
        IStateStore store, string from, string to)
    {
        var kvInterface = FindKvInterface(store);
        if (kvInterface == null)
            return [];

        var (keyType, valueType) = GetKvTypes(kvInterface);
        var rangeMethod = kvInterface.GetMethod("Range");
        if (rangeMethod == null)
            return [];

        var fromKey = DeserializeKey(from, keyType);
        var toKey = DeserializeKey(to, keyType);

        var entries = rangeMethod.Invoke(store, [fromKey, toKey]) as System.Collections.IEnumerable;
        if (entries == null)
            return [];

        var result = new List<KeyValuePair<string, object?>>();
        foreach (var entry in entries)
        {
            var (k, v) = ExtractKv(entry, keyType, valueType);
            result.Add(new KeyValuePair<string, object?>(k, v));
        }
        return result;
    }

    private static object? LookupByKey(IStateStore store, string keyString)
    {
        var kvInterface = FindKvInterface(store);
        if (kvInterface == null)
            return null;

        var (keyType, valueType) = GetKvTypes(kvInterface);

        // Scan All() to detect true key presence — this avoids false positives
        // for value types where default(T) is indistinguishable from "not found"
        // when calling Get() directly.
        var allMethod = kvInterface.GetMethod("All");
        var entries = allMethod?.Invoke(store, null) as System.Collections.IEnumerable;
        if (entries == null)
            return null;

        var desiredKey = DeserializeKey(keyString, keyType);

        foreach (var entry in entries)
        {
            var entryType = entry.GetType();
            var keyProp = entryType.GetProperty("Key");
            var valueProp = entryType.GetProperty("Value");
            if (keyProp == null || valueProp == null) continue;

            var rawKey = keyProp.GetValue(entry);
            if (!Equals(rawKey, desiredKey)) continue;

            var rawValue = valueProp.GetValue(entry);
            if (rawValue == null) return null;

            return JsonSerializer.Deserialize<object?>(
                JsonSerializer.SerializeToUtf8Bytes(rawValue, valueType, JsonOptions),
                JsonOptions);
        }

        return null;
    }

    private static long GetApproximateCount(IStateStore store)
    {
        var prop = store.GetType().GetProperty("ApproximateNumEntries");
        if (prop?.GetValue(store) is long count)
            return count;
        return 0;
    }

    // -----------------------------------------------------------------------
    // Reflection utilities
    // -----------------------------------------------------------------------

    private static System.Type? FindKvInterface(IStateStore store)
    {
        return store.GetType().GetInterfaces().FirstOrDefault(i =>
            i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IReadOnlyKeyValueStore<,>));
    }

    private static (System.Type keyType, System.Type valueType) GetKvTypes(System.Type kvInterface)
    {
        var args = kvInterface.GetGenericArguments();
        return (args[0], args[1]);
    }

    private static (string key, object? value) ExtractKv(
        object entry, System.Type keyType, System.Type valueType)
    {
        var entryType = entry.GetType();
        var keyProp = entryType.GetProperty("Key");
        var valueProp = entryType.GetProperty("Value");

        var rawKey = keyProp?.GetValue(entry);
        var rawValue = valueProp?.GetValue(entry);

        var keyStr = rawKey == null
            ? string.Empty
            : JsonSerializer.Serialize(rawKey, keyType, JsonOptions).Trim('"');

        object? valueObj = null;
        if (rawValue != null)
        {
            var bytes = JsonSerializer.SerializeToUtf8Bytes(rawValue, valueType, JsonOptions);
            valueObj = JsonSerializer.Deserialize<object?>(bytes, JsonOptions);
        }

        return (keyStr, valueObj);
    }

    private static object? DeserializeKey(string keyString, System.Type keyType)
    {
        // Treat plain strings as JSON strings by quoting them if needed
        if (keyType == typeof(string))
            return keyString;

        try
        {
            return JsonSerializer.Deserialize(keyString, keyType, JsonOptions);
        }
        catch (JsonException)
        {
            // Last-resort: wrap in quotes and try again (e.g. bare identifier)
            return JsonSerializer.Deserialize($"\"{keyString}\"", keyType, JsonOptions);
        }
    }
}
