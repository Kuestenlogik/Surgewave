namespace Kuestenlogik.Surgewave.Connect;

/// <summary>
/// Extension methods for dictionary types.
/// </summary>
public static class DictionaryExtensions
{
    /// <summary>
    /// Gets the value associated with the specified key, or a default value if the key is not found.
    /// </summary>
    /// <typeparam name="TKey">The type of keys in the dictionary.</typeparam>
    /// <typeparam name="TValue">The type of values in the dictionary.</typeparam>
    /// <param name="dictionary">The dictionary to search.</param>
    /// <param name="key">The key to locate.</param>
    /// <param name="defaultValue">The default value to return if the key is not found.</param>
    /// <returns>The value associated with the key, or the default value if not found.</returns>
    public static TValue? GetValueOrDefault<TKey, TValue>(
        this IDictionary<TKey, TValue> dictionary,
        TKey key,
        TValue? defaultValue = default)
    {
        return dictionary.TryGetValue(key, out var value) ? value : defaultValue;
    }
}
