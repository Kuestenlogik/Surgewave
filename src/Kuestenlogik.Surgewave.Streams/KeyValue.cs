namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// A key-value pair.
/// </summary>
public readonly record struct KeyValue<TKey, TValue>(TKey Key, TValue Value);
