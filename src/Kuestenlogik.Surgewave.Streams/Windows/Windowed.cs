namespace Kuestenlogik.Surgewave.Streams.Windows;

/// <summary>
/// Windowed key wrapper.
/// </summary>
public sealed record Windowed<TKey>(TKey Key, Window Window);
