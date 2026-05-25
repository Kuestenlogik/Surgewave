namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Change record for changelog streams.
/// </summary>
public sealed record Change<TValue>(TValue? OldValue, TValue? NewValue);
