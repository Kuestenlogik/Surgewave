namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// Typed message for batch send.
/// </summary>
public record TypedMessage<TKey, TValue>(TKey? Key, TValue Value, Dictionary<string, byte[]>? Headers = null);
