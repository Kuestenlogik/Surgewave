namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// Typed received message with deserialized key and value.
/// </summary>
public record TypedReceivedMessage<TKey, TValue>(long Offset, long Timestamp, TKey? Key, TValue Value);
