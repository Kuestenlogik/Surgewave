namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// Result of a receive operation.
/// </summary>
public record ReceiveResult(long HighWatermark, List<ReceivedMessage> Messages);
