using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Admin;

/// <summary>
/// Result of a leader election.
/// </summary>
public record ElectionResult(string Topic, int Partition, SurgewaveErrorCode ErrorCode, string? ErrorMessage);
