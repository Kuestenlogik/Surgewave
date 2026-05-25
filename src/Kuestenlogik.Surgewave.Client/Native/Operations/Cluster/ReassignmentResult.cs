using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;

/// <summary>
/// Result of a reassignment operation.
/// </summary>
public record ReassignmentResult(bool Success, int PartitionCount, SurgewaveErrorCode ErrorCode);
