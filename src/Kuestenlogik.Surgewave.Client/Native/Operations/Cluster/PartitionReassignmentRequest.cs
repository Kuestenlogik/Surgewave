namespace Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;

/// <summary>
/// Partition reassignment request.
/// </summary>
public record PartitionReassignmentRequest(string Topic, int Partition, List<int> Replicas);
