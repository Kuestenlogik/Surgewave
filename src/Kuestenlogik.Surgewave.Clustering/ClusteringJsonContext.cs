using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Clustering;

/// <summary>
/// JSON source generator context for clustering serialization types.
/// Enables trimming and AOT compilation by avoiding reflection-based serialization.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    WriteIndented = false,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
// Raft Commands
[JsonSerializable(typeof(BrokerRegisteredCommand))]
[JsonSerializable(typeof(BrokerRemovedCommand))]
[JsonSerializable(typeof(TopicCreatedCommand))]
[JsonSerializable(typeof(TopicDeletedCommand))]
[JsonSerializable(typeof(PartitionAssignedCommand))]
[JsonSerializable(typeof(IsrChangedCommand))]
[JsonSerializable(typeof(LeaderChangedCommand))]
[JsonSerializable(typeof(ConfigChangedCommand))]
// Raft State/Snapshots
[JsonSerializable(typeof(MetadataSnapshot))]
[JsonSerializable(typeof(PartitionStateSnapshot))]
[JsonSerializable(typeof(RaftPersistentState))]
// Core Models (used in snapshots)
[JsonSerializable(typeof(BrokerNode))]
[JsonSerializable(typeof(TopicMetadata))]
[JsonSerializable(typeof(PartitionState))]
[JsonSerializable(typeof(List<BrokerNode>))]
[JsonSerializable(typeof(List<TopicMetadata>))]
[JsonSerializable(typeof(List<PartitionStateSnapshot>))]
[JsonSerializable(typeof(List<int>))]
[JsonSerializable(typeof(Dictionary<string, string>))]
internal sealed partial class ClusteringJsonContext : JsonSerializerContext
{
}
