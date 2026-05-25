using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// JSON source generator context for broker serialization types.
/// Enables trimming and AOT compilation by avoiding reflection-based serialization.
/// Note: Clustering-related types (Raft commands, snapshots) are in ClusteringJsonContext.
/// </summary>
[JsonSourceGenerationOptions(
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    WriteIndented = false,
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull)]
// Core Models
[JsonSerializable(typeof(BrokerNode))]
[JsonSerializable(typeof(TopicMetadata))]
[JsonSerializable(typeof(PartitionState))]
[JsonSerializable(typeof(List<BrokerNode>))]
[JsonSerializable(typeof(List<TopicMetadata>))]
[JsonSerializable(typeof(List<int>))]
[JsonSerializable(typeof(Dictionary<string, string>))]
// Broker Persistence Types
[JsonSerializable(typeof(GroupOffsets))]
[JsonSerializable(typeof(PersistedTransactionState))]
[JsonSerializable(typeof(PersistedPartition))]
[JsonSerializable(typeof(PendingOffsetEntry))]
[JsonSerializable(typeof(List<PersistedPartition>))]
[JsonSerializable(typeof(List<PendingOffsetEntry>))]
[JsonSerializable(typeof(List<string>))]
[JsonSerializable(typeof(PersistedQuotaConfig))]
[JsonSerializable(typeof(Dictionary<string, long>))]
internal sealed partial class BrokerJsonContext : JsonSerializerContext
{
}
