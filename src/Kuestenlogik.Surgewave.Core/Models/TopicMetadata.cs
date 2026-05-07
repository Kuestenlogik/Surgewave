using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Core.Models;

/// <summary>
/// Topic metadata
/// </summary>
public sealed record TopicMetadata
{
    public required string Name { get; init; }
    /// <summary>
    /// Unique topic identifier (UUID). Assigned on topic creation.
    /// Used by Kafka protocol v10+ for topic identification instead of name.
    /// </summary>
    public required Guid TopicId { get; init; }
    public required int PartitionCount { get; set; }
    public required short ReplicationFactor { get; init; }
    public required Dictionary<string, string> Config { get; init; }
    public required DateTime CreatedAt { get; init; }

    /// <summary>
    /// Whether this topic is a mirror topic (replicated from a remote cluster via geo-replication).
    /// </summary>
    public bool IsMirror { get; set; }

    /// <summary>
    /// Whether this topic is read-only (mirror topics are read-only until promoted).
    /// </summary>
    public bool IsReadOnly { get; set; }

    /// <summary>
    /// The cluster link ID that this mirror topic is sourced from.
    /// </summary>
    public string? SourceLinkId { get; set; }

    /// <summary>
    /// Cleanup policy for this topic (delete, compact, or both)
    /// Determined by config["cleanup.policy"] or defaults to Delete
    /// </summary>
    public CleanupPolicy CleanupPolicy =>
        Config.TryGetValue("cleanup.policy", out var policy)
            ? ParseCleanupPolicy(policy)
            : CleanupPolicy.Delete;

    private static CleanupPolicy ParseCleanupPolicy(string policy) =>
        policy.ToLowerInvariant() switch
        {
            "compact" => CleanupPolicy.Compact,
            "delete" => CleanupPolicy.Delete,
            "compact,delete" or "delete,compact" => CleanupPolicy.DeleteAndCompact,
            "ephemeral" => CleanupPolicy.Ephemeral,
            _ => CleanupPolicy.Delete
        };
}
