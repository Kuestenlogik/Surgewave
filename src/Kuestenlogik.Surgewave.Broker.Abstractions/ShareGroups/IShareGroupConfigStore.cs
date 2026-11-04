namespace Kuestenlogik.Surgewave.Broker.ShareGroups;

/// <summary>
/// Protocol-neutral seam over the share-group config store (KIP-1240 per-group
/// dynamic config via IncrementalAlterConfigs). Implemented by the broker's
/// <c>ShareGroupCoordinator</c>. Keeps the Kafka config handler off the concrete
/// coordinator type (#59 b4-tier2).
/// </summary>
public interface IShareGroupConfigStore
{
    /// <summary>
    /// Set a share-group config value, answered once it is durable with the group's state (#219). Returns an error
    /// message when the group, config name, or value is rejected; <c>null</c> on success.
    /// </summary>
    Task<string?> SetShareGroupConfigAsync(string groupId, string name, string? value, CancellationToken cancellationToken);
}
