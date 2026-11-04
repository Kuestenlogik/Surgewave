using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace Kuestenlogik.Surgewave.Coordination.Coordinators;

/// <summary>
/// The groups' dynamic configs (Kafka's GROUP config resource, e.g. KIP-1240's share-group configs) as every broker
/// knows them (#228): in a cluster they are entries of the metadata log, so a partition's leader applies a share group's
/// limits as the group's coordinator does.
/// </summary>
public interface IGroupConfigStore
{
    /// <summary>The configs set for the group, empty where none are.</summary>
    IReadOnlyDictionary<string, string> ConfigsOf(string groupId);

    /// <summary>
    /// Sets one config of the group, or removes it with a null value, answered once it is in the metadata log.
    /// </summary>
    Task<GroupConfigWriteStatus> SetAsync(string groupId, string name, string? value, CancellationToken cancellationToken);
}

/// <summary>How a group config change ended.</summary>
public enum GroupConfigWriteStatus
{
    /// <summary>The change is committed.</summary>
    Ok,

    /// <summary>No controller could take it now — none known, unreachable, or it lost the role; the client retries.</summary>
    NotAvailable,
}

/// <summary>Group configs of a broker on its own, kept in its memory.</summary>
public sealed class InMemoryGroupConfigStore : IGroupConfigStore
{
    private readonly ConcurrentDictionary<string, ImmutableDictionary<string, string>> _configs = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public IReadOnlyDictionary<string, string> ConfigsOf(string groupId) =>
        _configs.TryGetValue(groupId, out var configs) ? configs : ImmutableDictionary<string, string>.Empty;

    /// <inheritdoc />
    public Task<GroupConfigWriteStatus> SetAsync(string groupId, string name, string? value, CancellationToken cancellationToken)
    {
        _configs.AddOrUpdate(groupId,
            _ => value is null ? ImmutableDictionary<string, string>.Empty : ImmutableDictionary<string, string>.Empty.SetItem(name, value),
            (_, configs) => value is null ? configs.Remove(name) : configs.SetItem(name, value));
        return Task.FromResult(GroupConfigWriteStatus.Ok);
    }
}
