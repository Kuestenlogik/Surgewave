using Kuestenlogik.Surgewave.Clustering.Replication;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Represents a failure domain in a hierarchical structure.
/// Failure domains are organized as: Region > Datacenter > Zone > Rack
/// </summary>
public sealed class FailureDomain
{
    /// <summary>
    /// The level of this failure domain in the hierarchy.
    /// </summary>
    public required FailureDomainLevel Level { get; init; }

    /// <summary>
    /// The name of this failure domain.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// The full path of this domain (e.g., "us-east/dc1/zone-a").
    /// </summary>
    public required string Path { get; init; }

    /// <summary>
    /// The parent domain, if any.
    /// </summary>
    public FailureDomain? Parent { get; init; }

    /// <summary>
    /// Child domains within this domain.
    /// </summary>
    public List<FailureDomain> Children { get; } = [];

    /// <summary>
    /// Broker IDs directly in this domain (at this exact level).
    /// </summary>
    public HashSet<int> Brokers { get; } = [];

    /// <summary>
    /// Gets all broker IDs in this domain and all child domains.
    /// </summary>
    public IEnumerable<int> GetAllBrokers()
    {
        foreach (var broker in Brokers)
            yield return broker;

        foreach (var child in Children)
        {
            foreach (var broker in child.GetAllBrokers())
                yield return broker;
        }
    }

    /// <summary>
    /// Checks if this domain is an ancestor of another domain.
    /// </summary>
    public bool IsAncestorOf(FailureDomain other)
    {
        var current = other.Parent;
        while (current != null)
        {
            if (current == this)
                return true;
            current = current.Parent;
        }
        return false;
    }

    /// <summary>
    /// Checks if this domain contains the specified broker.
    /// </summary>
    public bool ContainsBroker(int brokerId)
    {
        if (Brokers.Contains(brokerId))
            return true;

        return Children.Any(c => c.ContainsBroker(brokerId));
    }

    public override string ToString() => $"{Level}:{Path}";

    public override int GetHashCode() => Path.GetHashCode();

    public override bool Equals(object? obj) =>
        obj is FailureDomain other && Path == other.Path;
}
