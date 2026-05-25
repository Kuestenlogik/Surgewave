using Kuestenlogik.Surgewave.Clustering.Cluster;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Validates that replica assignments meet failure domain requirements.
/// Ensures replicas are spread across different racks/zones/datacenters.
/// </summary>
public sealed class FailureDomainValidator
{
    private readonly ClusterState _clusterState;
    private readonly FailureDomainValidatorOptions _options;

    public FailureDomainValidator(ClusterState clusterState, FailureDomainValidatorOptions? options = null)
    {
        _clusterState = clusterState;
        _options = options ?? new FailureDomainValidatorOptions();
    }

    /// <summary>
    /// Validates that a replica assignment meets failure domain requirements.
    /// </summary>
    /// <param name="replicas">The broker IDs assigned as replicas.</param>
    /// <param name="topic">The topic name (for error reporting).</param>
    /// <param name="partition">The partition number (for error reporting).</param>
    /// <returns>A list of violations, empty if assignment is valid.</returns>
    public List<FailureDomainViolation> ValidateAssignment(
        IReadOnlyList<int> replicas,
        string? topic = null,
        int? partition = null)
    {
        var violations = new List<FailureDomainViolation>();

        if (replicas.Count == 0)
            return violations;

        // Get unique domains at the configured validation level
        var domains = GetDomainsForReplicas(replicas, _options.ValidationLevel);
        var uniqueDomains = domains.Distinct().Count();

        // Check minimum domain spread
        if (_options.MinDistinctDomains > 0 && uniqueDomains < _options.MinDistinctDomains)
        {
            violations.Add(new FailureDomainViolation
            {
                Type = ViolationType.InsufficientDomainSpread,
                Message = $"Replicas span only {uniqueDomains} {_options.ValidationLevel}(s), but {_options.MinDistinctDomains} required",
                Topic = topic,
                Partition = partition,
                RequiredDomains = _options.MinDistinctDomains,
                ActualDomains = uniqueDomains,
                Level = _options.ValidationLevel
            });
        }

        // Check if all replicas are in same domain (single point of failure)
        if (replicas.Count > 1 && uniqueDomains == 1 && _options.PreventSingleDomainReplicas)
        {
            violations.Add(new FailureDomainViolation
            {
                Type = ViolationType.SingleDomainReplicas,
                Message = $"All {replicas.Count} replicas are in the same {_options.ValidationLevel}: {domains.First()}",
                Topic = topic,
                Partition = partition,
                RequiredDomains = Math.Min(replicas.Count, GetAvailableDomainCount(_options.ValidationLevel)),
                ActualDomains = 1,
                Level = _options.ValidationLevel
            });
        }

        return violations;
    }

    /// <summary>
    /// Validates whether a topic can be created with the given replication factor.
    /// </summary>
    /// <param name="replicationFactor">The desired replication factor.</param>
    /// <param name="topic">The topic name (for error reporting).</param>
    /// <returns>A list of violations, empty if creation is allowed.</returns>
    public List<FailureDomainViolation> ValidateTopicCreation(short replicationFactor, string topic)
    {
        var violations = new List<FailureDomainViolation>();

        var availableDomains = GetAvailableDomainCount(_options.ValidationLevel);

        // Check if we have enough domains for the required minimum spread
        if (_options.MinDistinctDomains > 0 && availableDomains < _options.MinDistinctDomains)
        {
            violations.Add(new FailureDomainViolation
            {
                Type = ViolationType.InsufficientDomainsAvailable,
                Message = $"Cluster has only {availableDomains} {_options.ValidationLevel}(s), but {_options.MinDistinctDomains} required",
                Topic = topic,
                RequiredDomains = _options.MinDistinctDomains,
                ActualDomains = availableDomains,
                Level = _options.ValidationLevel
            });
        }

        // Warn if replication factor exceeds available domains
        if (_options.WarnOnInsufficientDomains && replicationFactor > availableDomains && availableDomains > 0)
        {
            violations.Add(new FailureDomainViolation
            {
                Type = ViolationType.InsufficientDomainsAvailable,
                Message = $"Replication factor {replicationFactor} exceeds available {_options.ValidationLevel}s ({availableDomains}). " +
                          "Multiple replicas will be placed in the same failure domain.",
                Topic = topic,
                RequiredDomains = replicationFactor,
                ActualDomains = availableDomains,
                Level = _options.ValidationLevel
            });
        }

        return violations;
    }

    /// <summary>
    /// Gets the failure domain for each replica broker.
    /// </summary>
    private List<string> GetDomainsForReplicas(IReadOnlyList<int> replicas, FailureDomainLevel level)
    {
        var domains = new List<string>(replicas.Count);
        foreach (var brokerId in replicas)
        {
            var broker = _clusterState.GetBroker(brokerId);
            domains.Add(GetDomainAtLevel(broker?.Rack, level));
        }
        return domains;
    }

    /// <summary>
    /// Extracts the domain at the specified level from a hierarchical rack string.
    /// Format: "region/datacenter/zone/rack" or just "rack"
    /// </summary>
    private static string GetDomainAtLevel(string? rack, FailureDomainLevel level)
    {
        if (string.IsNullOrEmpty(rack))
            return "default";

        var parts = rack.Split('/');

        // Handle flat rack format (no hierarchy)
        if (parts.Length == 1)
            return rack;

        // Hierarchical format: region/datacenter/zone/rack (indices 0/1/2/3)
        // Map level to index from start
        var index = level switch
        {
            FailureDomainLevel.Region => 0,
            FailureDomainLevel.Datacenter => Math.Min(1, parts.Length - 1),
            FailureDomainLevel.Zone => Math.Min(2, parts.Length - 1),
            FailureDomainLevel.Rack => parts.Length - 1,
            _ => parts.Length - 1
        };

        // For partial hierarchies, aggregate from start to requested level
        if (index >= parts.Length)
            return rack;

        return string.Join("/", parts.Take(index + 1));
    }

    /// <summary>
    /// Gets the count of available failure domains at the specified level.
    /// </summary>
    private int GetAvailableDomainCount(FailureDomainLevel level)
    {
        return _clusterState.Brokers.Values
            .Select(b => GetDomainAtLevel(b.Rack, level))
            .Distinct()
            .Count();
    }
}

/// <summary>
/// Configuration options for failure domain validation.
/// </summary>
public sealed class FailureDomainValidatorOptions
{
    /// <summary>
    /// The failure domain level to validate at.
    /// Default: Rack
    /// </summary>
    public FailureDomainLevel ValidationLevel { get; init; } = FailureDomainLevel.Rack;

    /// <summary>
    /// Minimum number of distinct failure domains required for replicas.
    /// 0 means no minimum (validation disabled).
    /// Default: 0
    /// </summary>
    public int MinDistinctDomains { get; init; } = 0;

    /// <summary>
    /// Whether to prevent all replicas from being in the same failure domain.
    /// Default: true
    /// </summary>
    public bool PreventSingleDomainReplicas { get; init; } = true;

    /// <summary>
    /// Whether to emit warnings when replication factor exceeds available domains.
    /// Default: true
    /// </summary>
    public bool WarnOnInsufficientDomains { get; init; } = true;
}
