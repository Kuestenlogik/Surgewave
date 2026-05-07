namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Represents a failure domain constraint violation.
/// </summary>
public sealed record FailureDomainViolation
{
    /// <summary>
    /// The type of violation.
    /// </summary>
    public required ViolationType Type { get; init; }

    /// <summary>
    /// Human-readable description of the violation.
    /// </summary>
    public required string Message { get; init; }

    /// <summary>
    /// The topic affected by this violation.
    /// </summary>
    public string? Topic { get; init; }

    /// <summary>
    /// The partition affected by this violation.
    /// </summary>
    public int? Partition { get; init; }

    /// <summary>
    /// The required number of distinct failure domains.
    /// </summary>
    public int? RequiredDomains { get; init; }

    /// <summary>
    /// The actual number of distinct failure domains in the assignment.
    /// </summary>
    public int? ActualDomains { get; init; }

    /// <summary>
    /// The failure domain level being checked (Rack, Zone, Datacenter, Region).
    /// </summary>
    public FailureDomainLevel? Level { get; init; }
}

/// <summary>
/// Types of failure domain violations.
/// </summary>
public enum ViolationType
{
    /// <summary>
    /// Replicas are not spread across enough failure domains.
    /// </summary>
    InsufficientDomainSpread,

    /// <summary>
    /// Not enough failure domains available to satisfy replication factor.
    /// </summary>
    InsufficientDomainsAvailable,

    /// <summary>
    /// All replicas are in the same failure domain.
    /// </summary>
    SingleDomainReplicas,

    /// <summary>
    /// A placement constraint was violated.
    /// </summary>
    ConstraintViolation
}

/// <summary>
/// Hierarchical failure domain levels.
/// </summary>
public enum FailureDomainLevel
{
    /// <summary>
    /// Rack level (lowest, most granular).
    /// </summary>
    Rack = 0,

    /// <summary>
    /// Zone/Availability Zone level.
    /// </summary>
    Zone = 1,

    /// <summary>
    /// Datacenter level.
    /// </summary>
    Datacenter = 2,

    /// <summary>
    /// Region level (highest, least granular).
    /// </summary>
    Region = 3
}
