namespace Kuestenlogik.Surgewave.Api.GraphQL.Types;

/// <summary>
/// Represents a consumer group in the GraphQL schema.
/// </summary>
public sealed class ConsumerGroupType
{
    /// <summary>
    /// Consumer group identifier.
    /// </summary>
    public required string GroupId { get; init; }

    /// <summary>
    /// Current state of the group (e.g., Stable, Empty, Rebalancing).
    /// </summary>
    public required string State { get; init; }

    /// <summary>
    /// Number of members in the group.
    /// </summary>
    public int MemberCount { get; init; }

    /// <summary>
    /// Protocol type (e.g., "consumer").
    /// </summary>
    public string? ProtocolType { get; init; }
}
