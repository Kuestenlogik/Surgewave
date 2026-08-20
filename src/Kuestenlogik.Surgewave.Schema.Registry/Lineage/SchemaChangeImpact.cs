using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry.Lineage;

/// <summary>A pipeline/consumer directly reading the changed topic.</summary>
public sealed record AffectedPipeline(
    [property: JsonPropertyName("name")] string Name,
    [property: JsonPropertyName("kind")] string Kind);

/// <summary>A topic reachable downstream of the changed topic, with its registered subject.</summary>
public sealed record DownstreamTopic(
    [property: JsonPropertyName("topic")] string Topic,
    [property: JsonPropertyName("via")] string Via,
    [property: JsonPropertyName("subject")] string? Subject,
    [property: JsonPropertyName("latestVersion")] int? LatestVersion);

/// <summary>
/// The answer to "what breaks if this schema version ships?" (#13): the compatibility verdict
/// for the subject itself, plus — from lineage — every pipeline that reads the topic and every
/// topic transitively downstream of it. An incompatible change does not abstractly "fail the
/// check"; it breaks THESE consumers, and everything below them goes stale.
/// </summary>
public sealed record SchemaChangeImpact
{
    [JsonPropertyName("subject")]
    public required string Subject { get; init; }

    [JsonPropertyName("topic")]
    public required string Topic { get; init; }

    [JsonPropertyName("compatible")]
    public required bool IsCompatible { get; init; }

    [JsonPropertyName("compatibilityErrors")]
    public required IReadOnlyList<string> CompatibilityErrors { get; init; }

    /// <summary>Direct readers of the changed topic — what an incompatible change breaks.</summary>
    [JsonPropertyName("affectedPipelines")]
    public required IReadOnlyList<AffectedPipeline> AffectedPipelines { get; init; }

    /// <summary>Topics transitively fed from the changed topic — what goes stale when a
    /// pipeline above them breaks.</summary>
    [JsonPropertyName("downstreamTopics")]
    public required IReadOnlyList<DownstreamTopic> DownstreamTopics { get; init; }

    /// <summary>True when no lineage source is registered (standalone registry): the pipeline
    /// lists are empty because nobody could answer, not because nobody reads the topic.</summary>
    [JsonPropertyName("lineageUnavailable")]
    public required bool LineageUnavailable { get; init; }
}
