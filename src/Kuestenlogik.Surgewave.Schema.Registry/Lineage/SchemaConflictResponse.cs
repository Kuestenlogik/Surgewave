using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry.Lineage;

/// <summary>
/// The 409 body for an incompatible registration, extended with impact data (#13). The first
/// two properties keep the exact wire names of <see cref="ErrorResponse"/> (error_code,
/// message), so Confluent-compatible clients that parse only those keep working — the impact
/// lists are additive fields such clients ignore.
/// </summary>
public sealed record SchemaConflictResponse
{
    [JsonPropertyName("error_code")]
    public required int ErrorCode { get; init; }

    [JsonPropertyName("message")]
    public required string Message { get; init; }

    [JsonPropertyName("affectedPipelines")]
    public required IReadOnlyList<AffectedPipeline> AffectedPipelines { get; init; }

    [JsonPropertyName("downstreamTopics")]
    public required IReadOnlyList<DownstreamTopic> DownstreamTopics { get; init; }
}
