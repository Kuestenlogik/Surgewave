namespace Kuestenlogik.Surgewave.Marketplace.Models;

/// <summary>
/// Server-side representation of a connector review. Mirrors the JSON shape
/// of <c>Kuestenlogik.Surgewave.Control.Models.Marketplace.ConnectorReview</c>
/// — the Control UI binds this contract over HTTP via its own model class
/// (intentional duplication; the HTTP boundary is the contract, not a shared
/// assembly).
/// </summary>
public sealed class ConnectorReview
{
    public string Id { get; init; } = Guid.NewGuid().ToString("N");
    public required string ConnectorId { get; init; }
    public required string Author { get; init; }
    public required int Rating { get; init; }
    public string? Title { get; init; }
    public string? Comment { get; init; }
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? UpdatedAt { get; set; }
    public int HelpfulCount { get; set; }
    public List<string> Tags { get; init; } = [];
}

/// <summary>
/// Aggregated rating snapshot — server returns these from the top-rated
/// endpoint and the per-package summary endpoint so the UI doesn't have to
/// download the full review list just to render a star strip.
/// </summary>
public sealed class ConnectorRatingSummary
{
    public required string ConnectorId { get; init; }
    public double AverageRating { get; init; }
    public int TotalReviews { get; init; }
    public int[] StarDistribution { get; init; } = new int[5];
    public List<string> TopTags { get; init; } = [];
}
