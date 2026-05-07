namespace Kuestenlogik.Surgewave.Control.Models.Marketplace;

/// <summary>
/// Aggregated rating summary for a connector.
/// </summary>
public sealed class ConnectorRatingSummary
{
    /// <summary>
    /// The connector package ID.
    /// </summary>
    public required string ConnectorId { get; init; }

    /// <summary>
    /// Average star rating across all reviews.
    /// </summary>
    public double AverageRating { get; init; }

    /// <summary>
    /// Total number of reviews submitted.
    /// </summary>
    public int TotalReviews { get; init; }

    /// <summary>
    /// Distribution of star ratings. Index 0 = 1-star, index 4 = 5-star.
    /// </summary>
    public int[] StarDistribution { get; init; } = new int[5];

    /// <summary>
    /// Most frequently used tags across reviews.
    /// </summary>
    public List<string> TopTags { get; init; } = [];
}
