namespace Kuestenlogik.Surgewave.Control.Models.Marketplace;

/// <summary>
/// Represents a user review for a connector in the marketplace.
/// </summary>
public sealed class ConnectorReview
{
    /// <summary>
    /// Unique identifier for this review.
    /// </summary>
    public string Id { get; init; } = Guid.NewGuid().ToString("N");

    /// <summary>
    /// The connector package ID being reviewed.
    /// </summary>
    public required string ConnectorId { get; init; }

    /// <summary>
    /// Display name of the review author.
    /// </summary>
    public required string Author { get; init; }

    /// <summary>
    /// Star rating from 1 (worst) to 5 (best).
    /// </summary>
    public required int Rating { get; init; }

    /// <summary>
    /// Optional review title/headline.
    /// </summary>
    public string? Title { get; init; }

    /// <summary>
    /// Optional detailed review comment.
    /// </summary>
    public string? Comment { get; init; }

    /// <summary>
    /// When the review was created.
    /// </summary>
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// When the review was last updated, if ever.
    /// </summary>
    public DateTimeOffset? UpdatedAt { get; set; }

    /// <summary>
    /// Number of times other users marked this review as helpful.
    /// </summary>
    public int HelpfulCount { get; set; }

    /// <summary>
    /// Categorization tags such as "easy-setup", "reliable", "fast".
    /// </summary>
    public List<string> Tags { get; init; } = [];
}
