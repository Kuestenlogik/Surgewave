using Kuestenlogik.Surgewave.Control.Models.Marketplace;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Service for managing connector ratings and reviews.
/// </summary>
public interface IReviewService
{
    /// <summary>
    /// Get all reviews for a specific connector.
    /// </summary>
    Task<IReadOnlyList<ConnectorReview>> GetReviewsAsync(string connectorId);

    /// <summary>
    /// Get the aggregated rating summary for a connector.
    /// </summary>
    Task<ConnectorRatingSummary> GetRatingSummaryAsync(string connectorId);

    /// <summary>
    /// Submit a new review.
    /// </summary>
    Task<ConnectorReview> SubmitReviewAsync(ConnectorReview review);

    /// <summary>
    /// Update an existing review's rating and comment.
    /// </summary>
    Task UpdateReviewAsync(string reviewId, int rating, string? title, string? comment, List<string>? tags);

    /// <summary>
    /// Delete a review by its ID.
    /// </summary>
    Task DeleteReviewAsync(string reviewId);

    /// <summary>
    /// Increment the helpful count for a review.
    /// </summary>
    Task MarkHelpfulAsync(string reviewId);

    /// <summary>
    /// Get the top-rated connectors.
    /// </summary>
    Task<IReadOnlyList<ConnectorRatingSummary>> GetTopRatedAsync(int limit = 10);
}
