using Kuestenlogik.Surgewave.Control.Models.Marketplace;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Service for managing connector ratings and reviews. Backed by the
/// Marketplace HTTP service at <c>Surgewave:Marketplace:BaseUrl</c> — see
/// <c>MarketplaceReviewService</c>. The connector id is part of every
/// mutating call so the route can address the correct per-package file
/// without a server-side index lookup.
/// </summary>
public interface IReviewService
{
    Task<IReadOnlyList<ConnectorReview>> GetReviewsAsync(string connectorId);

    Task<ConnectorRatingSummary> GetRatingSummaryAsync(string connectorId);

    Task<ConnectorReview> SubmitReviewAsync(ConnectorReview review);

    Task UpdateReviewAsync(string connectorId, string reviewId, int rating, string? title, string? comment, List<string>? tags);

    Task DeleteReviewAsync(string connectorId, string reviewId);

    Task MarkHelpfulAsync(string connectorId, string reviewId);

    Task<IReadOnlyList<ConnectorRatingSummary>> GetTopRatedAsync(int limit = 10);
}
