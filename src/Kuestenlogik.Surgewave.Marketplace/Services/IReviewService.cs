using Kuestenlogik.Surgewave.Marketplace.Models;

namespace Kuestenlogik.Surgewave.Marketplace.Services;

/// <summary>
/// Backend store for connector reviews — replaces the per-browser
/// LocalStorage backing the Control UI had before. One file per
/// connector, written under the Marketplace's data directory.
/// </summary>
public interface IReviewService
{
    Task<IReadOnlyList<ConnectorReview>> GetReviewsAsync(string connectorId, CancellationToken ct = default);

    Task<ConnectorRatingSummary> GetRatingSummaryAsync(string connectorId, CancellationToken ct = default);

    /// <summary>
    /// Persists a new review. The supplied <c>Id</c> is ignored — the server
    /// always assigns a fresh GUID so client-side replays don't collide.
    /// Returns the canonical, server-side representation.
    /// </summary>
    Task<ConnectorReview> SubmitReviewAsync(ConnectorReview review, CancellationToken ct = default);

    Task<ConnectorReview?> UpdateReviewAsync(
        string connectorId, string reviewId, int rating, string? title, string? comment, List<string>? tags,
        CancellationToken ct = default);

    Task<bool> DeleteReviewAsync(string connectorId, string reviewId, CancellationToken ct = default);

    Task<bool> MarkHelpfulAsync(string connectorId, string reviewId, CancellationToken ct = default);

    Task<IReadOnlyList<ConnectorRatingSummary>> GetTopRatedAsync(int limit = 10, CancellationToken ct = default);
}
