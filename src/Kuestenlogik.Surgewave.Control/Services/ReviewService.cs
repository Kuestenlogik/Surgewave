using Blazored.LocalStorage;
using Kuestenlogik.Surgewave.Control.Models.Marketplace;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// LocalStorage-backed implementation of <see cref="IReviewService"/>.
/// Reviews are stored per-user in the browser's LocalStorage.
/// </summary>
public sealed class ReviewService(ILocalStorageService localStorage) : IReviewService
{
    private const string StorageKey = "surgewave-connector-reviews";

    public async Task<IReadOnlyList<ConnectorReview>> GetReviewsAsync(string connectorId)
    {
        var allReviews = await LoadReviewsAsync();
        return allReviews
            .Where(r => r.ConnectorId.Equals(connectorId, StringComparison.OrdinalIgnoreCase))
            .OrderByDescending(r => r.HelpfulCount)
            .ThenByDescending(r => r.CreatedAt)
            .ToList();
    }

    public async Task<ConnectorRatingSummary> GetRatingSummaryAsync(string connectorId)
    {
        var reviews = await GetReviewsAsync(connectorId);
        return BuildSummary(connectorId, reviews);
    }

    public async Task<ConnectorReview> SubmitReviewAsync(ConnectorReview review)
    {
        ArgumentNullException.ThrowIfNull(review);

        if (review.Rating is < 1 or > 5)
            throw new ArgumentOutOfRangeException(nameof(review), "Rating must be between 1 and 5.");

        var allReviews = await LoadReviewsAsync();
        allReviews.Add(review);
        await SaveReviewsAsync(allReviews);
        return review;
    }

    public async Task UpdateReviewAsync(string reviewId, int rating, string? title, string? comment, List<string>? tags)
    {
        if (rating is < 1 or > 5)
            throw new ArgumentOutOfRangeException(nameof(rating), "Rating must be between 1 and 5.");

        var allReviews = await LoadReviewsAsync();
        var existing = allReviews.FirstOrDefault(r => r.Id == reviewId)
            ?? throw new InvalidOperationException($"Review '{reviewId}' not found.");

        // Since ConnectorReview uses init-only properties for immutable fields,
        // we replace the review with an updated copy.
        var updated = new ConnectorReview
        {
            Id = existing.Id,
            ConnectorId = existing.ConnectorId,
            Author = existing.Author,
            Rating = rating,
            Title = title ?? existing.Title,
            Comment = comment ?? existing.Comment,
            CreatedAt = existing.CreatedAt,
            UpdatedAt = DateTimeOffset.UtcNow,
            HelpfulCount = existing.HelpfulCount,
            Tags = tags ?? existing.Tags
        };

        allReviews.Remove(existing);
        allReviews.Add(updated);
        await SaveReviewsAsync(allReviews);
    }

    public async Task DeleteReviewAsync(string reviewId)
    {
        var allReviews = await LoadReviewsAsync();
        var review = allReviews.FirstOrDefault(r => r.Id == reviewId);
        if (review != null)
        {
            allReviews.Remove(review);
            await SaveReviewsAsync(allReviews);
        }
    }

    public async Task MarkHelpfulAsync(string reviewId)
    {
        var allReviews = await LoadReviewsAsync();
        var review = allReviews.FirstOrDefault(r => r.Id == reviewId);
        if (review != null)
        {
            review.HelpfulCount++;
            await SaveReviewsAsync(allReviews);
        }
    }

    public async Task<IReadOnlyList<ConnectorRatingSummary>> GetTopRatedAsync(int limit = 10)
    {
        var allReviews = await LoadReviewsAsync();
        return allReviews
            .GroupBy(r => r.ConnectorId, StringComparer.OrdinalIgnoreCase)
            .Select(g => BuildSummary(g.Key, g.ToList()))
            .Where(s => s.TotalReviews > 0)
            .OrderByDescending(s => s.AverageRating)
            .ThenByDescending(s => s.TotalReviews)
            .Take(limit)
            .ToList();
    }

    private async Task<List<ConnectorReview>> LoadReviewsAsync()
    {
        try
        {
            return await localStorage.GetItemAsync<List<ConnectorReview>>(StorageKey) ?? [];
        }
        catch
        {
            return [];
        }
    }

    private async Task SaveReviewsAsync(List<ConnectorReview> reviews)
    {
        await localStorage.SetItemAsync(StorageKey, reviews);
    }

    private static ConnectorRatingSummary BuildSummary(string connectorId, IReadOnlyList<ConnectorReview> reviews)
    {
        var distribution = new int[5];
        foreach (var review in reviews)
        {
            if (review.Rating is >= 1 and <= 5)
                distribution[review.Rating - 1]++;
        }

        var topTags = reviews
            .SelectMany(r => r.Tags)
            .GroupBy(t => t, StringComparer.OrdinalIgnoreCase)
            .OrderByDescending(g => g.Count())
            .Take(5)
            .Select(g => g.Key)
            .ToList();

        return new ConnectorRatingSummary
        {
            ConnectorId = connectorId,
            AverageRating = reviews.Count > 0 ? reviews.Average(r => r.Rating) : 0,
            TotalReviews = reviews.Count,
            StarDistribution = distribution,
            TopTags = topTags
        };
    }
}
