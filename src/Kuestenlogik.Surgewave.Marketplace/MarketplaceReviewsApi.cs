using Kuestenlogik.Surgewave.Marketplace.Models;
using Kuestenlogik.Surgewave.Marketplace.Services;

namespace Kuestenlogik.Surgewave.Marketplace;

/// <summary>
/// REST endpoints for the per-package review/rating store. Mounted at
/// <c>/api/v1/packages/{id}/reviews</c> plus a sibling <c>/api/v1/reviews/top-rated</c>
/// for the marketplace landing page's "top rated" list. Keeps reviews as
/// a strict server-side concern — the Control UI no longer persists them
/// in browser LocalStorage.
/// </summary>
public static class MarketplaceReviewsApi
{
    public static IEndpointRouteBuilder MapMarketplaceReviews(
        this IEndpointRouteBuilder app,
        IReviewService reviews)
    {
        var api = app.MapGroup("/api/v1").WithTags("Marketplace-Reviews");

        api.MapGet("/packages/{id}/reviews", async (string id, CancellationToken ct) =>
        {
            try { return Results.Ok(await reviews.GetReviewsAsync(id, ct)); }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        api.MapGet("/packages/{id}/reviews/summary", async (string id, CancellationToken ct) =>
        {
            try { return Results.Ok(await reviews.GetRatingSummaryAsync(id, ct)); }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        api.MapPost("/packages/{id}/reviews", async (string id, ConnectorReview review, ILogger<IReviewService> logger, CancellationToken ct) =>
        {
            // Route binds the connector id, body's value is ignored to prevent
            // smuggling reviews across packages with one POST.
            var bound = new ConnectorReview
            {
                ConnectorId = id,
                Author = review.Author,
                Rating = review.Rating,
                Title = review.Title,
                Comment = review.Comment,
                Tags = review.Tags ?? [],
            };
            try
            {
                var saved = await reviews.SubmitReviewAsync(bound, ct);
                logger.LogInformation("Review submitted for {ConnectorId} by {Author} ({Rating}*)", saved.ConnectorId, saved.Author, saved.Rating);
                return Results.Created($"/api/v1/packages/{id}/reviews/{saved.Id}", saved);
            }
            catch (ArgumentOutOfRangeException ex) { return Results.BadRequest(new { error = ex.Message }); }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        api.MapPut("/packages/{id}/reviews/{reviewId}", async (
            string id, string reviewId, UpdateReviewRequest body, CancellationToken ct) =>
        {
            try
            {
                var updated = await reviews.UpdateReviewAsync(id, reviewId, body.Rating, body.Title, body.Comment, body.Tags, ct);
                return updated is null
                    ? Results.NotFound(new { error = $"Review '{reviewId}' not found." })
                    : Results.Ok(updated);
            }
            catch (ArgumentOutOfRangeException ex) { return Results.BadRequest(new { error = ex.Message }); }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        api.MapDelete("/packages/{id}/reviews/{reviewId}", async (string id, string reviewId, CancellationToken ct) =>
        {
            try
            {
                var removed = await reviews.DeleteReviewAsync(id, reviewId, ct);
                return removed
                    ? Results.Ok(new { id = reviewId, deleted = true })
                    : Results.NotFound(new { error = $"Review '{reviewId}' not found." });
            }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        api.MapPost("/packages/{id}/reviews/{reviewId}/helpful", async (string id, string reviewId, CancellationToken ct) =>
        {
            try
            {
                var hit = await reviews.MarkHelpfulAsync(id, reviewId, ct);
                return hit
                    ? Results.Ok(new { id = reviewId, helpful = true })
                    : Results.NotFound(new { error = $"Review '{reviewId}' not found." });
            }
            catch (ArgumentException ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        api.MapGet("/reviews/top-rated", async (int? limit, CancellationToken ct) =>
        {
            var n = Math.Clamp(limit ?? 10, 1, 100);
            return Results.Ok(await reviews.GetTopRatedAsync(n, ct));
        });

        return app;
    }

    public sealed record UpdateReviewRequest(int Rating, string? Title, string? Comment, List<string>? Tags);
}
