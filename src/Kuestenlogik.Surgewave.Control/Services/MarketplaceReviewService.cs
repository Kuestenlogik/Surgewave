using System.Net;
using System.Net.Http.Json;
using Kuestenlogik.Surgewave.Control.Models.Marketplace;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// HTTP-backed <see cref="IReviewService"/> targeting the Surgewave
/// Marketplace's <c>/api/v1/packages/{id}/reviews</c> surface. Replaces the
/// previous browser-local <c>LocalStorage</c> impl so reviews survive
/// cache-clears, follow the operator across browsers, and are visible to
/// other team members.
///
/// All methods are best-effort: when the marketplace is unreachable, the
/// service degrades to empty results instead of throwing so the
/// <c>/plugins</c> page (which fans out one summary call per package) still
/// renders. Mutating calls surface errors via <see cref="HttpRequestException"/>.
/// </summary>
public sealed class MarketplaceReviewService : IReviewService
{
    private readonly HttpClient _http;
    private readonly ILogger<MarketplaceReviewService> _logger;

    public MarketplaceReviewService(
        HttpClient http,
        MarketplaceConfig config,
        ILogger<MarketplaceReviewService> logger)
    {
        ArgumentNullException.ThrowIfNull(config);
        _http = http;
        _http.BaseAddress = new Uri(config.BaseUrl.TrimEnd('/') + "/");
        _logger = logger;
    }

    public async Task<IReadOnlyList<ConnectorReview>> GetReviewsAsync(string connectorId)
    {
        try
        {
            var list = await _http.GetFromJsonAsync<List<ConnectorReview>>(
                $"api/v1/packages/{Uri.EscapeDataString(connectorId)}/reviews");
            return list ?? [];
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not load reviews for {ConnectorId}", connectorId);
            return [];
        }
    }

    public async Task<ConnectorRatingSummary> GetRatingSummaryAsync(string connectorId)
    {
        try
        {
            var summary = await _http.GetFromJsonAsync<ConnectorRatingSummary>(
                $"api/v1/packages/{Uri.EscapeDataString(connectorId)}/reviews/summary");
            return summary ?? new ConnectorRatingSummary { ConnectorId = connectorId };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not load summary for {ConnectorId}", connectorId);
            return new ConnectorRatingSummary { ConnectorId = connectorId };
        }
    }

    public async Task<ConnectorReview> SubmitReviewAsync(ConnectorReview review)
    {
        ArgumentNullException.ThrowIfNull(review);
        using var resp = await _http.PostAsJsonAsync(
            $"api/v1/packages/{Uri.EscapeDataString(review.ConnectorId)}/reviews", review);
        await EnsureOkAsync(resp, "submit review");
        var saved = await resp.Content.ReadFromJsonAsync<ConnectorReview>();
        return saved ?? throw new InvalidOperationException("Empty submit response.");
    }

    public async Task UpdateReviewAsync(string connectorId, string reviewId, int rating, string? title, string? comment, List<string>? tags)
    {
        var body = new UpdateReviewRequest(rating, title, comment, tags);
        using var resp = await _http.PutAsJsonAsync(
            $"api/v1/packages/{Uri.EscapeDataString(connectorId)}/reviews/{Uri.EscapeDataString(reviewId)}", body);
        await EnsureOkAsync(resp, "update review");
    }

    public async Task DeleteReviewAsync(string connectorId, string reviewId)
    {
        using var resp = await _http.DeleteAsync(
            $"api/v1/packages/{Uri.EscapeDataString(connectorId)}/reviews/{Uri.EscapeDataString(reviewId)}");
        if (resp.StatusCode == HttpStatusCode.NotFound) return; // idempotent
        await EnsureOkAsync(resp, "delete review");
    }

    public async Task MarkHelpfulAsync(string connectorId, string reviewId)
    {
        using var resp = await _http.PostAsync(
            $"api/v1/packages/{Uri.EscapeDataString(connectorId)}/reviews/{Uri.EscapeDataString(reviewId)}/helpful",
            content: null);
        if (resp.StatusCode == HttpStatusCode.NotFound) return;
        await EnsureOkAsync(resp, "mark helpful");
    }

    public async Task<IReadOnlyList<ConnectorRatingSummary>> GetTopRatedAsync(int limit = 10)
    {
        try
        {
            var list = await _http.GetFromJsonAsync<List<ConnectorRatingSummary>>(
                $"api/v1/reviews/top-rated?limit={limit}");
            return list ?? [];
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not load top-rated reviews");
            return [];
        }
    }

    private static async Task EnsureOkAsync(HttpResponseMessage resp, string operation)
    {
        if (resp.IsSuccessStatusCode) return;
        var body = await resp.Content.ReadAsStringAsync();
        throw new HttpRequestException($"Marketplace {operation} failed ({(int)resp.StatusCode}): {body}");
    }

    private sealed record UpdateReviewRequest(int Rating, string? Title, string? Comment, List<string>? Tags);
}
