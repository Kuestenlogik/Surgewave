using System.Collections.Concurrent;
using System.Text.Json;
using Kuestenlogik.Surgewave.Marketplace.Models;

namespace Kuestenlogik.Surgewave.Marketplace.Services;

/// <summary>
/// File-system backed review store: <c>{dataDir}/reviews/{connectorId}.json</c>
/// holds a JSON array of <see cref="ConnectorReview"/>. The per-connector
/// lock means concurrent writes against different packages don't queue;
/// writes against the same package serialise to avoid lost updates. Matches
/// the existing <c>FileSystemMetadataService</c> pattern — no DB
/// dependency, no daemon process, mirrors the lean Marketplace shape.
/// </summary>
public sealed class FileSystemReviewService : IReviewService
{
    private readonly string _reviewsDir;
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _gates = new(StringComparer.OrdinalIgnoreCase);

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public FileSystemReviewService(string dataDir)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(dataDir);
        _reviewsDir = Path.Combine(dataDir, "reviews");
        Directory.CreateDirectory(_reviewsDir);
    }

    public string ReviewsDirectory => _reviewsDir;

    public async Task<IReadOnlyList<ConnectorReview>> GetReviewsAsync(string connectorId, CancellationToken ct = default)
    {
        ValidateConnectorId(connectorId);
        var path = PathFor(connectorId);
        if (!File.Exists(path)) return [];
        var gate = GateFor(connectorId);
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return await ReadAsync(path, ct).ConfigureAwait(false);
        }
        finally { gate.Release(); }
    }

    public async Task<ConnectorRatingSummary> GetRatingSummaryAsync(string connectorId, CancellationToken ct = default)
    {
        var reviews = await GetReviewsAsync(connectorId, ct).ConfigureAwait(false);
        return Summarise(connectorId, reviews);
    }

    public async Task<ConnectorReview> SubmitReviewAsync(ConnectorReview review, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(review);
        ValidateConnectorId(review.ConnectorId);
        if (review.Rating < 1 || review.Rating > 5)
        {
            throw new ArgumentOutOfRangeException(nameof(review), "Rating must be between 1 and 5.");
        }
        if (string.IsNullOrWhiteSpace(review.Author))
        {
            throw new ArgumentException("Author is required.", nameof(review));
        }

        var canonical = new ConnectorReview
        {
            Id = Guid.NewGuid().ToString("N"), // server always assigns
            ConnectorId = review.ConnectorId,
            Author = review.Author,
            Rating = review.Rating,
            Title = review.Title,
            Comment = review.Comment,
            CreatedAt = DateTimeOffset.UtcNow,
            Tags = review.Tags?.ToList() ?? [],
        };

        await MutateAsync(review.ConnectorId, ct, list => { list.Add(canonical); }).ConfigureAwait(false);
        return canonical;
    }

    public async Task<ConnectorReview?> UpdateReviewAsync(
        string connectorId, string reviewId, int rating, string? title, string? comment, List<string>? tags,
        CancellationToken ct = default)
    {
        ValidateConnectorId(connectorId);
        ArgumentException.ThrowIfNullOrWhiteSpace(reviewId);
        if (rating < 1 || rating > 5)
        {
            throw new ArgumentOutOfRangeException(nameof(rating), "Rating must be between 1 and 5.");
        }

        ConnectorReview? updated = null;
        await MutateAsync(connectorId, ct, list =>
        {
            var index = list.FindIndex(r => r.Id == reviewId);
            if (index < 0) return;
            var existing = list[index];
            updated = new ConnectorReview
            {
                Id = existing.Id,
                ConnectorId = existing.ConnectorId,
                Author = existing.Author,
                Rating = rating,
                Title = title,
                Comment = comment,
                CreatedAt = existing.CreatedAt,
                UpdatedAt = DateTimeOffset.UtcNow,
                HelpfulCount = existing.HelpfulCount,
                Tags = tags ?? existing.Tags,
            };
            list[index] = updated;
        }).ConfigureAwait(false);
        return updated;
    }

    public async Task<bool> DeleteReviewAsync(string connectorId, string reviewId, CancellationToken ct = default)
    {
        ValidateConnectorId(connectorId);
        ArgumentException.ThrowIfNullOrWhiteSpace(reviewId);

        var removed = false;
        await MutateAsync(connectorId, ct, list =>
        {
            removed = list.RemoveAll(r => r.Id == reviewId) > 0;
        }).ConfigureAwait(false);
        return removed;
    }

    public async Task<bool> MarkHelpfulAsync(string connectorId, string reviewId, CancellationToken ct = default)
    {
        ValidateConnectorId(connectorId);
        ArgumentException.ThrowIfNullOrWhiteSpace(reviewId);

        var hit = false;
        await MutateAsync(connectorId, ct, list =>
        {
            var review = list.FirstOrDefault(r => r.Id == reviewId);
            if (review is null) return;
            review.HelpfulCount += 1;
            hit = true;
        }).ConfigureAwait(false);
        return hit;
    }

    public async Task<IReadOnlyList<ConnectorRatingSummary>> GetTopRatedAsync(int limit = 10, CancellationToken ct = default)
    {
        if (limit <= 0) return [];
        if (!Directory.Exists(_reviewsDir)) return [];

        var summaries = new List<ConnectorRatingSummary>();
        foreach (var file in Directory.EnumerateFiles(_reviewsDir, "*.json"))
        {
            ct.ThrowIfCancellationRequested();
            var connectorId = Path.GetFileNameWithoutExtension(file);
            var reviews = await ReadAsync(file, ct).ConfigureAwait(false);
            if (reviews.Count == 0) continue;
            summaries.Add(Summarise(connectorId, reviews));
        }
        return summaries
            .OrderByDescending(s => s.AverageRating)
            .ThenByDescending(s => s.TotalReviews)
            .Take(limit)
            .ToList();
    }

    private async Task MutateAsync(string connectorId, CancellationToken ct, Action<List<ConnectorReview>> mutator)
    {
        var path = PathFor(connectorId);
        var gate = GateFor(connectorId);
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var list = File.Exists(path)
                ? (await ReadAsync(path, ct).ConfigureAwait(false)).ToList()
                : new List<ConnectorReview>();
            mutator(list);
            await WriteAsync(path, list, ct).ConfigureAwait(false);
        }
        finally { gate.Release(); }
    }

    private static async Task<IReadOnlyList<ConnectorReview>> ReadAsync(string path, CancellationToken ct)
    {
        await using var stream = File.OpenRead(path);
        var reviews = await JsonSerializer.DeserializeAsync<List<ConnectorReview>>(stream, JsonOptions, ct).ConfigureAwait(false);
        return reviews ?? [];
    }

    private static async Task WriteAsync(string path, List<ConnectorReview> reviews, CancellationToken ct)
    {
        await using var stream = File.Create(path);
        await JsonSerializer.SerializeAsync(stream, reviews, JsonOptions, ct).ConfigureAwait(false);
    }

    private string PathFor(string connectorId) =>
        Path.Combine(_reviewsDir, $"{connectorId}.json");

    private SemaphoreSlim GateFor(string connectorId) =>
        _gates.GetOrAdd(connectorId, _ => new SemaphoreSlim(1, 1));

    private static ConnectorRatingSummary Summarise(string connectorId, IReadOnlyList<ConnectorReview> reviews)
    {
        if (reviews.Count == 0)
        {
            return new ConnectorRatingSummary { ConnectorId = connectorId };
        }
        var dist = new int[5];
        foreach (var r in reviews) dist[Math.Clamp(r.Rating, 1, 5) - 1] += 1;
        var topTags = reviews
            .SelectMany(r => r.Tags ?? [])
            .Where(t => !string.IsNullOrWhiteSpace(t))
            .GroupBy(t => t, StringComparer.OrdinalIgnoreCase)
            .OrderByDescending(g => g.Count())
            .Select(g => g.Key)
            .Take(5)
            .ToList();
        return new ConnectorRatingSummary
        {
            ConnectorId = connectorId,
            AverageRating = reviews.Average(r => r.Rating),
            TotalReviews = reviews.Count,
            StarDistribution = dist,
            TopTags = topTags,
        };
    }

    private static void ValidateConnectorId(string connectorId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(connectorId);
        // Path.GetInvalidFileNameChars() on Linux only excludes '/' and NUL —
        // '\' passes through. Reject both separators explicitly so a Linux
        // marketplace can't write reviews/with\back.json that would resolve
        // to a path-traversal target on a Windows-side mirror.
        if (connectorId.IndexOfAny(['/', '\\', '\0']) >= 0
            || connectorId.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
        {
            throw new ArgumentException("Connector id must not contain path separators or invalid file chars.", nameof(connectorId));
        }
    }
}
