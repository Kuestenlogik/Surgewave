using Kuestenlogik.Surgewave.Marketplace.Models;
using Kuestenlogik.Surgewave.Marketplace.Services;

namespace Kuestenlogik.Surgewave.Marketplace.Tests.Services;

/// <summary>
/// Round-trips the per-package JSON store: submit / list / update / delete /
/// helpful / summary / top-rated. Each test gets its own temp data dir so
/// they can run in parallel without sharing state.
/// </summary>
public sealed class FileSystemReviewServiceTests : IDisposable
{
    private readonly string _root;
    private readonly FileSystemReviewService _svc;

    public FileSystemReviewServiceTests()
    {
        _root = Directory.CreateTempSubdirectory("surgewave-reviews-").FullName;
        _svc = new FileSystemReviewService(_root);
    }

    public void Dispose()
    {
        if (Directory.Exists(_root)) Directory.Delete(_root, recursive: true);
    }

    private static ConnectorReview Review(string connectorId, int rating = 5, string author = "alice", params string[] tags) =>
        new()
        {
            ConnectorId = connectorId,
            Author = author,
            Rating = rating,
            Title = "looks good",
            Tags = tags.ToList(),
        };

    [Fact]
    public async Task GetReviews_EmptyStore_ReturnsEmpty()
    {
        var list = await _svc.GetReviewsAsync("acme.connector");
        Assert.Empty(list);
    }

    [Fact]
    public async Task SubmitReview_AssignsServerSideId_AndPersists()
    {
        var supplied = new ConnectorReview
        {
            Id = "client-supplied-id",
            ConnectorId = "acme",
            Author = "alice",
            Rating = 5,
        };
        var saved = await _svc.SubmitReviewAsync(supplied);

        Assert.NotEqual("client-supplied-id", saved.Id);
        Assert.NotEmpty(saved.Id);

        var fresh = new FileSystemReviewService(_root);
        var list = await fresh.GetReviewsAsync("acme");
        Assert.Single(list);
        Assert.Equal(saved.Id, list[0].Id);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(6)]
    public async Task SubmitReview_RatingOutOfRange_Throws(int rating)
    {
        var bogus = new ConnectorReview { ConnectorId = "acme", Author = "alice", Rating = rating };
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => _svc.SubmitReviewAsync(bogus));
    }

    [Fact]
    public async Task UpdateReview_ChangesPersistedFields()
    {
        var saved = await _svc.SubmitReviewAsync(Review("acme"));
        var updated = await _svc.UpdateReviewAsync("acme", saved.Id, 3, "meh", "actually it's ok", ["reliable"]);
        Assert.NotNull(updated);
        Assert.Equal(3, updated!.Rating);
        Assert.Equal("meh", updated.Title);
        Assert.NotNull(updated.UpdatedAt);
    }

    [Fact]
    public async Task UpdateReview_UnknownId_ReturnsNull()
    {
        await _svc.SubmitReviewAsync(Review("acme"));
        var result = await _svc.UpdateReviewAsync("acme", "ghost", 4, null, null, null);
        Assert.Null(result);
    }

    [Fact]
    public async Task DeleteReview_KnownId_ReturnsTrueAndRemoves()
    {
        var saved = await _svc.SubmitReviewAsync(Review("acme"));
        Assert.True(await _svc.DeleteReviewAsync("acme", saved.Id));
        Assert.Empty(await _svc.GetReviewsAsync("acme"));
    }

    [Fact]
    public async Task DeleteReview_UnknownId_ReturnsFalse()
    {
        Assert.False(await _svc.DeleteReviewAsync("acme", "never-existed"));
    }

    [Fact]
    public async Task MarkHelpful_IncrementsCount()
    {
        var saved = await _svc.SubmitReviewAsync(Review("acme"));
        Assert.True(await _svc.MarkHelpfulAsync("acme", saved.Id));
        Assert.True(await _svc.MarkHelpfulAsync("acme", saved.Id));
        var list = await _svc.GetReviewsAsync("acme");
        Assert.Equal(2, list.Single().HelpfulCount);
    }

    [Fact]
    public async Task Summary_AveragesDistributionAndTopTags()
    {
        await _svc.SubmitReviewAsync(Review("acme", 5, "alice", "fast", "reliable"));
        await _svc.SubmitReviewAsync(Review("acme", 5, "bob", "fast"));
        await _svc.SubmitReviewAsync(Review("acme", 3, "carol", "buggy"));

        var s = await _svc.GetRatingSummaryAsync("acme");
        Assert.Equal(3, s.TotalReviews);
        Assert.Equal((5 + 5 + 3) / 3.0, s.AverageRating, precision: 3);
        Assert.Equal(0, s.StarDistribution[0]);  // 1-star
        Assert.Equal(1, s.StarDistribution[2]);  // 3-star
        Assert.Equal(2, s.StarDistribution[4]);  // 5-star
        Assert.Equal("fast", s.TopTags[0]);      // most frequent
    }

    [Fact]
    public async Task TopRated_OrdersByAverageThenCount()
    {
        await _svc.SubmitReviewAsync(Review("excellent", 5));
        await _svc.SubmitReviewAsync(Review("mediocre", 3));
        await _svc.SubmitReviewAsync(Review("mediocre", 3));

        var top = await _svc.GetTopRatedAsync(10);
        Assert.Equal("excellent", top[0].ConnectorId);
        Assert.Equal("mediocre", top[1].ConnectorId);
    }

    [Theory]
    [InlineData("with/slash")]
    [InlineData("with\\back")]
    [InlineData("")]
    public async Task SubmitReview_InvalidConnectorId_Throws(string connectorId)
    {
        var bogus = Review(connectorId);
        await Assert.ThrowsAnyAsync<ArgumentException>(() => _svc.SubmitReviewAsync(bogus));
    }
}
