namespace Kuestenlogik.Surgewave.Control.Models.Marketplace;

/// <summary>
/// Configuration for marketplace features like ratings and reviews.
/// </summary>
public sealed class MarketplaceConfig
{
    /// <summary>
    /// Enable or disable the rating and reviews feature.
    /// When disabled, rating stars and review forms are hidden from the UI.
    /// </summary>
    public bool ReviewsEnabled { get; set; } = true;
}
