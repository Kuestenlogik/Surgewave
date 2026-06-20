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

    /// <summary>
    /// Base URL of the Surgewave Marketplace HTTP service. The Control's
    /// HTTP-backed <c>IReviewService</c> targets this URL for review CRUD.
    /// Defaults to the local-dev marketplace address; override in
    /// appsettings under <c>Surgewave:Marketplace:BaseUrl</c>.
    /// </summary>
    public string BaseUrl { get; set; } = "http://localhost:8081";
}
