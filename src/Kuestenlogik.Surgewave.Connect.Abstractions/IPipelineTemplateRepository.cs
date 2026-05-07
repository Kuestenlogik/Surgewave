namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Repository for pipeline templates.
/// </summary>
public interface IPipelineTemplateRepository
{
    /// <summary>
    /// Repository name.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Repository source (URL or path).
    /// </summary>
    string Source { get; }

    /// <summary>
    /// Search for templates.
    /// </summary>
    /// <param name="query">Search query (name, tags, description).</param>
    /// <param name="category">Optional category filter.</param>
    /// <param name="skip">Number of results to skip.</param>
    /// <param name="take">Number of results to take.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task<IReadOnlyList<PipelineTemplateInfo>> SearchAsync(
        string? query = null,
        string? category = null,
        int skip = 0,
        int take = 20,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get a specific template by ID.
    /// </summary>
    Task<PipelineTemplateInfo?> GetAsync(
        string templateId,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get all available categories.
    /// </summary>
    Task<IReadOnlyList<string>> GetCategoriesAsync(
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Extended template information with marketplace metadata.
/// </summary>
public record PipelineTemplateInfo
{
    /// <summary>
    /// Template identifier.
    /// </summary>
    public required string Id { get; init; }

    /// <summary>
    /// Template display name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Template description.
    /// </summary>
    public required string Description { get; init; }

    /// <summary>
    /// Template category.
    /// </summary>
    public required string Category { get; init; }

    /// <summary>
    /// Template icon (Material icon name).
    /// </summary>
    public string? Icon { get; init; }

    /// <summary>
    /// Template author.
    /// </summary>
    public string? Author { get; init; }

    /// <summary>
    /// Template version.
    /// </summary>
    public string Version { get; init; } = "1.0.0";

    /// <summary>
    /// Tags for discovery.
    /// </summary>
    public IReadOnlyList<string> Tags { get; init; } = [];

    /// <summary>
    /// Download/usage count.
    /// </summary>
    public long Downloads { get; init; }

    /// <summary>
    /// Average rating (1-5).
    /// </summary>
    public double? Rating { get; init; }

    /// <summary>
    /// Number of ratings.
    /// </summary>
    public int RatingCount { get; init; }

    /// <summary>
    /// Publication date.
    /// </summary>
    public DateTimeOffset? PublishedAt { get; init; }

    /// <summary>
    /// Last update date.
    /// </summary>
    public DateTimeOffset? UpdatedAt { get; init; }

    /// <summary>
    /// Whether this template is installed locally.
    /// </summary>
    public bool IsInstalled { get; init; }

    /// <summary>
    /// Source repository name.
    /// </summary>
    public string? SourceRepository { get; init; }

    /// <summary>
    /// The pipeline definition.
    /// </summary>
    public PipelineExportData? Pipeline { get; init; }
}
