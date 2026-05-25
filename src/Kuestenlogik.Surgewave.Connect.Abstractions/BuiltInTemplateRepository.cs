namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Repository providing built-in pipeline templates.
/// </summary>
public sealed class BuiltInTemplateRepository : IPipelineTemplateRepository
{
    /// <inheritdoc />
    public string Name => "built-in";

    /// <inheritdoc />
    public string Source => "internal";

    /// <inheritdoc />
    public Task<IReadOnlyList<PipelineTemplateInfo>> SearchAsync(
        string? query = null,
        string? category = null,
        int skip = 0,
        int take = 20,
        CancellationToken cancellationToken = default)
    {
        var templates = PipelineTemplates.All
            .Select(ToInfo)
            .AsEnumerable();

        if (!string.IsNullOrWhiteSpace(query))
        {
            var lowerQuery = query.ToLowerInvariant();
            templates = templates.Where(t =>
                t.Name.Contains(lowerQuery, StringComparison.OrdinalIgnoreCase) ||
                t.Description.Contains(lowerQuery, StringComparison.OrdinalIgnoreCase) ||
                t.Tags.Any(tag => tag.Contains(lowerQuery, StringComparison.OrdinalIgnoreCase)));
        }

        if (!string.IsNullOrWhiteSpace(category))
        {
            templates = templates.Where(t =>
                t.Category.Equals(category, StringComparison.OrdinalIgnoreCase));
        }

        var result = templates
            .Skip(skip)
            .Take(take)
            .ToList();

        return Task.FromResult<IReadOnlyList<PipelineTemplateInfo>>(result);
    }

    /// <inheritdoc />
    public Task<PipelineTemplateInfo?> GetAsync(
        string templateId,
        CancellationToken cancellationToken = default)
    {
        var template = PipelineTemplates.GetById(templateId);
        if (template == null)
        {
            return Task.FromResult<PipelineTemplateInfo?>(null);
        }

        return Task.FromResult<PipelineTemplateInfo?>(ToInfo(template));
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<string>> GetCategoriesAsync(
        CancellationToken cancellationToken = default)
    {
        var categories = PipelineTemplates.All
            .Select(t => t.Category)
            .Distinct()
            .OrderBy(c => c)
            .ToList();

        return Task.FromResult<IReadOnlyList<string>>(categories);
    }

    private static PipelineTemplateInfo ToInfo(PipelineTemplate template)
    {
        return new PipelineTemplateInfo
        {
            Id = template.Id,
            Name = template.Name,
            Description = template.Description,
            Category = template.Category,
            Icon = template.Icon,
            Author = "Surgewave Team",
            Version = "1.0.0",
            Tags = GetTags(template),
            Downloads = 0,
            Rating = null,
            RatingCount = 0,
            PublishedAt = null,
            UpdatedAt = null,
            IsInstalled = true, // Built-in templates are always available
            SourceRepository = "built-in",
            Pipeline = template.Pipeline
        };
    }

    private static string[] GetTags(PipelineTemplate template)
    {
        var tags = new List<string> { template.Category.ToLowerInvariant() };

        // Add tags based on connectors used
        foreach (var node in template.Pipeline.Nodes)
        {
            var connectorName = node.ConnectorType.Split('.').LastOrDefault() ?? "";
            connectorName = connectorName.Replace("Connector", "").Replace("Source", "").Replace("Sink", "");
            if (!string.IsNullOrEmpty(connectorName))
            {
                tags.Add(connectorName.ToLowerInvariant());
            }
        }

        return tags.Distinct().ToArray();
    }
}
