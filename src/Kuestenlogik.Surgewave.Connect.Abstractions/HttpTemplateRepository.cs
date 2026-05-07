using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Repository for pipeline templates from HTTP/REST API or static index.
/// </summary>
[SuppressMessage("Design", "CA1054:URI parameters should not be strings", Justification = "URLs are dynamically constructed")]
[SuppressMessage("Design", "CA2234:Pass System.Uri objects instead of strings", Justification = "URLs are dynamically constructed")]
public sealed class HttpTemplateRepository : IPipelineTemplateRepository, IDisposable
{
    private readonly HttpClient _httpClient;
    private readonly bool _ownsHttpClient;
    private List<PipelineTemplateInfo>? _cachedTemplates;
    private DateTime _cacheExpiry = DateTime.MinValue;
    private static readonly TimeSpan CacheDuration = TimeSpan.FromMinutes(5);

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    /// Creates a new HTTP template repository.
    /// </summary>
    /// <param name="name">Repository name.</param>
    /// <param name="source">Base URL of the repository.</param>
    /// <param name="httpClient">Optional HTTP client to use.</param>
    public HttpTemplateRepository(
        string name,
        string source,
        HttpClient? httpClient = null)
    {
        Name = name;
        Source = source.TrimEnd('/');
        _ownsHttpClient = httpClient == null;
        _httpClient = httpClient ?? new HttpClient();
    }

    /// <inheritdoc />
    public string Name { get; }

    /// <inheritdoc />
    public string Source { get; }

    /// <inheritdoc />
    public async Task<IReadOnlyList<PipelineTemplateInfo>> SearchAsync(
        string? query = null,
        string? category = null,
        int skip = 0,
        int take = 20,
        CancellationToken cancellationToken = default)
    {
        var templates = await GetAllTemplatesAsync(cancellationToken);

        var filtered = templates.AsEnumerable();

        if (!string.IsNullOrWhiteSpace(query))
        {
            var lowerQuery = query.ToLowerInvariant();
            filtered = filtered.Where(t =>
                t.Name.Contains(lowerQuery, StringComparison.OrdinalIgnoreCase) ||
                t.Description.Contains(lowerQuery, StringComparison.OrdinalIgnoreCase) ||
                t.Tags.Any(tag => tag.Contains(lowerQuery, StringComparison.OrdinalIgnoreCase)));
        }

        if (!string.IsNullOrWhiteSpace(category))
        {
            filtered = filtered.Where(t =>
                t.Category.Equals(category, StringComparison.OrdinalIgnoreCase));
        }

        return filtered
            .Skip(skip)
            .Take(take)
            .Select(t => t with { SourceRepository = Name })
            .ToList();
    }

    /// <inheritdoc />
    public async Task<PipelineTemplateInfo?> GetAsync(
        string templateId,
        CancellationToken cancellationToken = default)
    {
        // First check cache
        var templates = await GetAllTemplatesAsync(cancellationToken);
        var template = templates.FirstOrDefault(t =>
            t.Id.Equals(templateId, StringComparison.OrdinalIgnoreCase));

        if (template == null)
        {
            return null;
        }

        // If pipeline data is not included, fetch it
        if (template.Pipeline == null)
        {
            template = await FetchTemplateDetailsAsync(templateId, cancellationToken) ?? template;
        }

        return template with { SourceRepository = Name };
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<string>> GetCategoriesAsync(
        CancellationToken cancellationToken = default)
    {
        var templates = await GetAllTemplatesAsync(cancellationToken);

        return templates
            .Select(t => t.Category)
            .Distinct()
            .OrderBy(c => c)
            .ToList();
    }

    private async Task<List<PipelineTemplateInfo>> GetAllTemplatesAsync(CancellationToken cancellationToken)
    {
        // Check cache
        if (_cachedTemplates != null && DateTime.UtcNow < _cacheExpiry)
        {
            return _cachedTemplates;
        }

        var indexUrl = $"{Source}/templates.json";

        try
        {
            var response = await _httpClient.GetAsync(indexUrl, cancellationToken);
            response.EnsureSuccessStatusCode();

            var templates = await response.Content.ReadFromJsonAsync<List<PipelineTemplateInfo>>(JsonOptions, cancellationToken);
            _cachedTemplates = templates ?? [];
            _cacheExpiry = DateTime.UtcNow.Add(CacheDuration);

            return _cachedTemplates;
        }
        catch (HttpRequestException)
        {
            return [];
        }
    }

    private async Task<PipelineTemplateInfo?> FetchTemplateDetailsAsync(
        string templateId,
        CancellationToken cancellationToken)
    {
        var url = $"{Source}/templates/{templateId}.json";

        try
        {
            var response = await _httpClient.GetAsync(url, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                return null;
            }

            return await response.Content.ReadFromJsonAsync<PipelineTemplateInfo>(JsonOptions, cancellationToken);
        }
        catch (HttpRequestException)
        {
            return null;
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_ownsHttpClient)
        {
            _httpClient.Dispose();
        }
    }
}
