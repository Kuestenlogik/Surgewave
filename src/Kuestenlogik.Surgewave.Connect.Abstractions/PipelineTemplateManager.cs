using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Manages pipeline templates from multiple repositories.
/// </summary>
public sealed class PipelineTemplateManager : IDisposable
{
    private readonly List<IPipelineTemplateRepository> _repositories = [];
    private readonly string _localTemplatesDir;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    /// Creates a new template manager.
    /// </summary>
    /// <param name="localTemplatesDir">Directory for locally saved templates.</param>
    public PipelineTemplateManager(string? localTemplatesDir = null)
    {
        _localTemplatesDir = localTemplatesDir ?? GetDefaultTemplatesDir();

        // Always include built-in templates
        _repositories.Add(new BuiltInTemplateRepository());

        // Load local templates repository
        if (Directory.Exists(_localTemplatesDir))
        {
            _repositories.Add(new LocalTemplateRepository(_localTemplatesDir));
        }
    }

    /// <summary>
    /// All registered repositories.
    /// </summary>
    public IReadOnlyList<IPipelineTemplateRepository> Repositories => _repositories;

    /// <summary>
    /// Add a repository.
    /// </summary>
    public void AddRepository(IPipelineTemplateRepository repository)
    {
        _repositories.Add(repository);
    }

    /// <summary>
    /// Add an HTTP repository.
    /// </summary>
    [SuppressMessage("Design", "CA1054:URI-like parameters should not be strings")]
    public void AddHttpRepository(string name, string url)
    {
        _repositories.Add(new HttpTemplateRepository(name, url));
    }

    /// <summary>
    /// Remove a repository by name.
    /// </summary>
    public void RemoveRepository(string name)
    {
        var repo = _repositories.FirstOrDefault(r =>
            r.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

        if (repo != null)
        {
            _repositories.Remove(repo);
            (repo as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// Search all repositories for templates.
    /// </summary>
    public async Task<IReadOnlyList<PipelineTemplateInfo>> SearchAsync(
        string? query = null,
        string? category = null,
        int skip = 0,
        int take = 20,
        CancellationToken cancellationToken = default)
    {
        var results = new List<PipelineTemplateInfo>();
        var seenIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var repo in _repositories)
        {
            try
            {
                var templates = await repo.SearchAsync(query, category, 0, 100, cancellationToken);

                foreach (var template in templates)
                {
                    // Avoid duplicates (prefer first occurrence)
                    if (seenIds.Add(template.Id))
                    {
                        results.Add(template);
                    }
                }
            }
            catch (Exception)
            {
                // Continue with other repositories
            }
        }

        return results
            .OrderByDescending(t => t.Downloads)
            .ThenBy(t => t.Name)
            .Skip(skip)
            .Take(take)
            .ToList();
    }

    /// <summary>
    /// Get a specific template by ID.
    /// </summary>
    public async Task<PipelineTemplateInfo?> GetAsync(
        string templateId,
        CancellationToken cancellationToken = default)
    {
        foreach (var repo in _repositories)
        {
            try
            {
                var template = await repo.GetAsync(templateId, cancellationToken);
                if (template != null)
                {
                    return template;
                }
            }
            catch (Exception)
            {
                // Try next repository
            }
        }

        return null;
    }

    /// <summary>
    /// Get all available categories.
    /// </summary>
    public async Task<IReadOnlyList<string>> GetCategoriesAsync(
        CancellationToken cancellationToken = default)
    {
        var categories = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var repo in _repositories)
        {
            try
            {
                var repoCategories = await repo.GetCategoriesAsync(cancellationToken);
                foreach (var category in repoCategories)
                {
                    categories.Add(category);
                }
            }
            catch (Exception)
            {
                // Continue with other repositories
            }
        }

        return categories.OrderBy(c => c).ToList();
    }

    /// <summary>
    /// Save a template locally.
    /// </summary>
    public async Task SaveLocalAsync(
        PipelineTemplateInfo template,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(_localTemplatesDir);

        var filePath = Path.Combine(_localTemplatesDir, $"{template.Id}.json");
        var json = JsonSerializer.Serialize(template, JsonOptions);
        await File.WriteAllTextAsync(filePath, json, cancellationToken);

        // Refresh local repository
        RefreshLocalRepository();
    }

    /// <summary>
    /// Delete a locally saved template.
    /// </summary>
    public Task DeleteLocalAsync(string templateId)
    {
        var filePath = Path.Combine(_localTemplatesDir, $"{templateId}.json");
        if (File.Exists(filePath))
        {
            File.Delete(filePath);
            RefreshLocalRepository();
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Install a template from a remote repository locally.
    /// </summary>
    public async Task<PipelineTemplateInfo?> InstallAsync(
        string templateId,
        CancellationToken cancellationToken = default)
    {
        var template = await GetAsync(templateId, cancellationToken);
        if (template == null || template.Pipeline == null)
        {
            return null;
        }

        // Save locally
        var localTemplate = template with
        {
            IsInstalled = true,
            SourceRepository = template.SourceRepository
        };

        await SaveLocalAsync(localTemplate, cancellationToken);
        return localTemplate;
    }

    private void RefreshLocalRepository()
    {
        // Remove existing local repository
        var existing = _repositories.FirstOrDefault(r => r is LocalTemplateRepository);
        if (existing != null)
        {
            _repositories.Remove(existing);
        }

        // Add fresh local repository
        if (Directory.Exists(_localTemplatesDir))
        {
            _repositories.Add(new LocalTemplateRepository(_localTemplatesDir));
        }
    }

    private static string GetDefaultTemplatesDir()
    {
        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        return Path.Combine(home, ".surgewave", "templates");
    }

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var repo in _repositories)
        {
            (repo as IDisposable)?.Dispose();
        }
        _repositories.Clear();
    }
}

/// <summary>
/// Repository for locally saved templates.
/// </summary>
internal sealed class LocalTemplateRepository : IPipelineTemplateRepository
{
    private readonly string _templatesDir;
    private readonly List<PipelineTemplateInfo> _templates = [];

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true
    };

    public LocalTemplateRepository(string templatesDir)
    {
        _templatesDir = templatesDir;
        Name = "local";
        Source = templatesDir;
        LoadTemplates();
    }

    public string Name { get; }
    public string Source { get; }

    public Task<IReadOnlyList<PipelineTemplateInfo>> SearchAsync(
        string? query = null,
        string? category = null,
        int skip = 0,
        int take = 20,
        CancellationToken cancellationToken = default)
    {
        var filtered = _templates.AsEnumerable();

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

        var result = filtered
            .Skip(skip)
            .Take(take)
            .Select(t => t with { IsInstalled = true, SourceRepository = "local" })
            .ToList();

        return Task.FromResult<IReadOnlyList<PipelineTemplateInfo>>(result);
    }

    public Task<PipelineTemplateInfo?> GetAsync(
        string templateId,
        CancellationToken cancellationToken = default)
    {
        var template = _templates.FirstOrDefault(t =>
            t.Id.Equals(templateId, StringComparison.OrdinalIgnoreCase));

        if (template != null)
        {
            template = template with { IsInstalled = true, SourceRepository = "local" };
        }

        return Task.FromResult(template);
    }

    public Task<IReadOnlyList<string>> GetCategoriesAsync(
        CancellationToken cancellationToken = default)
    {
        var categories = _templates
            .Select(t => t.Category)
            .Distinct()
            .OrderBy(c => c)
            .ToList();

        return Task.FromResult<IReadOnlyList<string>>(categories);
    }

    private void LoadTemplates()
    {
        _templates.Clear();

        if (!Directory.Exists(_templatesDir))
        {
            return;
        }

        foreach (var file in Directory.GetFiles(_templatesDir, "*.json"))
        {
            try
            {
                var json = File.ReadAllText(file);
                var template = JsonSerializer.Deserialize<PipelineTemplateInfo>(json, JsonOptions);
                if (template != null)
                {
                    _templates.Add(template);
                }
            }
            catch (Exception)
            {
                // Skip invalid files
            }
        }
    }
}
