namespace Kuestenlogik.Surgewave.Plugins.Repository;

/// <summary>
/// Broker-side persistence façade over <see cref="RepositoryConfiguration"/>.
/// The plain static <c>Load()/Save()</c> on <see cref="RepositoryConfiguration"/>
/// write to the user's home directory — fine for the CLI, wrong for a long-
/// running broker that owns a single canonical config in its data directory.
/// This service serialises access (mutexed reads + writes) so the REST surface
/// at <c>/api/plugins/repositories</c> can safely mutate from multiple
/// concurrent requests without losing entries.
/// </summary>
public sealed class RepositoryStore
{
    private readonly string _configPath;
    private readonly Lock _gate = new();

    public RepositoryStore(string configPath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(configPath);
        _configPath = configPath;
        Directory.CreateDirectory(Path.GetDirectoryName(configPath) ?? ".");
    }

    public string ConfigPath => _configPath;

    /// <summary>
    /// Last-write time of the underlying config file. Returns
    /// <see cref="DateTime.MinValue"/> when the file has not yet been
    /// materialised — used by <see cref="ConnectorRepositoryManager"/> to
    /// decide whether the in-memory repository list needs to be re-hydrated
    /// from the store (live-resync after REST mutations, no broker restart).
    /// </summary>
    public DateTime LastModifiedUtc =>
        File.Exists(_configPath) ? File.GetLastWriteTimeUtc(_configPath) : DateTime.MinValue;

    /// <summary>
    /// Snapshot of all repositories. Returned list is a copy so callers can
    /// freely enumerate without holding the store's lock.
    /// </summary>
    public IReadOnlyList<RepositoryEntry> List()
    {
        lock (_gate)
        {
            var cfg = LoadOrDefault();
            return cfg.Repositories.Select(Clone).ToList();
        }
    }

    public RepositoryEntry? Get(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        lock (_gate)
        {
            var cfg = LoadOrDefault();
            var entry = cfg.Repositories.FirstOrDefault(r => r.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
            return entry is null ? null : Clone(entry);
        }
    }

    /// <summary>
    /// Adds a new repository entry. Throws <see cref="InvalidOperationException"/>
    /// if a repository with the same name already exists — callers should use
    /// <see cref="Update"/> for in-place edits to avoid silently overwriting.
    /// </summary>
    public RepositoryEntry Add(RepositoryEntry entry)
    {
        ValidateEntry(entry);
        lock (_gate)
        {
            var cfg = LoadOrDefault();
            if (cfg.Repositories.Any(r => r.Name.Equals(entry.Name, StringComparison.OrdinalIgnoreCase)))
            {
                throw new InvalidOperationException(
                    $"A repository named '{entry.Name}' already exists. Use PUT to update or delete it first.");
            }
            cfg.Repositories.Add(Clone(entry));
            cfg.SaveTo(_configPath);
            return Clone(entry);
        }
    }

    /// <summary>
    /// Replaces an existing repository in place (matched by name).
    /// </summary>
    public RepositoryEntry Update(string name, RepositoryEntry entry)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ValidateEntry(entry);
        if (!entry.Name.Equals(name, StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException("Entry name must match the path name. Rename via Delete+Add.", nameof(entry));
        }
        lock (_gate)
        {
            var cfg = LoadOrDefault();
            var index = cfg.Repositories.FindIndex(r => r.Name.Equals(name, StringComparison.OrdinalIgnoreCase));
            if (index < 0)
            {
                throw new KeyNotFoundException($"No repository named '{name}'.");
            }
            cfg.Repositories[index] = Clone(entry);
            cfg.SaveTo(_configPath);
            return Clone(entry);
        }
    }

    /// <summary>Removes a repository by name. Returns false if no such name exists.</summary>
    public bool Remove(string name)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        lock (_gate)
        {
            var cfg = LoadOrDefault();
            var removed = cfg.RemoveRepository(name);
            if (removed) cfg.SaveTo(_configPath);
            return removed;
        }
    }

    private RepositoryConfiguration LoadOrDefault()
    {
        if (!File.Exists(_configPath))
        {
            var seeded = RepositoryConfiguration.CreateDefault();
            seeded.SaveTo(_configPath);
            return seeded;
        }
        return RepositoryConfiguration.LoadFrom(_configPath);
    }

    private static RepositoryEntry Clone(RepositoryEntry e) => new()
    {
        Name = e.Name,
        Type = e.Type,
        Source = e.Source,
        Enabled = e.Enabled,
        PackagePrefix = e.PackagePrefix,
        Credentials = e.Credentials is null ? null : new RepositoryCredentials
        {
            Username = e.Credentials.Username,
            Password = e.Credentials.Password,
            Token = e.Credentials.Token,
        },
    };

    private static void ValidateEntry(RepositoryEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentException.ThrowIfNullOrWhiteSpace(entry.Name);
        ArgumentException.ThrowIfNullOrWhiteSpace(entry.Source);
        if (entry.Name.Length > 128)
        {
            throw new ArgumentException("Repository name must be ≤ 128 characters.", nameof(entry));
        }
        // Reject path separators cross-platform — Path.GetInvalidFileNameChars()
        // on Linux only excludes '/' and NUL; '\' is a legal filename char there,
        // so a Windows-side broker reading a Linux-written config could otherwise
        // see entries like "..\..\etc\passwd". Block both explicitly, then layer
        // the platform-specific invalid-chars on top for everything else.
        if (entry.Name.IndexOfAny(['/', '\\', '\0']) >= 0
            || entry.Name.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
        {
            throw new ArgumentException("Repository name must not contain path separators or invalid file chars.", nameof(entry));
        }
        if (!Uri.TryCreate(entry.Source, UriKind.Absolute, out _))
        {
            throw new ArgumentException($"Source '{entry.Source}' is not an absolute URL.", nameof(entry));
        }
    }
}
