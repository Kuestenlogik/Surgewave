using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// In-memory schema store with optional file-based persistence.
/// </summary>
public sealed class SchemaStore : ISchemaStore, IDisposable
{
    private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

    private readonly ILogger<SchemaStore> _logger;
    private readonly string? _dataPath;
    private readonly ReaderWriterLockSlim _lock = new();

    // Global schema ID counter
    private int _nextSchemaId = 1;

    // Schema ID -> Schema
    private readonly ConcurrentDictionary<int, Schema> _schemasById = new();

    // Schema hash -> Schema ID (for deduplication)
    private readonly ConcurrentDictionary<string, int> _schemasByHash = new();

    // Subject -> List of versions
    private readonly ConcurrentDictionary<string, List<Schema>> _subjectVersions = new();

    // Subject -> Config
    private readonly ConcurrentDictionary<string, SubjectConfig> _subjectConfigs = new();

    // Global default compatibility
    private CompatibilityMode _globalCompatibility = CompatibilityMode.Backward;

    public SchemaStore(ILogger<SchemaStore> logger, string? dataPath = null)
    {
        _logger = logger;
        _dataPath = dataPath;

        if (!string.IsNullOrEmpty(dataPath))
        {
            LoadFromDisk();
        }
    }

    /// <summary>
    /// Gets or sets the global default compatibility mode.
    /// </summary>
    public CompatibilityMode GlobalCompatibility
    {
        get => _globalCompatibility;
        set
        {
            _globalCompatibility = value;
            _logger.LogInformation("Global compatibility set to {Compatibility}", value);
            SaveToDisk();
        }
    }

    /// <summary>
    /// Gets all subjects.
    /// </summary>
    public IReadOnlyList<string> GetSubjects(bool includeDeleted = false)
    {
        _lock.EnterReadLock();
        try
        {
            if (includeDeleted)
            {
                return _subjectVersions.Keys.ToList();
            }

            return _subjectVersions.Keys
                .Where(s => !_subjectConfigs.TryGetValue(s, out var config) || !config.IsDeleted)
                .ToList();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Gets all versions for a subject.
    /// </summary>
    public IReadOnlyList<int> GetVersions(string subject, bool includeDeleted = false)
    {
        _lock.EnterReadLock();
        try
        {
            if (!_subjectVersions.TryGetValue(subject, out var versions))
            {
                return [];
            }

            if (_subjectConfigs.TryGetValue(subject, out var config) && config.IsDeleted && !includeDeleted)
            {
                return [];
            }

            return versions.Select(s => s.Version).ToList();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Gets a schema by subject and version.
    /// </summary>
    public Schema? GetSchema(string subject, int version)
    {
        _lock.EnterReadLock();
        try
        {
            if (!_subjectVersions.TryGetValue(subject, out var versions))
            {
                return null;
            }

            return versions.FirstOrDefault(s => s.Version == version);
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Gets the latest schema for a subject.
    /// </summary>
    public Schema? GetLatestSchema(string subject)
    {
        _lock.EnterReadLock();
        try
        {
            if (!_subjectVersions.TryGetValue(subject, out var versions) || versions.Count == 0)
            {
                return null;
            }

            if (_subjectConfigs.TryGetValue(subject, out var config) && config.IsDeleted)
            {
                return null;
            }

            return versions[^1];
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    /// <summary>
    /// Gets a schema by its global ID.
    /// </summary>
    public Schema? GetSchemaById(int id)
    {
        return _schemasById.TryGetValue(id, out var schema) ? schema : null;
    }

    /// <summary>
    /// Registers a new schema under a subject.
    /// </summary>
    public Schema RegisterSchema(string subject, string schemaString, SchemaType schemaType, IReadOnlyList<SchemaReference>? references = null)
    {
        var normalizedSchema = NormalizeSchema(schemaString, schemaType);
        var hash = ComputeSchemaHash(normalizedSchema, schemaType);

        _lock.EnterWriteLock();
        try
        {
            // Check if this exact schema already exists globally
            if (_schemasByHash.TryGetValue(hash, out var existingId))
            {
                var existingSchema = _schemasById[existingId];

                // Check if it's already registered under this subject
                if (_subjectVersions.TryGetValue(subject, out var versions))
                {
                    var existingVersion = versions.FirstOrDefault(s => s.Id == existingId);
                    if (existingVersion != null)
                    {
                        _logger.LogDebug("Schema already registered under {Subject} as version {Version}",
                            subject, existingVersion.Version);
                        return existingVersion;
                    }
                }

                // Register existing schema under new subject
                return RegisterExistingSchemaUnderSubject(subject, existingSchema, references);
            }

            // New schema - assign new ID
            var schemaId = _nextSchemaId++;
            var version = GetNextVersion(subject);

            var schema = new Schema
            {
                Id = schemaId,
                Subject = subject,
                Version = version,
                SchemaType = schemaType,
                SchemaString = normalizedSchema,
                References = references,
                RegisteredAt = DateTimeOffset.UtcNow
            };

            _schemasById[schemaId] = schema;
            _schemasByHash[hash] = schemaId;

            if (!_subjectVersions.TryGetValue(subject, out var subjectVersions))
            {
                subjectVersions = [];
                _subjectVersions[subject] = subjectVersions;
            }
            subjectVersions.Add(schema);

            if (!_subjectConfigs.ContainsKey(subject))
            {
                _subjectConfigs[subject] = new SubjectConfig { Subject = subject };
            }

            _logger.LogInformation("Registered schema {SchemaId} under {Subject} version {Version}",
                schemaId, subject, version);

            SaveToDisk();
            return schema;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private Schema RegisterExistingSchemaUnderSubject(string subject, Schema existingSchema, IReadOnlyList<SchemaReference>? references)
    {
        var version = GetNextVersion(subject);

        var schema = new Schema
        {
            Id = existingSchema.Id,
            Subject = subject,
            Version = version,
            SchemaType = existingSchema.SchemaType,
            SchemaString = existingSchema.SchemaString,
            References = references ?? existingSchema.References,
            RegisteredAt = DateTimeOffset.UtcNow
        };

        if (!_subjectVersions.TryGetValue(subject, out var versions))
        {
            versions = [];
            _subjectVersions[subject] = versions;
        }
        versions.Add(schema);

        if (!_subjectConfigs.ContainsKey(subject))
        {
            _subjectConfigs[subject] = new SubjectConfig { Subject = subject };
        }

        _logger.LogInformation("Registered existing schema {SchemaId} under {Subject} version {Version}",
            schema.Id, subject, version);

        SaveToDisk();
        return schema;
    }

    private int GetNextVersion(string subject)
    {
        if (!_subjectVersions.TryGetValue(subject, out var versions) || versions.Count == 0)
        {
            return 1;
        }
        return versions.Max(s => s.Version) + 1;
    }

    /// <summary>
    /// Deletes a subject and all its versions.
    /// </summary>
    public IReadOnlyList<int> DeleteSubject(string subject, bool permanent = false)
    {
        _lock.EnterWriteLock();
        try
        {
            if (!_subjectVersions.TryGetValue(subject, out var versions))
            {
                return [];
            }

            var deletedVersions = versions.Select(s => s.Version).ToList();

            if (permanent)
            {
                _subjectVersions.TryRemove(subject, out _);
                _subjectConfigs.TryRemove(subject, out _);
            }
            else
            {
                if (!_subjectConfigs.TryGetValue(subject, out var config))
                {
                    config = new SubjectConfig { Subject = subject };
                    _subjectConfigs[subject] = config;
                }
                config.IsDeleted = true;
            }

            _logger.LogInformation("{Action} subject {Subject} with {Count} versions",
                permanent ? "Permanently deleted" : "Soft-deleted", subject, deletedVersions.Count);

            SaveToDisk();
            return deletedVersions;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Deletes a specific version of a subject.
    /// </summary>
    public int? DeleteVersion(string subject, int version, bool permanent = false)
    {
        _lock.EnterWriteLock();
        try
        {
            if (!_subjectVersions.TryGetValue(subject, out var versions))
            {
                return null;
            }

            var schema = versions.FirstOrDefault(s => s.Version == version);
            if (schema == null)
            {
                return null;
            }

            if (permanent)
            {
                versions.Remove(schema);
                if (versions.Count == 0)
                {
                    _subjectVersions.TryRemove(subject, out _);
                }
            }

            _logger.LogInformation("{Action} version {Version} of subject {Subject}",
                permanent ? "Permanently deleted" : "Soft-deleted", version, subject);

            SaveToDisk();
            return version;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Gets the compatibility mode for a subject.
    /// </summary>
    public CompatibilityMode GetCompatibility(string subject)
    {
        if (_subjectConfigs.TryGetValue(subject, out var config))
        {
            return config.Compatibility;
        }
        return _globalCompatibility;
    }

    /// <summary>
    /// Sets the compatibility mode for a subject.
    /// </summary>
    public void SetCompatibility(string subject, CompatibilityMode compatibility)
    {
        _lock.EnterWriteLock();
        try
        {
            if (!_subjectConfigs.TryGetValue(subject, out var config))
            {
                config = new SubjectConfig { Subject = subject };
                _subjectConfigs[subject] = config;
            }
            config.Compatibility = compatibility;

            _logger.LogInformation("Set compatibility for {Subject} to {Compatibility}", subject, compatibility);
            SaveToDisk();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Looks up schema ID by schema string.
    /// </summary>
    public int? LookupSchemaId(string subject, string schemaString, SchemaType schemaType)
    {
        var normalizedSchema = NormalizeSchema(schemaString, schemaType);
        var hash = ComputeSchemaHash(normalizedSchema, schemaType);

        if (_schemasByHash.TryGetValue(hash, out var schemaId))
        {
            // Verify it's registered under this subject
            if (_subjectVersions.TryGetValue(subject, out var versions))
            {
                if (versions.Any(s => s.Id == schemaId))
                {
                    return schemaId;
                }
            }
        }

        return null;
    }

    /// <summary>
    /// Gets all schemas (for a subject) that need to be checked for compatibility.
    /// </summary>
    public IReadOnlyList<Schema> GetSchemasForCompatibilityCheck(string subject, CompatibilityMode mode)
    {
        _lock.EnterReadLock();
        try
        {
            if (!_subjectVersions.TryGetValue(subject, out var versions) || versions.Count == 0)
            {
                return [];
            }

            return mode switch
            {
                CompatibilityMode.Backward or CompatibilityMode.Forward or CompatibilityMode.Full =>
                    [versions[^1]], // Only latest
                CompatibilityMode.BackwardTransitive or CompatibilityMode.ForwardTransitive or CompatibilityMode.FullTransitive =>
                    versions.ToList(), // All versions
                _ => []
            };
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private static string NormalizeSchema(string schemaString, SchemaType schemaType)
    {
        // Normalize by parsing and re-serializing to remove whitespace differences
        try
        {
            return schemaType switch
            {
                SchemaType.Avro or SchemaType.Json => NormalizeJsonSchema(schemaString),
                SchemaType.Protobuf => schemaString.Trim(), // Proto normalization is more complex
                _ => schemaString
            };
        }
        catch
        {
            return schemaString;
        }
    }

    private static string NormalizeJsonSchema(string schemaString)
    {
        try
        {
            using var doc = JsonDocument.Parse(schemaString);
            return JsonSerializer.Serialize(doc.RootElement);
        }
        catch
        {
            return schemaString;
        }
    }

    private static string ComputeSchemaHash(string schemaString, SchemaType schemaType)
    {
        var input = $"{schemaType}:{schemaString}";
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexString(bytes);
    }

    private void SaveToDisk()
    {
        if (string.IsNullOrEmpty(_dataPath))
        {
            return;
        }

        try
        {
            Directory.CreateDirectory(_dataPath);

            var state = new SchemaStoreState
            {
                NextSchemaId = _nextSchemaId,
                GlobalCompatibility = _globalCompatibility,
                Schemas = _schemasById.Values.ToList(),
                SubjectConfigs = _subjectConfigs.Values.ToList()
            };

            var json = JsonSerializer.Serialize(state, s_jsonOptions);
            File.WriteAllText(Path.Combine(_dataPath, "schemas.json"), json);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to save schema store to disk");
        }
    }

    private void LoadFromDisk()
    {
        if (string.IsNullOrEmpty(_dataPath))
        {
            return;
        }

        var filePath = Path.Combine(_dataPath, "schemas.json");
        if (!File.Exists(filePath))
        {
            return;
        }

        try
        {
            var json = File.ReadAllText(filePath);
            var state = JsonSerializer.Deserialize<SchemaStoreState>(json);

            if (state == null)
            {
                return;
            }

            _nextSchemaId = state.NextSchemaId;
            _globalCompatibility = state.GlobalCompatibility;

            foreach (var schema in state.Schemas)
            {
                _schemasById[schema.Id] = schema;

                var hash = ComputeSchemaHash(schema.SchemaString, schema.SchemaType);
                _schemasByHash.TryAdd(hash, schema.Id);

                if (!_subjectVersions.TryGetValue(schema.Subject, out var versions))
                {
                    versions = [];
                    _subjectVersions[schema.Subject] = versions;
                }
                versions.Add(schema);
            }

            // Sort versions
            foreach (var versions in _subjectVersions.Values)
            {
                versions.Sort((a, b) => a.Version.CompareTo(b.Version));
            }

            foreach (var config in state.SubjectConfigs)
            {
                _subjectConfigs[config.Subject] = config;
            }

            _logger.LogInformation("Loaded {Count} schemas from disk", state.Schemas.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to load schema store from disk");
        }
    }

    public void Dispose()
    {
        _lock.Dispose();
    }

    private sealed class SchemaStoreState
    {
        public int NextSchemaId { get; set; }
        public CompatibilityMode GlobalCompatibility { get; set; }
        public List<Schema> Schemas { get; set; } = [];
        public List<SubjectConfig> SubjectConfigs { get; set; } = [];
    }
}
