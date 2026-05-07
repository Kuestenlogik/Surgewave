using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Schema.Registry.Migration;

/// <summary>
/// Intercepts produce and fetch operations to transparently migrate messages
/// between schema versions. This enables zero-downtime schema evolution where
/// consumers do not need to be restarted when schemas change.
/// </summary>
public sealed class SchemaMigrationInterceptor
{
    private readonly SchemaMigrator _migrator;
    private readonly SchemaMigrationCache _cache;
    private readonly SchemaMigrationConfig _config;
    private readonly ISchemaStore _store;
    private readonly ILogger<SchemaMigrationInterceptor> _logger;

    private long _readMigrations;
    private long _writeMigrations;
    private long _errors;

    /// <summary>
    /// Total number of migrations performed on read.
    /// </summary>
    public long ReadMigrations => Interlocked.Read(ref _readMigrations);

    /// <summary>
    /// Total number of migrations performed on write.
    /// </summary>
    public long WriteMigrations => Interlocked.Read(ref _writeMigrations);

    /// <summary>
    /// Total number of migration errors.
    /// </summary>
    public long Errors => Interlocked.Read(ref _errors);

    public SchemaMigrationInterceptor(
        SchemaMigrator migrator,
        SchemaMigrationCache cache,
        SchemaMigrationConfig config,
        ISchemaStore store,
        ILogger<SchemaMigrationInterceptor> logger)
    {
        _migrator = migrator;
        _cache = cache;
        _config = config;
        _store = store;
        _logger = logger;
    }

    /// <summary>
    /// Migrate a message on read (fetch) from the message's schema version
    /// to the consumer's expected schema version.
    /// </summary>
    /// <param name="message">The raw message bytes.</param>
    /// <param name="subject">The schema subject name.</param>
    /// <param name="messageVersion">The schema version the message was produced with.</param>
    /// <param name="consumerVersion">The schema version the consumer expects.</param>
    /// <returns>The migrated message bytes, or the original if no migration is needed.</returns>
    public byte[] MigrateOnRead(byte[] message, string subject, int messageVersion, int consumerVersion)
    {
        if (!_config.AutoMigrateOnRead || messageVersion == consumerVersion)
        {
            return message;
        }

        try
        {
            // Check cache first
            var cachedMigrator = _cache.GetMigrator(subject, messageVersion, consumerVersion);
            if (cachedMigrator is not null)
            {
                Interlocked.Increment(ref _readMigrations);
                return cachedMigrator(message);
            }

            // Build and cache the migrator
            var fromSchema = _store.GetSchema(subject, messageVersion);
            var toSchema = _store.GetSchema(subject, consumerVersion);

            if (fromSchema is null || toSchema is null)
            {
                _logger.LogWarning(
                    "Cannot migrate {Subject} v{From}->v{To}: schema version not found",
                    subject, messageVersion, consumerVersion);
                return message;
            }

            var migrator = _migrator.BuildMigrator(fromSchema.SchemaString, toSchema.SchemaString, _config);
            _cache.CacheMigrator(subject, messageVersion, consumerVersion, migrator);

            Interlocked.Increment(ref _readMigrations);
            return migrator(message);
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _errors);
            _logger.LogError(ex,
                "Failed to migrate message for {Subject} v{From}->v{To}",
                subject, messageVersion, consumerVersion);
            return message; // Return original on error
        }
    }

    /// <summary>
    /// Migrate a message on write (produce) to the latest schema version.
    /// </summary>
    /// <param name="message">The raw message bytes.</param>
    /// <param name="subject">The schema subject name.</param>
    /// <param name="messageVersion">The schema version of the produced message.</param>
    /// <returns>The migrated message bytes, or the original if no migration is needed.</returns>
    public byte[] MigrateOnWrite(byte[] message, string subject, int messageVersion)
    {
        if (!_config.AutoMigrateOnWrite)
        {
            return message;
        }

        var latestSchema = _store.GetLatestSchema(subject);
        if (latestSchema is null || latestSchema.Version == messageVersion)
        {
            return message;
        }

        try
        {
            var cachedMigrator = _cache.GetMigrator(subject, messageVersion, latestSchema.Version);
            if (cachedMigrator is not null)
            {
                Interlocked.Increment(ref _writeMigrations);
                return cachedMigrator(message);
            }

            var fromSchema = _store.GetSchema(subject, messageVersion);
            if (fromSchema is null)
            {
                _logger.LogWarning(
                    "Cannot migrate {Subject} v{From}->v{To}: source schema not found",
                    subject, messageVersion, latestSchema.Version);
                return message;
            }

            var migrator = _migrator.BuildMigrator(fromSchema.SchemaString, latestSchema.SchemaString, _config);
            _cache.CacheMigrator(subject, messageVersion, latestSchema.Version, migrator);

            Interlocked.Increment(ref _writeMigrations);
            return migrator(message);
        }
        catch (Exception ex)
        {
            Interlocked.Increment(ref _errors);
            _logger.LogError(ex,
                "Failed to migrate message on write for {Subject} v{From}->latest",
                subject, messageVersion);
            return message;
        }
    }
}
