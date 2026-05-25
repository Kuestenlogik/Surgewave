using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Cdc;

/// <summary>
/// Configuration for a CDC source connection.
/// </summary>
public sealed class CdcConfig : IValidatableConfig
{
    /// <summary>
    /// Configuration section name in appsettings.json.
    /// </summary>
    public const string SectionName = "Surgewave:Cdc";

    /// <summary>
    /// PostgreSQL connection string.
    /// Must include replication=database for logical replication.
    /// </summary>
    public string ConnectionString { get; set; } = "";

    /// <summary>
    /// Name of the replication slot to use or create.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string SlotName { get; set; } = "surgewave_cdc";

    /// <summary>
    /// Name of the PostgreSQL publication to use or create.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string PublicationName { get; set; } = "surgewave_publication";

    /// <summary>
    /// List of tables to capture changes from.
    /// Empty list means all tables in the publication.
    /// Format: "schema.table" or just "table" (defaults to "public" schema).
    /// </summary>
    public List<string> Tables { get; set; } = [];

    /// <summary>
    /// Prefix for Surgewave topic names. Topics are named: {TopicPrefix}{schema}.{table}
    /// For example, with prefix "cdc." and table "public.orders", the topic is "cdc.public.orders".
    /// </summary>
    public string TopicPrefix { get; set; } = "cdc.";

    /// <summary>
    /// Include the database schema name in the topic name.
    /// When true: cdc.public.orders. When false: cdc.orders.
    /// </summary>
    public bool IncludeSchema { get; set; } = true;

    /// <summary>
    /// Whether to capture a snapshot of existing data on startup.
    /// This reads the current state of all tracked tables before streaming changes.
    /// </summary>
    public bool SnapshotOnStart { get; set; } = false;

    /// <summary>
    /// Interval in seconds for acknowledging LSN progress to PostgreSQL.
    /// Lower values reduce replay on restart but increase network overhead.
    /// </summary>
    [Range(1, 3600)]
    public int AckIntervalSeconds { get; set; } = 10;

    /// <summary>
    /// Whether CDC is enabled. When false, the CDC service does not start.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        // ConnectionString is only required when CDC is actually enabled
        if (Enabled && string.IsNullOrWhiteSpace(ConnectionString))
        {
            errors.Add($"{nameof(ConnectionString)}: required when CDC is enabled.");
        }

        if (Tables.Any(string.IsNullOrWhiteSpace))
        {
            errors.Add($"{nameof(Tables)}: must not contain empty entries.");
        }

        return errors;
    }
}
