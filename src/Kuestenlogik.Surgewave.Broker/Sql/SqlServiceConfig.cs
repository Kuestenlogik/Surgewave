using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Broker.Sql;

/// <summary>
/// Configuration for the Surgewave SQL query service.
/// Bound from the Surgewave:Sql configuration section.
/// </summary>
public sealed class SqlServiceConfig : IValidatableConfig
{
    /// <summary>
    /// Configuration section name in appsettings.json.
    /// </summary>
    public const string SectionName = "Surgewave:Sql";

    /// <summary>
    /// Whether the SQL query service is enabled.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Maximum number of concurrent continuous queries that can run simultaneously.
    /// </summary>
    [Range(1, 10_000)]
    public int MaxConcurrentQueries { get; set; } = 16;

    /// <summary>
    /// Default limit for query results if none specified in the SQL.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int DefaultResultLimit { get; set; } = 1000;

    /// <summary>
    /// Maximum number of messages to read from a topic for a single query.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxMessagesPerQuery { get; set; } = 100_000;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (DefaultResultLimit > MaxMessagesPerQuery)
        {
            errors.Add($"{nameof(DefaultResultLimit)} ({DefaultResultLimit}) must not exceed " +
                       $"{nameof(MaxMessagesPerQuery)} ({MaxMessagesPerQuery}).");
        }

        return errors;
    }
}
