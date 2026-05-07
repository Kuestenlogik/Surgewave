using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Protocol.PostgreSql;

/// <summary>
/// Configuration for the PostgreSQL wire protocol adapter.
/// Bound from the "Surgewave:PostgreSql" configuration section.
/// </summary>
public sealed class PostgreSqlConfig : IValidatableConfig
{
    /// <summary>
    /// Configuration section name in appsettings.json.
    /// </summary>
    public const string SectionName = "Surgewave:PostgreSql";

    /// <summary>
    /// Enable the PostgreSQL wire protocol adapter. Default: false.
    /// </summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// TCP port to listen for PostgreSQL connections. Default: 5432.
    /// </summary>
    [Range(1, 65535)]
    public int Port { get; set; } = 5432;

    /// <summary>
    /// Require password authentication. When false, all connections are accepted.
    /// Default: false.
    /// </summary>
    public bool RequirePassword { get; set; }

    /// <summary>
    /// Password for cleartext authentication. Only used when <see cref="RequirePassword"/> is true.
    /// </summary>
    public string? Password { get; set; }

    /// <summary>
    /// Maximum number of concurrent PostgreSQL connections. Default: 100.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int MaxConnections { get; set; } = 100;

    /// <summary>
    /// PostgreSQL server version string reported to clients. Default: "16.0".
    /// </summary>
    [Required]
    [MinLength(1)]
    public string ServerVersion { get; set; } = "16.0";

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (RequirePassword && string.IsNullOrEmpty(Password))
            errors.Add($"{nameof(Password)}: required when {nameof(RequirePassword)} is enabled.");

        return errors;
    }
}
