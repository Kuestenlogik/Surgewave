using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Protocol.PostgreSql;

/// <summary>
/// Protocol plugin for the PostgreSQL wire protocol adapter.
/// Allows any PostgreSQL client (psql, pgAdmin, JDBC, etc.) to connect
/// and query Surgewave topics using SQL. Runs as a BackgroundService with
/// its own TCP listener on the configured port (default 5432).
///
/// Also activates the Materialized Views subsystem, which enables
/// <c>CREATE MATERIALIZED VIEW name AS SELECT ...</c> against streaming
/// topics. The view body is re-evaluated periodically by a background
/// refresh loop and can be queried via <c>SELECT * FROM name</c>.
/// </summary>
public sealed class SurgewavePostgreSqlProtocolPlugin : IProtocolPlugin
{
    public string FeatureId => "Surgewave.Protocol.PostgreSQL";
    public string DisplayName => "PostgreSQL Wire Protocol";
    public int DefaultPort => 5432;

    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:PostgreSql:Enabled", false);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        services.AddSurgewavePostgreSql(configuration);
        services.AddSurgewaveMaterializedViews(configuration);
    }
}
