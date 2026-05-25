using Kuestenlogik.Surgewave.Plugins;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Api.GraphQL;

/// <summary>
/// Broker plugin for the GraphQL API subsystem.
/// </summary>
public sealed class SurgewaveGraphQLBrokerPlugin : IBrokerPlugin
{
    public string FeatureId => "Surgewave.Api.GraphQL";
    public string DisplayName => "GraphQL API";

    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:GraphQL:Enabled", false);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
        => services.AddSurgewaveGraphQL();

    public void Configure(object host, IServiceProvider services)
    {
        if (host is not WebApplication app) return;

        var configuration = services.GetRequiredService<IConfiguration>();
        var path = configuration.GetValue<string>("Surgewave:GraphQL:Path") ?? "/graphql";

        app.UseWebSockets();
        app.MapSurgewaveGraphQL(path);
    }
}
