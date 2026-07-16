using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;

namespace Kuestenlogik.Surgewave.Protocol.WebSocket.Tests;

/// <summary>
/// Bare <see cref="IEndpointRouteBuilder"/> that collects endpoint data sources so tests can
/// inspect mapped routes and invoke their request delegates without hosting a server.
/// </summary>
internal sealed class TestEndpointRouteBuilder : IEndpointRouteBuilder
{
    public TestEndpointRouteBuilder(IServiceProvider serviceProvider) => ServiceProvider = serviceProvider;

    public IServiceProvider ServiceProvider { get; }

    public ICollection<EndpointDataSource> DataSources { get; } = [];

    public IApplicationBuilder CreateApplicationBuilder() => new ApplicationBuilder(ServiceProvider);
}
