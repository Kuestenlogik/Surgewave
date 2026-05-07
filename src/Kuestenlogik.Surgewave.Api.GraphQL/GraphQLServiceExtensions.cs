using Kuestenlogik.Surgewave.Api.GraphQL.Mutation;
using Kuestenlogik.Surgewave.Api.GraphQL.Query;
using Kuestenlogik.Surgewave.Api.GraphQL.Services;
using Kuestenlogik.Surgewave.Api.GraphQL.Subscription;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

namespace Kuestenlogik.Surgewave.Api.GraphQL;

/// <summary>
/// Extension methods to register Surgewave GraphQL services and map endpoints.
/// </summary>
public static class GraphQLServiceExtensions
{
    /// <summary>
    /// Adds HotChocolate GraphQL services with Surgewave query, mutation, and subscription types.
    /// Also registers the <see cref="IGraphQLBrokerService"/> via <see cref="GraphQLBrokerServiceHolder"/>.
    /// Call <see cref="MapSurgewaveGraphQL"/> after building the app to map the endpoint.
    /// </summary>
    public static IServiceCollection AddSurgewaveGraphQL(this IServiceCollection services)
    {
        // Register the broker service via holder (late-bound after app.Build)
        services.AddSingleton<IGraphQLBrokerService>(_ =>
            GraphQLBrokerServiceHolder.Instance
            ?? throw new InvalidOperationException("GraphQLBrokerService not initialized. Ensure GraphQLBrokerServiceHolder.Instance is set before handling requests."));

        services
            .AddGraphQLServer()
            .AddQueryType<SurgewaveQuery>()
            .AddMutationType<SurgewaveMutation>()
            .AddSubscriptionType<SurgewaveSubscription>()
            .AddInMemorySubscriptions();

        return services;
    }

    /// <summary>
    /// Maps the GraphQL endpoint (default: <c>/graphql</c>).
    /// Includes WebSocket support for subscriptions and the Banana Cake Pop GraphQL IDE.
    /// </summary>
    public static IEndpointRouteBuilder MapSurgewaveGraphQL(this IEndpointRouteBuilder endpoints, string path = "/graphql")
    {
        endpoints.MapGraphQL(path);
        return endpoints;
    }
}
