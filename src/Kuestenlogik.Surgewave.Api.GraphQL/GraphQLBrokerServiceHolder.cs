using Kuestenlogik.Surgewave.Api.GraphQL.Services;

namespace Kuestenlogik.Surgewave.Api.GraphQL;

/// <summary>
/// Holder for late-binding the <see cref="IGraphQLBrokerService"/> implementation.
/// Required because the service depends on LogManager and RecordBatchSerializer
/// which are only available after <c>app.Build()</c>.
/// </summary>
public static class GraphQLBrokerServiceHolder
{
    /// <summary>
    /// The broker service instance, set after the broker initializes its dependencies.
    /// </summary>
    public static IGraphQLBrokerService? Instance { get; set; }
}
