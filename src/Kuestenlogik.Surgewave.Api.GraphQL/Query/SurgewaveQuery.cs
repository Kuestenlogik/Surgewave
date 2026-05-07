using HotChocolate;
using Kuestenlogik.Surgewave.Api.GraphQL.Services;
using Kuestenlogik.Surgewave.Api.GraphQL.Types;

namespace Kuestenlogik.Surgewave.Api.GraphQL.Query;

/// <summary>
/// Root GraphQL query type for Surgewave broker.
/// </summary>
public sealed class SurgewaveQuery
{
    /// <summary>
    /// List all topics in the broker.
    /// </summary>
    [GraphQLDescription("List all topics in the broker")]
    public async Task<IReadOnlyList<TopicType>> GetTopics(
        [Service] IGraphQLBrokerService service,
        CancellationToken cancellationToken) =>
        await service.GetTopicsAsync(cancellationToken);

    /// <summary>
    /// Get messages from a topic. Optionally filter by partition and offset.
    /// </summary>
    [GraphQLDescription("Get messages from a topic")]
    public async Task<IReadOnlyList<MessageType>> GetMessages(
        [Service] IGraphQLBrokerService service,
        string topic,
        int? partition = null,
        long? offset = null,
        int limit = 10,
        CancellationToken cancellationToken = default) =>
        await service.GetMessagesAsync(topic, partition, offset, limit, cancellationToken);

    /// <summary>
    /// List all consumer groups.
    /// </summary>
    [GraphQLDescription("List all consumer groups")]
    public async Task<IReadOnlyList<ConsumerGroupType>> GetConsumerGroups(
        [Service] IGraphQLBrokerService service,
        CancellationToken cancellationToken) =>
        await service.GetConsumerGroupsAsync(cancellationToken);

    /// <summary>
    /// Get cluster information.
    /// </summary>
    [GraphQLDescription("Get cluster information")]
    public async Task<ClusterInfoType> GetCluster(
        [Service] IGraphQLBrokerService service,
        CancellationToken cancellationToken) =>
        await service.GetClusterInfoAsync(cancellationToken);
}
