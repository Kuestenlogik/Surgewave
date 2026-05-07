using HotChocolate;
using HotChocolate.Subscriptions;
using Kuestenlogik.Surgewave.Api.GraphQL.Services;
using Kuestenlogik.Surgewave.Api.GraphQL.Types;

namespace Kuestenlogik.Surgewave.Api.GraphQL.Mutation;

/// <summary>
/// Root GraphQL mutation type for Surgewave broker.
/// </summary>
public sealed class SurgewaveMutation
{
    /// <summary>
    /// Produce a message to a topic.
    /// </summary>
    [GraphQLDescription("Produce a message to a topic")]
    public async Task<MessageType> ProduceMessage(
        [Service] IGraphQLBrokerService service,
        [Service] ITopicEventSender eventSender,
        string topic,
        string? key,
        string value,
        int partition = 0,
        CancellationToken cancellationToken = default)
    {
        var message = await service.ProduceMessageAsync(topic, key, value, partition, cancellationToken);

        // Publish to GraphQL subscription topic so subscribers receive real-time updates
        await eventSender.SendAsync($"OnMessage_{topic}", message, cancellationToken);

        return message;
    }

    /// <summary>
    /// Create a new topic.
    /// </summary>
    [GraphQLDescription("Create a new topic")]
    public async Task<TopicType> CreateTopic(
        [Service] IGraphQLBrokerService service,
        string name,
        int partitions = 1,
        int replicationFactor = 1,
        CancellationToken cancellationToken = default) =>
        await service.CreateTopicAsync(name, partitions, replicationFactor, cancellationToken);
}
