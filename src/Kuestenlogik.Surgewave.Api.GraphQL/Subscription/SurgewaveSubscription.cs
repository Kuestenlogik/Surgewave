using HotChocolate;
using HotChocolate.Execution;
using HotChocolate.Subscriptions;
using HotChocolate.Types;
using Kuestenlogik.Surgewave.Api.GraphQL.Types;

namespace Kuestenlogik.Surgewave.Api.GraphQL.Subscription;

/// <summary>
/// Root GraphQL subscription type for Surgewave broker.
/// Provides real-time message notifications via WebSocket.
/// </summary>
public sealed class SurgewaveSubscription
{
    /// <summary>
    /// Subscribe to new messages on a topic.
    /// Messages produced via the <c>produceMessage</c> mutation are delivered in real-time.
    /// </summary>
    [Subscribe(With = nameof(SubscribeToTopic))]
    [GraphQLDescription("Subscribe to new messages on a topic")]
    public MessageType OnMessage(
        [EventMessage] MessageType message,
        string topic) => message;

    /// <summary>
    /// Creates a filtered subscription stream scoped to the specified topic.
    /// </summary>
    public static ValueTask<ISourceStream<MessageType>> SubscribeToTopic(
        string topic,
        [Service] ITopicEventReceiver eventReceiver,
        CancellationToken cancellationToken) =>
        eventReceiver.SubscribeAsync<MessageType>($"OnMessage_{topic}", cancellationToken);
}
