namespace Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;

/// <summary>
/// Types of WebSocket messages.
/// </summary>
public static class WebSocketMessageType
{
    // Client -> Server
    public const string Subscribe = "subscribe";
    public const string Unsubscribe = "unsubscribe";
    public const string Produce = "produce";
    public const string ProduceBatch = "produce_batch";
    public const string Commit = "commit";
    public const string Admin = "admin";
    public const string Ping = "ping";

    // Server -> Client
    public const string SubscribeResponse = "subscribe_response";
    public const string UnsubscribeResponse = "unsubscribe_response";
    public const string Message = "message";
    public const string MessageBatch = "message_batch";
    public const string ProduceResponse = "produce_response";
    public const string ProduceBatchResponse = "produce_batch_response";
    public const string CommitResponse = "commit_response";
    public const string AdminResponse = "admin_response";
    public const string AdminEvent = "admin_event";
    public const string Error = "error";
    public const string Pong = "pong";
}

/// <summary>
/// Admin action types.
/// </summary>
public static class AdminActionType
{
    public const string ListTopics = "list_topics";
    public const string DescribeTopic = "describe_topic";
    public const string ListConsumerGroups = "list_consumer_groups";
    public const string DescribeConsumerGroup = "describe_consumer_group";
    public const string GetClusterInfo = "get_cluster_info";
}

/// <summary>
/// Admin event types for broadcasting.
/// </summary>
public static class AdminEventType
{
    public const string TopicCreated = "topic_created";
    public const string TopicDeleted = "topic_deleted";
    public const string PartitionAdded = "partition_added";
    public const string ConsumerGroupRebalanced = "consumer_group_rebalanced";
    public const string ConnectionEstablished = "connection_established";
    public const string ConnectionClosed = "connection_closed";
}

/// <summary>
/// Error codes for WebSocket error responses.
/// </summary>
public static class WebSocketErrorCode
{
    public const string InvalidMessage = "INVALID_MESSAGE";
    public const string UnknownCluster = "UNKNOWN_CLUSTER";
    public const string UnknownTopic = "UNKNOWN_TOPIC";
    public const string SubscribeError = "SUBSCRIBE_ERROR";
    public const string SubscriptionNotFound = "SUBSCRIPTION_NOT_FOUND";
    public const string MaxSubscriptionsExceeded = "MAX_SUBSCRIPTIONS_EXCEEDED";
    public const string ProduceFailed = "PRODUCE_FAILED";
    public const string CommitFailed = "COMMIT_FAILED";
    public const string Unauthorized = "UNAUTHORIZED";
    public const string InternalError = "INTERNAL_ERROR";
}
