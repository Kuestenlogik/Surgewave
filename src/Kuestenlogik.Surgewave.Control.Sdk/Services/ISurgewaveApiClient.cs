namespace Kuestenlogik.Surgewave.Control.Services;

using Kuestenlogik.Surgewave.Control.Models;

public interface ISurgewaveApiClient
{
    // Cluster
    Task<ClusterInfo?> GetClusterInfoAsync(string? clusterId = null, CancellationToken ct = default);
    Task<IReadOnlyList<BrokerInfo>> ListBrokersAsync(string? clusterId = null, CancellationToken ct = default);

    // Topics
    Task<IReadOnlyList<TopicInfo>> ListTopicsAsync(string? clusterId = null, bool includeInternal = false, CancellationToken ct = default);
    Task<TopicDescription?> DescribeTopicAsync(string topic, string? clusterId = null, CancellationToken ct = default);
    Task<bool> CreateTopicAsync(CreateTopicRequest request, string? clusterId = null, CancellationToken ct = default);
    Task<bool> DeleteTopicAsync(string topic, string? clusterId = null, CancellationToken ct = default);

    // Consumer Groups
    Task<IReadOnlyList<ConsumerGroupInfo>> ListConsumerGroupsAsync(string? clusterId = null, CancellationToken ct = default);
    Task<ConsumerGroupDescription?> DescribeConsumerGroupAsync(string groupId, string? clusterId = null, CancellationToken ct = default);
    Task<bool> DeleteConsumerGroupAsync(string groupId, string? clusterId = null, CancellationToken ct = default);

    // Health
    Task<bool> PingAsync(string? clusterId = null, CancellationToken ct = default);
    Task<HealthDetails?> GetHealthDetailsAsync(CancellationToken ct = default);

    // Connectors
    Task<IReadOnlyList<string>> ListConnectorsAsync(string? clusterId = null, CancellationToken ct = default);
    Task<ConnectorInfo?> GetConnectorAsync(string name, string? clusterId = null, CancellationToken ct = default);
    Task<ConnectorStatus?> GetConnectorStatusAsync(string name, string? clusterId = null, CancellationToken ct = default);
    Task<bool> CreateConnectorAsync(string name, Dictionary<string, string> config, string? clusterId = null, CancellationToken ct = default);
    Task<bool> DeleteConnectorAsync(string name, string? clusterId = null, CancellationToken ct = default);
    Task<bool> PauseConnectorAsync(string name, string? clusterId = null, CancellationToken ct = default);
    Task<bool> ResumeConnectorAsync(string name, string? clusterId = null, CancellationToken ct = default);
    Task<bool> RestartConnectorAsync(string name, bool includeTasks = false, bool onlyFailed = false, string? clusterId = null, CancellationToken ct = default);
    Task<IReadOnlyList<ConnectorPluginInfo>> ListConnectorPluginsAsync(string? clusterId = null, CancellationToken ct = default);

    // Message Browser
    Task<MessagesResult?> GetMessagesAsync(string topic, int partition, long offset = 0, int limit = 20, CancellationToken ct = default);
    Task<MessageDetail?> GetMessageAsync(string topic, int partition, long offset, CancellationToken ct = default);

    // Message Producer
    Task<ProduceMessageResult?> ProduceMessageAsync(string topic, ProduceMessageRequest request, CancellationToken ct = default);
    Task<IReadOnlyList<ProduceMessageResult>> ProduceBatchAsync(string topic, IReadOnlyList<ProduceMessageRequest> requests, CancellationToken ct = default);

    // Seek by Timestamp
    Task<long?> GetOffsetForTimestampAsync(string topic, int partition, DateTimeOffset timestamp, CancellationToken ct = default);

    // Consumer Lag
    Task<IReadOnlyList<ConsumerGroupLag>> GetAllConsumerLagsAsync(string? clusterId = null, CancellationToken ct = default);
    Task<ConsumerGroupLag?> GetConsumerGroupLagAsync(string groupId, string? clusterId = null, CancellationToken ct = default);

    // Offset Reset
    Task<bool> ResetConsumerGroupOffsetsAsync(string groupId, string topic, string resetStrategy, DateTimeOffset? timestamp = null, long? offset = null, string? clusterId = null, CancellationToken ct = default);

    // Broker Config
    Task<IReadOnlyList<BrokerConfigEntry>> GetBrokerConfigsAsync(int brokerId, string? clusterId = null, CancellationToken ct = default);
    Task<bool> AlterBrokerConfigAsync(int brokerId, string key, string value, string? clusterId = null, CancellationToken ct = default);

    // SQL Query
    Task<SqlQueryResult?> ExecuteSqlAsync(string sql, CancellationToken ct = default);
    Task<SqlContinuousQueryInfo?> CreateContinuousQueryAsync(string sql, string name, CancellationToken ct = default);
    Task<IReadOnlyList<SqlContinuousQueryInfo>> ListContinuousQueriesAsync(CancellationToken ct = default);
    Task<bool> TerminateContinuousQueryAsync(string queryId, CancellationToken ct = default);
}
