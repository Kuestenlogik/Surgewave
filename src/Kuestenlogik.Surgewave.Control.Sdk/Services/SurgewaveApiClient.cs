using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

public class SurgewaveApiClient : ISurgewaveApiClient
{
    private readonly HttpClient _httpClient;
    private readonly JsonSerializerOptions _jsonOptions;

    public SurgewaveApiClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
        _jsonOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            PropertyNameCaseInsensitive = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };
    }

    private string BuildUrl(string path, string? clusterId)
    {
        var clusterPath = clusterId ?? "default";
        return $"/v3/clusters/{clusterPath}{path}";
    }

    // Cluster operations

    public async Task<ClusterInfo?> GetClusterInfoAsync(string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            // Use health endpoint to check if broker is available
            var healthResponse = await _httpClient.GetAsync("/health", ct);
            if (!healthResponse.IsSuccessStatusCode)
            {
                return null;
            }

            // Return basic cluster info - broker is healthy
            return new ClusterInfo(
                clusterId ?? "default",
                ControllerId: 0,
                BrokerCount: 1,
                TopicCount: 0,
                PartitionCount: 0);
        }
        catch
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<BrokerInfo>> ListBrokersAsync(string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            // Get cluster info to determine controller
            var clusterInfo = await GetClusterInfoAsync(clusterId, ct);
            var controllerId = clusterInfo?.ControllerId ?? -1;

            var response = await _httpClient.GetFromJsonAsync<ListBrokersResponseDto>(
                BuildUrl("/brokers", clusterId), _jsonOptions, ct);

            return response?.Brokers?.Select(b => new BrokerInfo(
                b.BrokerId,
                b.Host ?? "unknown",
                b.Port,
                b.Rack,
                b.BrokerId == controllerId,
                PeerTransport: string.IsNullOrWhiteSpace(b.PeerTransport) ? "tcp" : b.PeerTransport!)).ToList() ?? [];
        }
        catch
        {
            return [];
        }
    }

    // Topic operations

    public async Task<IReadOnlyList<TopicInfo>> ListTopicsAsync(string? clusterId = null, bool includeInternal = false, CancellationToken ct = default)
    {
        try
        {
            var url = BuildUrl("/topics", clusterId);
            if (!includeInternal)
                url += "?include_internal=false";

            var response = await _httpClient.GetFromJsonAsync<TopicsResponse>(url, _jsonOptions, ct);

            // Use TopicInfos from gRPC response (has full topic info)
            return response?.TopicInfos?.Select(t => new TopicInfo(
                t.Name ?? "unknown",
                t.PartitionCount,
                (short)t.ReplicationFactor,
                t.IsInternal)).ToList() ?? [];
        }
        catch
        {
            return [];
        }
    }

    public async Task<TopicDescription?> DescribeTopicAsync(string topic, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<DescribeTopicResponseDto>(
                BuildUrl($"/topics/{topic}", clusterId), _jsonOptions, ct);

            var topicInfo = response?.Topics?.FirstOrDefault()?.TopicInfo;
            if (topicInfo == null) return null;

            var partitions = topicInfo.Partitions?.Select(p => new PartitionInfo(
                p.PartitionId,
                p.Leader,
                p.Replicas ?? [],
                p.Isr ?? [],
                p.HighWatermark,
                p.LogStartOffset)).ToList() ?? [];

            return new TopicDescription(
                topicInfo.Name ?? topic,
                topicInfo.NumPartitions,
                (short)topicInfo.ReplicationFactor,
                topicInfo.IsInternal,
                partitions,
                topicInfo.Config ?? new Dictionary<string, string>());
        }
        catch
        {
            return null;
        }
    }

    public async Task<bool> CreateTopicAsync(CreateTopicRequest request, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            // Use snake_case for gRPC JSON transcoding compatibility
            var payload = new Dictionary<string, object>
            {
                ["topic"] = request.Name,
                ["num_partitions"] = request.NumPartitions,
                ["replication_factor"] = request.ReplicationFactor,
                ["config"] = request.Configs ?? new Dictionary<string, string>()
            };

            var response = await _httpClient.PostAsJsonAsync(
                BuildUrl("/topics", clusterId), payload, ct);

            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public async Task<bool> DeleteTopicAsync(string topic, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.DeleteAsync(
                BuildUrl($"/topics/{topic}", clusterId), ct);

            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    // Consumer Group operations

    public async Task<IReadOnlyList<ConsumerGroupInfo>> ListConsumerGroupsAsync(string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<ConsumerGroupsResponse>(
                BuildUrl("/consumer-groups", clusterId), _jsonOptions, ct);

            return response?.Groups?.Select(g => new ConsumerGroupInfo(
                g.GroupId ?? "unknown",
                g.State ?? "Unknown",
                g.ProtocolType ?? "",
                0)).ToList() ?? [];
        }
        catch
        {
            return [];
        }
    }

    public async Task<ConsumerGroupDescription?> DescribeConsumerGroupAsync(string groupId, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<DescribeGroupResponseDto>(
                BuildUrl($"/consumer-groups/{groupId}", clusterId), _jsonOptions, ct);

            var group = response?.Groups?.FirstOrDefault();
            if (group == null) return null;

            var members = group.Members?.Select(m => new MemberInfo(
                m.MemberId ?? "unknown",
                m.ClientId ?? "unknown",
                m.ClientHost ?? "unknown",
                [])).ToList() ?? [];

            return new ConsumerGroupDescription(
                group.GroupId ?? groupId,
                group.State ?? "Unknown",
                group.ProtocolType ?? "",
                group.ProtocolName ?? "",
                -1, // Coordinator not in gRPC response
                members);
        }
        catch
        {
            return null;
        }
    }

    public async Task<bool> DeleteConsumerGroupAsync(string groupId, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.DeleteAsync(
                BuildUrl($"/consumer-groups/{groupId}", clusterId), ct);

            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    // Health - uses the standard ASP.NET Core health endpoint

    public async Task<bool> PingAsync(string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.GetAsync("/health", ct);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public async Task<HealthDetails?> GetHealthDetailsAsync(CancellationToken ct = default)
    {
        try
        {
            // Use snake_case options for health endpoint
            var options = new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
                PropertyNameCaseInsensitive = true
            };
            return await _httpClient.GetFromJsonAsync<HealthDetails>("/health", options, ct);
        }
        catch
        {
            return null;
        }
    }

    // Connector operations

    public async Task<IReadOnlyList<string>> ListConnectorsAsync(string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<ListConnectorsResponseDto>(
                BuildUrl("/connectors", clusterId), _jsonOptions, ct);
            return response?.Connectors ?? [];
        }
        catch
        {
            return [];
        }
    }

    public async Task<ConnectorInfo?> GetConnectorAsync(string name, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<GetConnectorResponseDto>(
                BuildUrl($"/connectors/{name}", clusterId), _jsonOptions, ct);
            if (response == null) return null;
            return new ConnectorInfo(
                response.Name ?? name,
                response.Type ?? "unknown",
                response.Config ?? new Dictionary<string, string>(),
                response.Tasks?.Select(t => t.TaskId).ToList() ?? []);
        }
        catch
        {
            return null;
        }
    }

    public async Task<ConnectorStatus?> GetConnectorStatusAsync(string name, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<GetConnectorStatusResponseDto>(
                BuildUrl($"/connectors/{name}/status", clusterId), _jsonOptions, ct);
            if (response == null) return null;
            return new ConnectorStatus(
                response.Name ?? name,
                response.Type ?? "unknown",
                response.ConnectorState?.State ?? "UNKNOWN",
                response.ConnectorState?.WorkerId ?? "",
                response.TaskStates?.Select(t => new TaskStatus(t.Id, t.State ?? "UNKNOWN", t.WorkerId ?? "")).ToList() ?? []);
        }
        catch
        {
            return null;
        }
    }

    public async Task<bool> CreateConnectorAsync(string name, Dictionary<string, string> config, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var payload = new { name, config };
            var response = await _httpClient.PostAsJsonAsync(
                BuildUrl("/connectors", clusterId), payload, _jsonOptions, ct);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public async Task<bool> DeleteConnectorAsync(string name, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.DeleteAsync(
                BuildUrl($"/connectors/{name}", clusterId), ct);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public async Task<bool> PauseConnectorAsync(string name, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.PutAsync(
                BuildUrl($"/connectors/{name}/pause", clusterId), null, ct);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public async Task<bool> ResumeConnectorAsync(string name, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.PutAsync(
                BuildUrl($"/connectors/{name}/resume", clusterId), null, ct);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public async Task<bool> RestartConnectorAsync(string name, bool includeTasks = false, bool onlyFailed = false, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var payload = new { includeTasks, onlyFailed };
            var response = await _httpClient.PostAsJsonAsync(
                BuildUrl($"/connectors/{name}/restart", clusterId), payload, _jsonOptions, ct);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public async Task<IReadOnlyList<ConnectorPluginInfo>> ListConnectorPluginsAsync(string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.GetFromJsonAsync<ListConnectorPluginsResponseDto>(
                BuildUrl("/connector-plugins", clusterId), _jsonOptions, ct);
            return response?.Plugins?.Select(p => new ConnectorPluginInfo(
                p.ClassName ?? "unknown",
                p.Type ?? "unknown",
                p.Version ?? "")).ToList() ?? [];
        }
        catch
        {
            return [];
        }
    }

    // Message Browser operations

    public async Task<MessagesResult?> GetMessagesAsync(string topic, int partition, long offset = 0, int limit = 20, CancellationToken ct = default)
    {
        try
        {
            var url = $"/admin/messages/{Uri.EscapeDataString(topic)}/{partition}?offset={offset}&limit={limit}";
            var response = await _httpClient.GetFromJsonAsync<MessagesResponseDto>(url, _jsonOptions, ct);
            if (response == null) return null;

            return new MessagesResult(
                response.Topic ?? topic,
                response.Partition,
                response.Offset,
                response.HighWatermark,
                response.LogStartOffset,
                response.Messages?.Select(m => new MessageDetail(
                    m.Offset,
                    m.Timestamp,
                    m.Key,
                    m.Value,
                    m.ValueBase64,
                    m.Headers ?? new Dictionary<string, string>(),
                    m.IsCompressed,
                    m.ValueSizeBytes)).ToList() ?? []);
        }
        catch
        {
            return null;
        }
    }

    public async Task<MessageDetail?> GetMessageAsync(string topic, int partition, long offset, CancellationToken ct = default)
    {
        try
        {
            var url = $"/admin/messages/{Uri.EscapeDataString(topic)}/{partition}/{offset}";
            var response = await _httpClient.GetFromJsonAsync<MessageDetailDto>(url, _jsonOptions, ct);
            if (response == null) return null;

            return new MessageDetail(
                response.Offset,
                response.Timestamp,
                response.Key,
                response.Value,
                response.ValueBase64,
                response.Headers ?? new Dictionary<string, string>(),
                response.IsCompressed,
                response.ValueSizeBytes);
        }
        catch
        {
            return null;
        }
    }

    // Message Producer operations

    public async Task<ProduceMessageResult?> ProduceMessageAsync(string topic, ProduceMessageRequest request, CancellationToken ct = default)
    {
        try
        {
            var url = $"/admin/messages/{Uri.EscapeDataString(topic)}";
            var payload = new
            {
                key = request.Key,
                value = request.Value,
                headers = request.Headers,
                partition = request.Partition
            };
            var response = await _httpClient.PostAsJsonAsync(url, payload, _jsonOptions, ct);
            if (!response.IsSuccessStatusCode) return null;

            var result = await response.Content.ReadFromJsonAsync<ProduceResultDto>(_jsonOptions, ct);
            if (result == null) return null;

            return new ProduceMessageResult(result.Topic ?? topic, result.Partition, result.Offset, result.Timestamp);
        }
        catch
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<ProduceMessageResult>> ProduceBatchAsync(string topic, IReadOnlyList<ProduceMessageRequest> requests, CancellationToken ct = default)
    {
        try
        {
            var url = $"/admin/messages/{Uri.EscapeDataString(topic)}/batch";
            var payload = requests.Select(r => new
            {
                key = r.Key,
                value = r.Value,
                headers = r.Headers,
                partition = r.Partition
            }).ToList();
            var response = await _httpClient.PostAsJsonAsync(url, payload, _jsonOptions, ct);
            if (!response.IsSuccessStatusCode) return [];

            var results = await response.Content.ReadFromJsonAsync<List<ProduceResultDto>>(_jsonOptions, ct);
            return results?.Select(r => new ProduceMessageResult(
                r.Topic ?? topic, r.Partition, r.Offset, r.Timestamp)).ToList() ?? [];
        }
        catch
        {
            return [];
        }
    }

    public async Task<long?> GetOffsetForTimestampAsync(string topic, int partition, DateTimeOffset timestamp, CancellationToken ct = default)
    {
        try
        {
            var url = $"/admin/messages/{Uri.EscapeDataString(topic)}/{partition}/offset-for-timestamp?timestamp={timestamp.ToUnixTimeMilliseconds()}";
            var response = await _httpClient.GetFromJsonAsync<OffsetForTimestampDto>(url, _jsonOptions, ct);
            return response?.Offset;
        }
        catch
        {
            return null;
        }
    }

    // Consumer Lag operations

    public async Task<IReadOnlyList<ConsumerGroupLag>> GetAllConsumerLagsAsync(string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            // Direkte Broker-REST-Route (ConsumerLagRestApi), nicht /v3-Transcoding —
            // dort wuerde /consumer-groups/lag die DescribeGroup-Route mit
            // group_id="lag" matchen und still eine leere Antwort liefern.
            var url = "/api/consumer-groups/lag";
            var response = await _httpClient.GetFromJsonAsync<ConsumerLagListDto>(url, _jsonOptions, ct);
            return response?.Groups?.Select(MapConsumerGroupLag).ToList() ?? [];
        }
        catch
        {
            return [];
        }
    }

    public async Task<ConsumerGroupLag?> GetConsumerGroupLagAsync(string groupId, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var url = $"/api/consumer-groups/{Uri.EscapeDataString(groupId)}/lag";
            var response = await _httpClient.GetFromJsonAsync<ConsumerGroupLagDto>(url, _jsonOptions, ct);
            return response != null ? MapConsumerGroupLag(response) : null;
        }
        catch
        {
            return null;
        }
    }

    public async Task<bool> ResetConsumerGroupOffsetsAsync(string groupId, string topic, string resetStrategy,
        DateTimeOffset? timestamp = null, long? offset = null, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var url = $"/api/consumer-groups/{Uri.EscapeDataString(groupId)}/offsets";
            var payload = new
            {
                topic,
                strategy = resetStrategy,
                timestamp = timestamp?.ToUnixTimeMilliseconds(),
                offset
            };
            var response = await _httpClient.PostAsJsonAsync(url, payload, _jsonOptions, ct);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    public async Task<IReadOnlyList<BrokerConfigEntry>> GetBrokerConfigsAsync(int brokerId, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            // Transcodierte gRPC-Route (AdminService.DescribeBrokerConfig) —
            // liefert die effektive Broker-Config aus DynamicBrokerConfig.
            // Bei Fehlern ehrlich leer statt der frueheren Fake-Kafka-Defaults.
            var url = BuildUrl($"/brokers/{brokerId}/configs", clusterId);
            var response = await _httpClient.GetFromJsonAsync<DescribeBrokerConfigResponseDto>(url, _jsonOptions, ct);
            if (response?.Configs == null) return [];

            return response.Configs.Select(c => new BrokerConfigEntry
            {
                Key = c.Key ?? "",
                Value = c.Value ?? "",
                DefaultValue = null,
                Source = c.IsDefault ? ConfigSource.StaticBroker : ConfigSource.DynamicBroker,
                IsReadOnly = c.IsReadOnly,
                IsSensitive = c.IsSensitive,
                Documentation = null,
                Category = CategorizeConfig(c.Key ?? "")
            }).ToList();
        }
        catch
        {
            return [];
        }
    }

    public async Task<bool> AlterBrokerConfigAsync(int brokerId, string key, string value, string? clusterId = null, CancellationToken ct = default)
    {
        try
        {
            var url = BuildUrl($"/brokers/{brokerId}/configs", clusterId);
            // AlterBrokerConfigRequest erwartet ein configs[]-Array (Proto-Transcoding).
            var payload = new { configs = new[] { new { key, value } } };
            var response = await _httpClient.PutAsJsonAsync(url, payload, _jsonOptions, ct);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    private static string CategorizeConfig(string key)
    {
        if (key.StartsWith("log.", StringComparison.Ordinal)) return "Log";
        if (key.StartsWith("num.", StringComparison.Ordinal)) return "Threading";
        if (key.StartsWith("replica.", StringComparison.Ordinal) || key.Contains("insync") || key.Contains("replication")) return "Replication";
        if (key.StartsWith("socket.", StringComparison.Ordinal) || key.Contains("connections")) return "Network";
        if (key.Contains("message") || key.Contains("compression")) return "Messages";
        if (key.Contains("group") || key.Contains("offset")) return "Consumer Groups";
        if (key.Contains("transaction")) return "Transactions";
        if (key.Contains("topic") || key.Contains("partition")) return "Topic Defaults";
        return "General";
    }

    // SQL Query operations

    public async Task<SqlQueryResult?> ExecuteSqlAsync(string sql, CancellationToken ct = default)
    {
        try
        {
            var payload = new { sql };
            var response = await _httpClient.PostAsJsonAsync("/api/sql/execute", payload, _jsonOptions, ct);
            if (!response.IsSuccessStatusCode)
            {
                var errorBody = await response.Content.ReadAsStringAsync(ct);
                return new SqlQueryResult(null, null, 0, $"HTTP {(int)response.StatusCode}: {errorBody}");
            }

            return await response.Content.ReadFromJsonAsync<SqlQueryResult>(_jsonOptions, ct);
        }
        catch (Exception ex)
        {
            return new SqlQueryResult(null, null, 0, ex.Message);
        }
    }

    public async Task<SqlContinuousQueryInfo?> CreateContinuousQueryAsync(string sql, string name, CancellationToken ct = default)
    {
        try
        {
            var payload = new { sql, name };
            var response = await _httpClient.PostAsJsonAsync("/api/sql/queries", payload, _jsonOptions, ct);
            if (!response.IsSuccessStatusCode) return null;

            return await response.Content.ReadFromJsonAsync<SqlContinuousQueryInfo>(_jsonOptions, ct);
        }
        catch
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<SqlContinuousQueryInfo>> ListContinuousQueriesAsync(CancellationToken ct = default)
    {
        try
        {
            return await _httpClient.GetFromJsonAsync<List<SqlContinuousQueryInfo>>("/api/sql/queries", _jsonOptions, ct) ?? [];
        }
        catch
        {
            return [];
        }
    }

    public async Task<bool> TerminateContinuousQueryAsync(string queryId, CancellationToken ct = default)
    {
        try
        {
            var response = await _httpClient.DeleteAsync($"/api/sql/queries/{Uri.EscapeDataString(queryId)}", ct);
            return response.IsSuccessStatusCode;
        }
        catch
        {
            return false;
        }
    }

    private record BrokerConfigDto(string? Name, string? Value, string? DefaultValue, string? Source, bool IsReadOnly, bool IsSensitive, string? Documentation);
    private record DescribeBrokerConfigResponseDto(List<BrokerConfigDetailDto>? Configs);
    private record BrokerConfigDetailDto(string? Key, string? Value, bool IsDefault, bool IsReadOnly, bool IsSensitive);

    private static ConsumerGroupLag MapConsumerGroupLag(ConsumerGroupLagDto dto) => new(
        dto.GroupId ?? "unknown",
        dto.State ?? "Unknown",
        dto.TotalLag,
        dto.Partitions?.Select(p => new TopicPartitionLag(
            p.Topic ?? "unknown",
            p.Partition,
            p.CurrentOffset,
            p.EndOffset,
            p.Lag,
            p.ConsumerId)).ToList() ?? []);

    // Response DTOs for JSON deserialization (gRPC JSON Transcoding uses camelCase)
    private record TopicsResponse(List<string>? Topics, List<TopicSummaryDto>? TopicInfos);
    private record TopicSummaryDto(string? Name, int PartitionCount, int ReplicationFactor, bool IsInternal);

    // gRPC DescribeTopic response format
    private record DescribeTopicResponseDto(List<TopicDescriptionDto>? Topics);
    private record TopicDescriptionDto(TopicInfoDto? TopicInfo, ResponseStatusDto? Status);
    private record TopicInfoDto(
        string? Name,
        int NumPartitions,
        int ReplicationFactor,
        Dictionary<string, string>? Config,
        List<PartitionInfoDto>? Partitions,
        bool IsInternal);
    private record PartitionInfoDto(
        int PartitionId,
        int Leader,
        List<int>? Replicas,
        List<int>? Isr,
        long HighWatermark,
        long LogStartOffset);
    private record ResponseStatusDto(string? ErrorCode, string? ErrorMessage);

    // gRPC ListGroups response format
    private record ConsumerGroupsResponse(List<GroupListingDto>? Groups, ResponseStatusDto? Status);
    private record GroupListingDto(string? GroupId, string? ProtocolType, string? State);

    // gRPC DescribeGroup response format
    private record DescribeGroupResponseDto(List<GroupDescriptionDto>? Groups);
    private record GroupDescriptionDto(
        string? GroupId,
        string? State,
        string? ProtocolType,
        string? ProtocolName,
        int GenerationId,
        List<MemberDescriptionDto>? Members,
        ResponseStatusDto? Status);
    private record MemberDescriptionDto(
        string? MemberId,
        string? GroupInstanceId,
        string? ClientId,
        string? ClientHost,
        string? MemberMetadata,
        string? MemberAssignment);

    // gRPC ListBrokers response format
    private record ListBrokersResponseDto(List<BrokerInfoGrpcDto>? Brokers, ResponseStatusDto? Status);
    private record BrokerInfoGrpcDto(int BrokerId, string? Host, int Port, string? Rack, string? PeerTransport);

    // gRPC Connect response formats
    private record ListConnectorsResponseDto(List<string>? Connectors, ResponseStatusDto? Status);
    private record GetConnectorResponseDto(
        string? Name,
        string? Type,
        Dictionary<string, string>? Config,
        List<TaskInfoDto>? Tasks,
        ResponseStatusDto? Status);
    private record TaskInfoDto(string? Connector, int TaskId);
    private record GetConnectorStatusResponseDto(
        string? Name,
        string? Type,
        ConnectorStateDto? ConnectorState,
        List<TaskStateDto>? TaskStates,
        ResponseStatusDto? Status);
    private record ConnectorStateDto(string? State, string? WorkerId, string? Trace);
    private record TaskStateDto(int Id, string? State, string? WorkerId, string? Trace);
    private record ListConnectorPluginsResponseDto(List<ConnectorPluginDto>? Plugins, ResponseStatusDto? Status);
    private record ConnectorPluginDto(string? ClassName, string? Type, string? Version);

    // Consumer Lag DTOs
    private record ConsumerLagListDto(List<ConsumerGroupLagDto>? Groups);
    private record ConsumerGroupLagDto(string? GroupId, string? State, long TotalLag, List<PartitionLagDto>? Partitions);
    private record PartitionLagDto(string? Topic, int Partition, long CurrentOffset, long EndOffset, long Lag, string? ConsumerId);

    // Message Producer DTOs
    private record ProduceResultDto(string? Topic, int Partition, long Offset, DateTimeOffset Timestamp);
    private record OffsetForTimestampDto(long Offset);

    // Message Browser DTOs
    private record MessagesResponseDto(
        string? Topic,
        int Partition,
        long Offset,
        long HighWatermark,
        long LogStartOffset,
        List<MessageDetailDto>? Messages);
    private record MessageDetailDto(
        long Offset,
        DateTimeOffset Timestamp,
        string? Key,
        string? Value,
        string? ValueBase64,
        Dictionary<string, string>? Headers,
        bool IsCompressed,
        int ValueSizeBytes);
}

// Connector model records
public record ConnectorInfo(
    string Name,
    string Type,
    Dictionary<string, string> Config,
    List<int> TaskIds);

public record ConnectorStatus(
    string Name,
    string Type,
    string State,
    string WorkerId,
    List<TaskStatus> Tasks);

public record TaskStatus(int Id, string State, string WorkerId);

public record ConnectorPluginInfo(string ClassName, string Type, string Version);
