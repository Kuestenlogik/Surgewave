using Kuestenlogik.Surgewave.Client.Native.Commands;
using Kuestenlogik.Surgewave.Client.Native.Commands.ConsumerGroups;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;

/// <summary>
/// Consumer group operations for Surgewave native client.
/// </summary>
public sealed class SurgewaveConsumerGroupOperations
{
    private readonly SurgewaveNativeClient _client;
    private readonly CommandExecutor _executor;

    internal SurgewaveConsumerGroupOperations(SurgewaveNativeClient client)
    {
        _client = client;
        _executor = new CommandExecutor(client);
    }

    /// <summary>
    /// List all consumer groups.
    /// </summary>
    public Task<List<ConsumerGroupInfo>> ListAsync(CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new ListGroupsCommand(), cancellationToken);

    /// <summary>
    /// Describe a consumer group.
    /// </summary>
    public Task<ConsumerGroupDescription> DescribeAsync(string groupId, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new DescribeGroupCommand(groupId), cancellationToken);

    /// <summary>
    /// Delete a consumer group.
    /// </summary>
    public Task DeleteAsync(string groupId, CancellationToken cancellationToken = default)
        => _executor.ExecuteVoidAsync(new DeleteGroupCommand(groupId), cancellationToken);

    /// <summary>
    /// Join a consumer group.
    /// </summary>
    public Task<JoinGroupResponse> JoinAsync(
        string groupId,
        string? memberId,
        string clientId,
        string protocolType,
        int sessionTimeoutMs,
        int rebalanceTimeoutMs,
        List<(string Name, byte[] Metadata)> protocols,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new JoinGroupCommand(
            groupId, memberId, clientId, protocolType,
            sessionTimeoutMs, rebalanceTimeoutMs, protocols), cancellationToken);

    /// <summary>
    /// Start building a join group request with fluent API.
    /// </summary>
    public JoinGroupBuilder Join(string groupId) => new(_client, groupId);

    /// <summary>
    /// Sync a consumer group (called by leader to distribute assignments).
    /// </summary>
    public Task<SyncGroupResponse> SyncAsync(
        string groupId,
        string memberId,
        int generationId,
        List<(string MemberId, byte[] Assignment)> assignments,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new SyncGroupCommand(groupId, memberId, generationId, assignments), cancellationToken);

    /// <summary>
    /// Send a heartbeat to keep group membership alive.
    /// </summary>
    public Task<ushort> HeartbeatAsync(
        string groupId,
        string memberId,
        int generationId,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new HeartbeatCommand(groupId, memberId, generationId), cancellationToken);

    /// <summary>
    /// Leave a consumer group.
    /// </summary>
    public Task LeaveAsync(string groupId, string memberId, CancellationToken cancellationToken = default)
        => _executor.ExecuteVoidAsync(new LeaveGroupCommand(groupId, memberId), cancellationToken);

    /// <summary>
    /// Commit an offset for a consumer group.
    /// </summary>
    public Task CommitOffsetAsync(
        string groupId,
        string memberId,
        int generationId,
        string topic,
        int partition,
        long offset,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteVoidAsync(new CommitOffsetCommand(groupId, memberId, generationId, topic, partition, offset), cancellationToken);

    /// <summary>
    /// Fetch the committed offset for a consumer group.
    /// </summary>
    public Task<long> FetchOffsetAsync(
        string groupId,
        string topic,
        int partition,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new FetchOffsetCommand(groupId, topic, partition), cancellationToken);

    /// <summary>
    /// Get lag information for a consumer group.
    /// </summary>
    public Task<ConsumerGroupLag> GetLagAsync(
        string groupId,
        CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetGroupLagCommand(groupId), cancellationToken);

    /// <summary>
    /// Get lag summary for all consumer groups.
    /// </summary>
    public Task<LagSummaryResult> GetLagSummaryAsync(CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetLagSummaryCommand(), cancellationToken);
}
