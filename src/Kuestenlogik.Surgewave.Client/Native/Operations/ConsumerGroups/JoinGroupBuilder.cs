using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;

/// <summary>
/// Fluent builder for joining a consumer group.
/// </summary>
public sealed class JoinGroupBuilder
{
    private readonly SurgewaveNativeClient _client;
    private readonly string _groupId;
    private string? _memberId;
    private string _clientId = "surgewave-client";
    private string _protocolType = "consumer";
    private int _sessionTimeoutMs = 30000;
    private int _rebalanceTimeoutMs = 60000;
    private readonly List<(string Name, byte[] Metadata)> _protocols = new();

    internal JoinGroupBuilder(SurgewaveNativeClient client, string groupId)
    {
        _client = client;
        _groupId = groupId;
    }

    /// <summary>
    /// Set the member ID (null for new members).
    /// </summary>
    public JoinGroupBuilder WithMemberId(string? memberId)
    {
        _memberId = memberId;
        return this;
    }

    /// <summary>
    /// Set the client ID.
    /// </summary>
    public JoinGroupBuilder WithClientId(string clientId)
    {
        _clientId = clientId;
        return this;
    }

    /// <summary>
    /// Set the protocol type.
    /// </summary>
    public JoinGroupBuilder WithProtocolType(string protocolType)
    {
        _protocolType = protocolType;
        return this;
    }

    /// <summary>
    /// Set the session timeout.
    /// </summary>
    public JoinGroupBuilder WithSessionTimeout(TimeSpan timeout)
    {
        _sessionTimeoutMs = (int)timeout.TotalMilliseconds;
        return this;
    }

    /// <summary>
    /// Set the rebalance timeout.
    /// </summary>
    public JoinGroupBuilder WithRebalanceTimeout(TimeSpan timeout)
    {
        _rebalanceTimeoutMs = (int)timeout.TotalMilliseconds;
        return this;
    }

    /// <summary>
    /// Add a protocol.
    /// </summary>
    public JoinGroupBuilder WithProtocol(string name, byte[] metadata)
    {
        _protocols.Add((name, metadata));
        return this;
    }

    /// <summary>
    /// Execute the join group request.
    /// </summary>
    public Task<JoinGroupResponse> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        if (_protocols.Count == 0)
        {
            throw new InvalidOperationException("At least one protocol must be specified");
        }

        return _client.Groups.JoinAsync(
            _groupId,
            _memberId,
            _clientId,
            _protocolType,
            _sessionTimeoutMs,
            _rebalanceTimeoutMs,
            _protocols,
            cancellationToken);
    }
}
