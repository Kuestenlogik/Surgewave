using Kuestenlogik.Surgewave.Client.Native.Operations.Admin;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// Administrative operations for Surgewave native client (quotas, ACLs, elections, broker config).
/// </summary>
public sealed class SurgewaveAdminOperations
{
    private readonly SurgewaveNativeClient _client;

    internal SurgewaveAdminOperations(SurgewaveNativeClient client) => _client = client;

    #region Quota Operations

    /// <summary>
    /// Get the current quota configuration.
    /// </summary>
    public async Task<QuotaConfig> GetQuotaConfigAsync(CancellationToken cancellationToken = default)
    {
        var (header, payload) = await _client.SendRequestAsync(
            SurgewaveOpCode.GetQuotaConfig,
            ReadOnlyMemory<byte>.Empty,
            cancellationToken);

        var reader = new SurgewavePayloadReader(payload.Span);
        var errorCode = (SurgewaveErrorCode)reader.ReadUInt16();

        if (errorCode != SurgewaveErrorCode.None)
        {
            throw new InvalidOperationException($"GetQuotaConfig failed: {errorCode}");
        }

        var produceRateLimit = reader.ReadInt64();
        var fetchRateLimit = reader.ReadInt64();
        var requestRateLimit = reader.ReadInt64();
        var enabled = reader.ReadUInt8() != 0;

        return new QuotaConfig(produceRateLimit, fetchRateLimit, requestRateLimit, enabled);
    }

    /// <summary>
    /// Set the quota configuration.
    /// </summary>
    public async Task SetQuotaConfigAsync(
        long? produceRateLimit = null,
        long? fetchRateLimit = null,
        long? requestRateLimit = null,
        bool? enabled = null,
        CancellationToken cancellationToken = default)
    {
        var payloadBuffer = new byte[64];
        var writer = new SurgewavePayloadWriter(payloadBuffer);

        // Use -1 to indicate "don't change"
        writer.WriteInt64(produceRateLimit ?? -1);
        writer.WriteInt64(fetchRateLimit ?? -1);
        writer.WriteInt64(requestRateLimit ?? -1);
        writer.WriteInt8(enabled.HasValue ? (enabled.Value ? (sbyte)1 : (sbyte)0) : (sbyte)-1);

        var (header, payload) = await _client.SendRequestAsync(
            SurgewaveOpCode.SetQuotaConfig,
            payloadBuffer.AsMemory(0, writer.Position),
            cancellationToken);

        var reader = new SurgewavePayloadReader(payload.Span);
        var errorCode = (SurgewaveErrorCode)reader.ReadUInt16();

        if (errorCode != SurgewaveErrorCode.None)
        {
            throw new InvalidOperationException($"SetQuotaConfig failed: {errorCode}");
        }
    }

    /// <summary>
    /// List all clients with quota tracking information.
    /// </summary>
    public async Task<List<ClientQuotaInfo>> ListClientQuotasAsync(CancellationToken cancellationToken = default)
    {
        var (header, payload) = await _client.SendRequestAsync(
            SurgewaveOpCode.ListClientQuotas,
            ReadOnlyMemory<byte>.Empty,
            cancellationToken);

        var reader = new SurgewavePayloadReader(payload.Span);
        var errorCode = (SurgewaveErrorCode)reader.ReadUInt16();

        if (errorCode != SurgewaveErrorCode.None)
        {
            throw new InvalidOperationException($"ListClientQuotas failed: {errorCode}");
        }

        var count = reader.ReadInt32();
        var result = new List<ClientQuotaInfo>(count);

        for (int i = 0; i < count; i++)
        {
            var clientId = reader.ReadString() ?? string.Empty;
            var produceRate = reader.ReadInt64();
            var fetchRate = reader.ReadInt64();
            var isThrottled = reader.ReadUInt8() != 0;
            result.Add(new ClientQuotaInfo(clientId, produceRate, fetchRate, isThrottled));
        }

        return result;
    }

    /// <summary>
    /// Describe quota usage for specific clients.
    /// </summary>
    public async Task<List<ClientQuotaDescription>> DescribeClientQuotasAsync(
        List<string> clientIds,
        CancellationToken cancellationToken = default)
    {
        var payloadBuffer = new byte[4096];
        var writer = new SurgewavePayloadWriter(payloadBuffer);

        writer.WriteInt32(clientIds.Count);
        foreach (var clientId in clientIds)
        {
            writer.WriteString(clientId);
        }

        var (header, payload) = await _client.SendRequestAsync(
            SurgewaveOpCode.DescribeClientQuotas,
            payloadBuffer.AsMemory(0, writer.Position),
            cancellationToken);

        var reader = new SurgewavePayloadReader(payload.Span);
        var errorCode = (SurgewaveErrorCode)reader.ReadUInt16();

        if (errorCode != SurgewaveErrorCode.None)
        {
            throw new InvalidOperationException($"DescribeClientQuotas failed: {errorCode}");
        }

        var count = reader.ReadInt32();
        var result = new List<ClientQuotaDescription>(count);

        for (int i = 0; i < count; i++)
        {
            var clientId = reader.ReadString() ?? string.Empty;
            var produceRate = reader.ReadInt64();
            var fetchRate = reader.ReadInt64();
            var produceTokens = reader.ReadInt64();
            var fetchTokens = reader.ReadInt64();
            var isThrottled = reader.ReadUInt8() != 0;
            var lastActivityMs = reader.ReadInt64();
            result.Add(new ClientQuotaDescription(
                clientId, produceRate, fetchRate,
                produceTokens, fetchTokens, isThrottled, lastActivityMs));
        }

        return result;
    }

    #endregion

    #region ACL Operations

    /// <summary>
    /// Describe ACLs matching the given filter criteria.
    /// </summary>
    public async Task<AclDescribeResult> DescribeAclsAsync(
        AclResourceType resourceType = AclResourceType.Any,
        string? resourceName = null,
        AclPatternType patternType = AclPatternType.Any,
        string? principal = null,
        string? host = null,
        AclOperation operation = AclOperation.Any,
        AclPermission permission = AclPermission.Any,
        CancellationToken cancellationToken = default)
    {
        var payloadBuffer = new byte[1024];
        var writer = new SurgewavePayloadWriter(payloadBuffer);

        writer.WriteUInt8((byte)resourceType);
        writer.WriteString(resourceName);
        writer.WriteUInt8((byte)patternType);
        writer.WriteString(principal);
        writer.WriteString(host);
        writer.WriteUInt8((byte)operation);
        writer.WriteUInt8((byte)permission);

        var (header, payload) = await _client.SendRequestAsync(
            SurgewaveOpCode.DescribeAcls,
            payloadBuffer.AsMemory(0, writer.Position),
            cancellationToken);

        var reader = new SurgewavePayloadReader(payload.Span);
        var errorCode = (SurgewaveErrorCode)reader.ReadUInt16();
        var aclCount = reader.ReadInt32();
        var acls = new List<AclEntry>(aclCount);

        for (int i = 0; i < aclCount; i++)
        {
            acls.Add(new AclEntry
            {
                ResourceType = (AclResourceType)reader.ReadUInt8(),
                ResourceName = reader.ReadString() ?? string.Empty,
                PatternType = (AclPatternType)reader.ReadUInt8(),
                Principal = reader.ReadString() ?? string.Empty,
                Host = reader.ReadString() ?? string.Empty,
                Operation = (AclOperation)reader.ReadUInt8(),
                Permission = (AclPermission)reader.ReadUInt8()
            });
        }

        return new AclDescribeResult(errorCode, acls);
    }

    /// <summary>
    /// Create new ACL entries.
    /// </summary>
    public async Task<List<AclCreateResult>> CreateAclsAsync(
        List<AclEntry> entries,
        CancellationToken cancellationToken = default)
    {
        var payloadBuffer = new byte[4096];
        var writer = new SurgewavePayloadWriter(payloadBuffer);

        writer.WriteInt32(entries.Count);
        foreach (var entry in entries)
        {
            writer.WriteUInt8((byte)entry.ResourceType);
            writer.WriteString(entry.ResourceName);
            writer.WriteUInt8((byte)entry.PatternType);
            writer.WriteString(entry.Principal);
            writer.WriteString(entry.Host);
            writer.WriteUInt8((byte)entry.Operation);
            writer.WriteUInt8((byte)entry.Permission);
        }

        var (header, payload) = await _client.SendRequestAsync(
            SurgewaveOpCode.CreateAcls,
            payloadBuffer.AsMemory(0, writer.Position),
            cancellationToken);

        var reader = new SurgewavePayloadReader(payload.Span);
        var resultCount = reader.ReadInt32();
        var results = new List<AclCreateResult>(resultCount);

        for (int i = 0; i < resultCount; i++)
        {
            var errorCode = (SurgewaveErrorCode)reader.ReadUInt16();
            var errorMessage = reader.ReadString();
            results.Add(new AclCreateResult(errorCode, errorMessage));
        }

        return results;
    }

    /// <summary>
    /// Delete ACLs matching the given filter criteria.
    /// </summary>
    public async Task<AclDeleteResult> DeleteAclsAsync(
        AclResourceType resourceType = AclResourceType.Any,
        string? resourceName = null,
        AclPatternType patternType = AclPatternType.Any,
        string? principal = null,
        string? host = null,
        AclOperation operation = AclOperation.Any,
        AclPermission permission = AclPermission.Any,
        CancellationToken cancellationToken = default)
    {
        var payloadBuffer = new byte[1024];
        var writer = new SurgewavePayloadWriter(payloadBuffer);

        writer.WriteUInt8((byte)resourceType);
        writer.WriteString(resourceName);
        writer.WriteUInt8((byte)patternType);
        writer.WriteString(principal);
        writer.WriteString(host);
        writer.WriteUInt8((byte)operation);
        writer.WriteUInt8((byte)permission);

        var (header, payload) = await _client.SendRequestAsync(
            SurgewaveOpCode.DeleteAcls,
            payloadBuffer.AsMemory(0, writer.Position),
            cancellationToken);

        var reader = new SurgewavePayloadReader(payload.Span);
        var errorCode = (SurgewaveErrorCode)reader.ReadUInt16();
        var deletedCount = reader.ReadInt32();
        var deletedAcls = new List<AclEntry>(deletedCount);

        for (int i = 0; i < deletedCount; i++)
        {
            deletedAcls.Add(new AclEntry
            {
                ResourceType = (AclResourceType)reader.ReadUInt8(),
                ResourceName = reader.ReadString() ?? string.Empty,
                PatternType = (AclPatternType)reader.ReadUInt8(),
                Principal = reader.ReadString() ?? string.Empty,
                Host = reader.ReadString() ?? string.Empty,
                Operation = (AclOperation)reader.ReadUInt8(),
                Permission = (AclPermission)reader.ReadUInt8()
            });
        }

        return new AclDeleteResult(errorCode, deletedAcls);
    }

    /// <summary>
    /// Start building an ACL entry with fluent API.
    /// </summary>
    public AclBuilder CreateAcl() => new(_client);

    #endregion

    #region Leader Election Operations

    /// <summary>
    /// Trigger leader election for specified partitions.
    /// </summary>
    public async Task<List<ElectionResult>> ElectLeaderAsync(
        List<(string Topic, int Partition)> partitions,
        ElectionType electionType = ElectionType.Preferred,
        CancellationToken cancellationToken = default)
    {
        var payloadBuffer = new byte[4096];
        var writer = new SurgewavePayloadWriter(payloadBuffer);

        writer.WriteUInt8((byte)electionType);
        writer.WriteInt32(partitions.Count);
        foreach (var (topic, partition) in partitions)
        {
            writer.WriteString(topic);
            writer.WriteInt32(partition);
        }

        var (header, payload) = await _client.SendRequestAsync(
            SurgewaveOpCode.ElectLeader,
            payloadBuffer.AsMemory(0, writer.Position),
            cancellationToken);

        var reader = new SurgewavePayloadReader(payload.Span);
        var resultCount = reader.ReadInt32();
        var results = new List<ElectionResult>(resultCount);

        for (int i = 0; i < resultCount; i++)
        {
            var topic = reader.ReadString() ?? string.Empty;
            var partition = reader.ReadInt32();
            var errorCode = (SurgewaveErrorCode)reader.ReadUInt16();
            var errorMessage = reader.ReadString();
            results.Add(new ElectionResult(topic, partition, errorCode, errorMessage));
        }

        return results;
    }

    #endregion

    #region Broker Config Operations

    /// <summary>
    /// Describe broker configuration.
    /// </summary>
    public async Task<Dictionary<string, BrokerConfigEntry>> DescribeBrokerConfigAsync(
        int brokerId,
        List<string>? configKeys = null,
        CancellationToken cancellationToken = default)
    {
        var payloadBuffer = new byte[4096];
        var writer = new SurgewavePayloadWriter(payloadBuffer);

        writer.WriteInt32(brokerId);
        if (configKeys != null)
        {
            writer.WriteInt32(configKeys.Count);
            foreach (var key in configKeys)
            {
                writer.WriteString(key);
            }
        }
        else
        {
            writer.WriteInt32(0);
        }

        var (header, payload) = await _client.SendRequestAsync(
            SurgewaveOpCode.DescribeBrokerConfig,
            payloadBuffer.AsMemory(0, writer.Position),
            cancellationToken);

        var reader = new SurgewavePayloadReader(payload.Span);
        var errorCode = (SurgewaveErrorCode)reader.ReadUInt16();

        if (errorCode != SurgewaveErrorCode.None)
        {
            throw new InvalidOperationException($"DescribeBrokerConfig failed: {errorCode}");
        }

        var configCount = reader.ReadInt32();
        var configs = new Dictionary<string, BrokerConfigEntry>(configCount);

        for (int i = 0; i < configCount; i++)
        {
            var name = reader.ReadString() ?? string.Empty;
            var value = reader.ReadString() ?? string.Empty;
            var isReadOnly = reader.ReadUInt8() != 0;
            var isDefault = reader.ReadUInt8() != 0;
            var isSensitive = reader.ReadUInt8() != 0;
            configs[name] = new BrokerConfigEntry(name, value, isReadOnly, isDefault, isSensitive);
        }

        return configs;
    }

    /// <summary>
    /// Alter broker configuration (for dynamic configs only).
    /// </summary>
    public async Task<SurgewaveErrorCode> AlterBrokerConfigAsync(
        int brokerId,
        Dictionary<string, string?> configUpdates,
        CancellationToken cancellationToken = default)
    {
        var payloadBuffer = new byte[4096];
        var writer = new SurgewavePayloadWriter(payloadBuffer);

        writer.WriteInt32(brokerId);
        writer.WriteInt32(configUpdates.Count);
        foreach (var (key, value) in configUpdates)
        {
            writer.WriteString(key);
            writer.WriteString(value); // null = delete config
        }

        var (header, payload) = await _client.SendRequestAsync(
            SurgewaveOpCode.AlterBrokerConfig,
            payloadBuffer.AsMemory(0, writer.Position),
            cancellationToken);

        var reader = new SurgewavePayloadReader(payload.Span);
        return (SurgewaveErrorCode)reader.ReadUInt16();
    }

    #endregion
}
