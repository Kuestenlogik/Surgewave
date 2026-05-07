using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol admin operations: leader election and broker config.
/// </summary>
public sealed class NativeAdminHandler : INativeRequestHandler
{
    private readonly LogManager _logManager;
    private readonly BrokerConfig _config;

    public IEnumerable<SurgewaveOpCode> SupportedOpCodes =>
    [
        SurgewaveOpCode.ElectLeader,
        SurgewaveOpCode.DescribeBrokerConfig,
        SurgewaveOpCode.AlterBrokerConfig
    ];

    public NativeAdminHandler(LogManager logManager, BrokerConfig config)
    {
        _logManager = logManager;
        _config = config;
    }

    public Task HandleAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        return context.Header.OpCode switch
        {
            SurgewaveOpCode.ElectLeader => HandleElectLeaderAsync(context, payload, cancellationToken),
            SurgewaveOpCode.DescribeBrokerConfig => HandleDescribeBrokerConfigAsync(context, payload, cancellationToken),
            SurgewaveOpCode.AlterBrokerConfig => HandleAlterBrokerConfigAsync(context, payload, cancellationToken),
            _ => Task.CompletedTask
        };
    }

    private async Task HandleElectLeaderAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        var reader = new SurgewavePayloadReader(payload.Span);
        var electionType = reader.ReadUInt8(); // 0 = preferred, 1 = unclean
        var partitionCount = reader.ReadInt32();

        var results = new List<(string Topic, int Partition, SurgewaveErrorCode ErrorCode, string? ErrorMessage)>();

        for (int i = 0; i < partitionCount; i++)
        {
            var topic = reader.ReadString() ?? string.Empty;
            var partition = reader.ReadInt32();

            try
            {
                var metadata = _logManager.GetTopicMetadata(topic);
                if (metadata == null)
                {
                    results.Add((topic, partition, SurgewaveErrorCode.TopicNotFound, $"Topic '{topic}' not found"));
                    continue;
                }

                if (partition < 0 || partition >= metadata.PartitionCount)
                {
                    results.Add((topic, partition, SurgewaveErrorCode.PartitionNotFound, $"Partition {partition} not found"));
                    continue;
                }

                // In a single-broker setup, we're always the leader
                // In a multi-broker setup, this would trigger a leader election
                // For now, we simulate successful election
                if (electionType == 0)
                {
                    // Preferred replica election - check if we're the preferred replica
                    results.Add((topic, partition, SurgewaveErrorCode.None, null));
                }
                else
                {
                    // Unclean election - always succeeds in single-broker mode
                    results.Add((topic, partition, SurgewaveErrorCode.None, null));
                }
            }
            catch (Exception ex)
            {
                results.Add((topic, partition, SurgewaveErrorCode.UnknownError, ex.Message));
            }
        }

        using var writer = new BigEndianWriter();
        writer.Write(results.Count);
        foreach (var (topic, partition, errorCode, errorMessage) in results)
        {
            writer.WriteNullableString(topic);
            writer.Write(partition);
            writer.Write((ushort)errorCode);
            writer.WriteNullableString(errorMessage);
        }

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.ElectLeader,
            SurgewaveErrorCode.None, writer.AsMemory(), cancellationToken);
    }

    private async Task HandleDescribeBrokerConfigAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        try
        {
            var reader = new SurgewavePayloadReader(payload.Span);
            var brokerId = reader.ReadInt32();
            var keyCount = reader.ReadInt32();

            // If brokerId doesn't match this broker, return error
            if (brokerId != _config.BrokerId && brokerId != -1)
            {
                using var errorWriter = new BigEndianWriter();
                errorWriter.Write((ushort)SurgewaveErrorCode.InvalidRequest);
                errorWriter.Write(0);
                await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.DescribeBrokerConfig,
                    SurgewaveErrorCode.InvalidRequest, errorWriter.AsMemory(), cancellationToken);
                return;
            }

            var requestedKeys = new HashSet<string>();
            for (int i = 0; i < keyCount; i++)
            {
                var key = reader.ReadString();
                if (key != null) requestedKeys.Add(key);
            }

            var allConfigs = GetBrokerConfigs();
            var configsToReturn = requestedKeys.Count == 0
                ? allConfigs
                : allConfigs.Where(c => requestedKeys.Contains(c.Key)).ToDictionary(c => c.Key, c => c.Value);

            using var writer = new BigEndianWriter();
            writer.Write((ushort)SurgewaveErrorCode.None);
            writer.Write(configsToReturn.Count);

            foreach (var (name, (value, isReadOnly, isDefault, isSensitive)) in configsToReturn)
            {
                writer.WriteNullableString(name);
                writer.WriteNullableString(isSensitive ? "********" : value);
                writer.Write((byte)(isReadOnly ? 1 : 0));
                writer.Write((byte)(isDefault ? 1 : 0));
                writer.Write((byte)(isSensitive ? 1 : 0));
            }

            await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.DescribeBrokerConfig,
                SurgewaveErrorCode.None, writer.AsMemory(), cancellationToken);
        }
        catch (Exception ex)
        {
            await context.SendErrorAsync(context.Header.RequestId, SurgewaveOpCode.DescribeBrokerConfig,
                SurgewaveErrorCode.UnknownError, ex.Message, cancellationToken);
        }
    }

    private async Task HandleAlterBrokerConfigAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        // Broker config modification is not supported at runtime
        using var writer = new BigEndianWriter();
        writer.Write((ushort)SurgewaveErrorCode.InvalidConfig);

        await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.AlterBrokerConfig,
            SurgewaveErrorCode.InvalidConfig, writer.AsMemory(), cancellationToken);
    }

    private Dictionary<string, (string Value, bool IsReadOnly, bool IsDefault, bool IsSensitive)> GetBrokerConfigs()
    {
        var retentionMs = _config.LogRetentionHours == -1 ? -1 : _config.LogRetentionHours * 3600000L;
        var listeners = $"PLAINTEXT://{_config.Host}:{_config.Port}";

        return new Dictionary<string, (string, bool, bool, bool)>
        {
            // Core broker identity
            ["broker.id"] = (_config.BrokerId.ToString(), true, false, false),
            ["node.id"] = (_config.BrokerId.ToString(), true, false, false),

            // Network settings
            ["listeners"] = (listeners, true, false, false),
            ["advertised.listeners"] = (listeners, true, false, false),
            ["socket.send.buffer.bytes"] = (_config.SocketSendBufferBytes.ToString(), false, false, false),
            ["socket.receive.buffer.bytes"] = (_config.SocketReceiveBufferBytes.ToString(), false, false, false),
            ["socket.request.max.bytes"] = (_config.MaxRequestSize.ToString(), false, false, false),
            ["max.connections.per.ip"] = (_config.MaxConnectionsPerIp.ToString(), false, false, false),

            // Log/storage settings
            ["log.dirs"] = (_config.DataDirectory, true, false, false),
            ["log.dir"] = (_config.DataDirectory, true, false, false),
            ["log.segment.bytes"] = (_config.LogSegmentBytes.ToString(), false, false, false),
            ["log.retention.hours"] = (_config.LogRetentionHours.ToString(), false, false, false),
            ["log.retention.ms"] = (retentionMs.ToString(), false, false, false),
            ["log.retention.bytes"] = (_config.LogRetentionBytes.ToString(), false, false, false),

            // Topic defaults
            ["num.partitions"] = (_config.DefaultNumPartitions.ToString(), false, false, false),
            ["default.replication.factor"] = (_config.DefaultReplicationFactor.ToString(), false, false, false),
            ["auto.create.topics.enable"] = (_config.AutoCreateTopics.ToString().ToLowerInvariant(), false, false, false),
            ["min.insync.replicas"] = (_config.MinInSyncReplicas.ToString(), false, false, false),

            // Message settings
            ["message.max.bytes"] = (_config.MaxRequestSize.ToString(), false, false, false),
            ["replica.fetch.max.bytes"] = (_config.ReplicaFetchMaxBytes.ToString(), false, false, false),

            // Replication settings
            ["replica.lag.time.max.ms"] = (_config.ReplicaLagTimeMaxMs.ToString(), false, false, false),
            ["replica.lag.max.messages"] = (_config.ReplicaLagMaxMessages.ToString(), false, false, false),
            ["replica.fetch.wait.max.ms"] = (_config.ReplicaFetchWaitMaxMs.ToString(), false, false, false),

            // Controller/leader settings
            ["auto.leader.rebalance.enable"] = (_config.AllowAutoLeaderRebalance.ToString().ToLowerInvariant(), false, false, false),
            ["leader.imbalance.check.interval.seconds"] = (_config.LeaderImbalanceCheckIntervalSeconds.ToString(), false, false, false),
            ["controlled.shutdown.max.retries"] = (_config.ControlledShutdownMaxRetries.ToString(), false, false, false),

            // Security settings
            ["security.inter.broker.protocol"] = (_config.Security.SaslEnabled ? "SASL_PLAINTEXT" : "PLAINTEXT", false, false, false),
            ["sasl.enabled.mechanisms"] = (string.Join(",", _config.Security.SaslMechanisms), false, false, false),

            // Quota settings
            ["quota.producer.default"] = (_config.Quotas.ProducerBytesPerSecond.ToString(), false, false, false),
            ["quota.consumer.default"] = (_config.Quotas.ConsumerBytesPerSecond.ToString(), false, false, false),

            // Cluster settings
            ["cluster.id"] = (_config.ClusterId ?? "surgewave-cluster", true, false, false),
            ["broker.rack"] = (_config.Rack ?? "", false, false, false),

            // Native protocol
            ["native.protocol.compression.enabled"] = (_config.NativeProtocolCompressionEnabled.ToString().ToLowerInvariant(), false, false, false),

            // Producer defaults (for client configuration)
            ["producer.batch.size.bytes"] = (_config.ProducerBatchSizeBytes.ToString(), false, false, false),
            ["producer.linger.ms"] = (_config.ProducerLingerMs.ToString(), false, false, false),
            ["producer.max.batch.messages"] = (_config.ProducerMaxBatchMessages.ToString(), false, false, false),
        };
    }
}
