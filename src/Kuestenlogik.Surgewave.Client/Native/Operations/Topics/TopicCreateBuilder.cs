using Kuestenlogik.Surgewave.Client.Validation;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Topics;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Topics;

/// <summary>
/// Fluent builder for topic creation.
/// </summary>
public sealed class TopicCreateBuilder
{
    private readonly SurgewaveNativeClient _client;
    private readonly string _name;
    private int _partitions = 1;
    private short _replicationFactor = 1;
    private readonly Dictionary<string, string> _config = new();

    internal TopicCreateBuilder(SurgewaveNativeClient client, string name)
    {
        _client = client;
        Guard.ValidTopicName(name);
        _name = name;
    }

    /// <summary>
    /// Set the number of partitions.
    /// </summary>
    public TopicCreateBuilder WithPartitions(int partitions)
    {
        Guard.GreaterThan(partitions, 0);
        _partitions = partitions;
        return this;
    }

    /// <summary>
    /// Set the replication factor.
    /// </summary>
    public TopicCreateBuilder WithReplicationFactor(short factor)
    {
        Guard.GreaterThan(factor, (short)0);
        _replicationFactor = factor;
        return this;
    }

    /// <summary>
    /// Add a configuration option.
    /// </summary>
    public TopicCreateBuilder WithConfig(string key, string value)
    {
        _config[key] = value;
        return this;
    }

    /// <summary>
    /// Set retention time.
    /// </summary>
    public TopicCreateBuilder WithRetention(TimeSpan retention)
    {
        _config["retention.ms"] = ((long)retention.TotalMilliseconds).ToString();
        return this;
    }

    /// <summary>
    /// Enable log compaction.
    /// </summary>
    public TopicCreateBuilder WithCompaction()
    {
        _config["cleanup.policy"] = "compact";
        return this;
    }

    /// <summary>
    /// Execute the topic creation.
    /// </summary>
    public async Task ExecuteAsync(CancellationToken cancellationToken = default)
    {
        TopicConfigPayload[]? configs = null;
        if (_config.Count > 0)
        {
            configs = _config.Select(kv => new TopicConfigPayload { Key = kv.Key, Value = kv.Value }).ToArray();
        }

        var request = new CreateTopicRequestPayload
        {
            Name = _name,
            Partitions = _partitions,
            ReplicationFactor = _replicationFactor,
            Configs = configs
        };

        var payloadBuffer = new byte[request.EstimateSize()];
        var writer = new SurgewavePayloadWriter(payloadBuffer);
        request.Write(ref writer);

        await _client.SendRequestAsync(
            SurgewaveOpCode.CreateTopic,
            payloadBuffer.AsMemory(0, writer.Position),
            cancellationToken);
    }
}
