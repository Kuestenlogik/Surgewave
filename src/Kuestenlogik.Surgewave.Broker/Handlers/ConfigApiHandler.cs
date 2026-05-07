using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

namespace Kuestenlogik.Surgewave.Broker.Handlers;

/// <summary>
/// Handler for configuration APIs: DescribeConfigs, AlterConfigs
/// </summary>
public sealed class ConfigApiHandler : IKafkaRequestHandler
{
    private readonly BrokerConfig _config;
    private readonly DynamicBrokerConfig _dynamicConfig;
    private readonly LogManager _logManager;

    private static readonly HashSet<string> ValidTopicConfigs =
    [
        "cleanup.policy", "retention.ms", "retention.bytes",
        "segment.bytes", "segment.ms", "min.insync.replicas", "max.message.bytes",
        "compression.type", "delete.retention.ms", "file.delete.delay.ms",
        "flush.messages", "flush.ms", "index.interval.bytes",
        "message.timestamp.type", "message.timestamp.difference.max.ms",
        "preallocate", "unclean.leader.election.enable"
    ];

    public IEnumerable<ApiKey> SupportedApiKeys => [ApiKey.DescribeConfigs, ApiKey.AlterConfigs, ApiKey.IncrementalAlterConfigs];

    public ConfigApiHandler(BrokerConfig config, DynamicBrokerConfig dynamicConfig, LogManager logManager)
    {
        _config = config;
        _dynamicConfig = dynamicConfig;
        _logManager = logManager;
    }

    public Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult<KafkaResponse>(request switch
        {
            DescribeConfigsRequest describeConfigsRequest => HandleDescribeConfigs(describeConfigsRequest),
            AlterConfigsRequest alterConfigsRequest => HandleAlterConfigs(alterConfigsRequest),
            IncrementalAlterConfigsRequest incrementalRequest => HandleIncrementalAlterConfigs(incrementalRequest),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by ConfigApiHandler")
        });
    }

    private DescribeConfigsResponse HandleDescribeConfigs(DescribeConfigsRequest request)
    {
        var results = new List<DescribeConfigsResponse.DescribeConfigsResult>();

        foreach (var resource in request.Resources)
        {
            var configs = new List<DescribeConfigsResponse.ConfigEntry>();

            if (resource.ResourceType == ConfigResourceType.Topic)
            {
                var metadata = _logManager.GetTopicMetadata(resource.ResourceName);
                if (metadata == null)
                {
                    results.Add(new DescribeConfigsResponse.DescribeConfigsResult { ErrorCode = ErrorCode.UnknownTopicOrPartition, ErrorMessage = $"Topic '{resource.ResourceName}' does not exist", ResourceType = resource.ResourceType, ResourceName = resource.ResourceName, Configs = [] });
                    continue;
                }
                configs.AddRange(GetTopicConfigs(metadata, resource.ConfigurationKeys));
            }
            else if (resource.ResourceType == ConfigResourceType.Broker)
            {
                configs.AddRange(GetBrokerConfigs(resource.ConfigurationKeys));
            }
            else
            {
                results.Add(new DescribeConfigsResponse.DescribeConfigsResult { ErrorCode = ErrorCode.InvalidConfig, ErrorMessage = $"Unsupported resource type: {resource.ResourceType}", ResourceType = resource.ResourceType, ResourceName = resource.ResourceName, Configs = [] });
                continue;
            }

            results.Add(new DescribeConfigsResponse.DescribeConfigsResult { ErrorCode = ErrorCode.None, ResourceType = resource.ResourceType, ResourceName = resource.ResourceName, Configs = configs });
        }

        return new DescribeConfigsResponse { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion, Results = results };
    }

    private AlterConfigsResponse HandleAlterConfigs(AlterConfigsRequest request)
    {
        var results = new List<AlterConfigsResponse.AlterConfigsResult>();

        foreach (var resource in request.Resources)
        {
            if (resource.ResourceType == ConfigResourceType.Topic)
            {
                var metadata = _logManager.GetTopicMetadata(resource.ResourceName);
                if (metadata == null)
                {
                    results.Add(new AlterConfigsResponse.AlterConfigsResult { ErrorCode = ErrorCode.UnknownTopicOrPartition, ErrorMessage = $"Topic '{resource.ResourceName}' does not exist", ResourceType = resource.ResourceType, ResourceName = resource.ResourceName });
                    continue;
                }

                if (!request.ValidateOnly)
                {
                    var configUpdates = new Dictionary<string, string>();
                    foreach (var config in resource.Configs)
                    {
                        if (config.Value != null && ValidTopicConfigs.Contains(config.Name))
                            configUpdates[config.Name] = config.Value;
                    }
                    if (configUpdates.Count > 0)
                        _logManager.UpdateTopicConfig(resource.ResourceName, configUpdates);
                }

                results.Add(new AlterConfigsResponse.AlterConfigsResult { ErrorCode = ErrorCode.None, ResourceType = resource.ResourceType, ResourceName = resource.ResourceName });
            }
            else if (resource.ResourceType == ConfigResourceType.Broker)
            {
                // Support runtime broker config changes for dynamic configs
                var errors = new List<string>();

                if (!request.ValidateOnly)
                {
                    foreach (var config in resource.Configs)
                    {
                        var error = _dynamicConfig.SetConfig(config.Name, config.Value);
                        if (error != null)
                        {
                            errors.Add(error);
                        }
                    }
                }
                else
                {
                    // Validate only mode - check if configs are valid without applying
                    foreach (var config in resource.Configs)
                    {
                        if (DynamicBrokerConfig.ReadOnlyConfigKeys.Contains(config.Name))
                        {
                            errors.Add($"Config '{config.Name}' is read-only and requires broker restart");
                        }
                        else if (!DynamicBrokerConfig.DynamicConfigKeys.Contains(config.Name))
                        {
                            errors.Add($"Config '{config.Name}' is not a recognized dynamic broker config");
                        }
                    }
                }

                if (errors.Count > 0)
                {
                    results.Add(new AlterConfigsResponse.AlterConfigsResult
                    {
                        ErrorCode = ErrorCode.InvalidConfig,
                        ErrorMessage = string.Join("; ", errors),
                        ResourceType = resource.ResourceType,
                        ResourceName = resource.ResourceName
                    });
                }
                else
                {
                    results.Add(new AlterConfigsResponse.AlterConfigsResult
                    {
                        ErrorCode = ErrorCode.None,
                        ResourceType = resource.ResourceType,
                        ResourceName = resource.ResourceName
                    });
                }
            }
            else
            {
                results.Add(new AlterConfigsResponse.AlterConfigsResult { ErrorCode = ErrorCode.InvalidConfig, ErrorMessage = $"Unsupported resource type: {resource.ResourceType}", ResourceType = resource.ResourceType, ResourceName = resource.ResourceName });
            }
        }

        return new AlterConfigsResponse { CorrelationId = request.CorrelationId, ApiVersion = request.ApiVersion, Results = results };
    }

    private List<DescribeConfigsResponse.ConfigEntry> GetTopicConfigs(TopicMetadata metadata, List<string>? requestedConfigs)
    {
        var defaultConfigs = new Dictionary<string, string>
        {
            // Retention
            ["cleanup.policy"] = "delete",
            ["retention.ms"] = "-1",
            ["retention.bytes"] = "-1",
            ["delete.retention.ms"] = "86400000",

            // Segments
            ["segment.bytes"] = "1073741824",
            ["segment.ms"] = "604800000",
            ["segment.index.bytes"] = "10485760",
            ["segment.jitter.ms"] = "0",

            // Message limits
            ["max.message.bytes"] = "1048576",
            ["message.timestamp.type"] = "CreateTime",
            ["message.timestamp.difference.max.ms"] = "9223372036854775807",

            // Replication
            ["min.insync.replicas"] = "1",
            ["unclean.leader.election.enable"] = "false",

            // Compression
            ["compression.type"] = "producer",

            // Flush
            ["flush.messages"] = "9223372036854775807",
            ["flush.ms"] = "9223372036854775807",

            // Index
            ["index.interval.bytes"] = "4096",

            // File management
            ["file.delete.delay.ms"] = "60000",
            ["preallocate"] = "false",

            // Message format
            ["message.format.version"] = "3.0-IV1",
            ["message.downconversion.enable"] = "true"
        };

        var allConfigs = new Dictionary<string, (string value, ConfigSource source)>();
        foreach (var (name, defaultValue) in defaultConfigs)
        {
            allConfigs[name] = metadata.Config.TryGetValue(name, out var configValue)
                ? (configValue, ConfigSource.DynamicTopicConfig)
                : (defaultValue, ConfigSource.DefaultConfig);
        }

        IEnumerable<string> configsToReturn = requestedConfigs == null || requestedConfigs.Count == 0 ? allConfigs.Keys : requestedConfigs;
        return configsToReturn.Where(n => allConfigs.ContainsKey(n)).Select(name =>
        {
            var config = allConfigs[name];
            return new DescribeConfigsResponse.ConfigEntry { Name = name, Value = config.value, ReadOnly = false, IsDefault = config.source == ConfigSource.DefaultConfig, IsSensitive = false, ConfigSource = config.source, ConfigType = GetConfigType(name) };
        }).ToList();
    }

    private List<DescribeConfigsResponse.ConfigEntry> GetBrokerConfigs(List<string>? requestedConfigs)
    {
        var retentionMs = _config.LogRetentionHours == -1 ? -1 : _config.LogRetentionHours * 3600000L;
        var listeners = $"PLAINTEXT://{_config.Host}:{_config.Port}";

        var allConfigs = new Dictionary<string, (string value, ConfigSource source, bool readOnly)>
        {
            // Core broker identity
            ["broker.id"] = (_config.BrokerId.ToString(), ConfigSource.StaticBrokerConfig, true),
            ["node.id"] = (_config.BrokerId.ToString(), ConfigSource.StaticBrokerConfig, true),

            // Network settings
            ["listeners"] = (listeners, ConfigSource.StaticBrokerConfig, true),
            ["advertised.listeners"] = (listeners, ConfigSource.StaticBrokerConfig, true),
            ["listener.security.protocol.map"] = ("PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL", ConfigSource.DefaultConfig, false),
            ["socket.send.buffer.bytes"] = (_config.SocketSendBufferBytes.ToString(), ConfigSource.StaticBrokerConfig, false),
            ["socket.receive.buffer.bytes"] = (_config.SocketReceiveBufferBytes.ToString(), ConfigSource.StaticBrokerConfig, false),
            ["socket.request.max.bytes"] = (_config.MaxRequestSize.ToString(), ConfigSource.StaticBrokerConfig, false),
            ["max.connections.per.ip"] = (_config.MaxConnectionsPerIp.ToString(), ConfigSource.StaticBrokerConfig, false),

            // Log/storage settings
            ["log.dirs"] = (_config.DataDirectory, ConfigSource.StaticBrokerConfig, true),
            ["log.dir"] = (_config.DataDirectory, ConfigSource.StaticBrokerConfig, true),
            ["log.segment.bytes"] = (_config.LogSegmentBytes.ToString(), ConfigSource.StaticBrokerConfig, false),
            ["log.retention.hours"] = (_config.LogRetentionHours.ToString(), ConfigSource.StaticBrokerConfig, false),
            ["log.retention.ms"] = (retentionMs.ToString(), ConfigSource.StaticBrokerConfig, false),
            ["log.retention.bytes"] = (_config.LogRetentionBytes.ToString(), ConfigSource.StaticBrokerConfig, false),

            // Topic defaults
            ["num.partitions"] = (_config.DefaultNumPartitions.ToString(), ConfigSource.StaticBrokerConfig, false),
            ["default.replication.factor"] = (_config.DefaultReplicationFactor.ToString(), ConfigSource.StaticBrokerConfig, false),
            ["auto.create.topics.enable"] = (_config.AutoCreateTopics.ToString().ToLowerInvariant(), ConfigSource.StaticBrokerConfig, false),
            ["min.insync.replicas"] = (_config.MinInSyncReplicas.ToString(), ConfigSource.StaticBrokerConfig, false),

            // Message settings
            ["message.max.bytes"] = (_config.MaxRequestSize.ToString(), ConfigSource.StaticBrokerConfig, false),
            ["replica.fetch.max.bytes"] = (_config.ReplicaFetchMaxBytes.ToString(), ConfigSource.StaticBrokerConfig, false),

            // Replication settings
            ["replica.lag.time.max.ms"] = (_config.ReplicaLagTimeMaxMs.ToString(), ConfigSource.StaticBrokerConfig, false),
            ["replica.lag.max.messages"] = (_config.ReplicaLagMaxMessages.ToString(), ConfigSource.StaticBrokerConfig, false),
            ["replica.fetch.wait.max.ms"] = (_config.ReplicaFetchWaitMaxMs.ToString(), ConfigSource.StaticBrokerConfig, false),

            // Controller/leader settings
            ["auto.leader.rebalance.enable"] = (_config.AllowAutoLeaderRebalance.ToString().ToLowerInvariant(), ConfigSource.StaticBrokerConfig, false),
            ["leader.imbalance.check.interval.seconds"] = (_config.LeaderImbalanceCheckIntervalSeconds.ToString(), ConfigSource.StaticBrokerConfig, false),
            ["controlled.shutdown.max.retries"] = (_config.ControlledShutdownMaxRetries.ToString(), ConfigSource.StaticBrokerConfig, false),

            // Consumer group settings
            ["group.initial.rebalance.delay.ms"] = ("0", ConfigSource.DefaultConfig, false),
            ["group.min.session.timeout.ms"] = ("6000", ConfigSource.DefaultConfig, false),
            ["group.max.session.timeout.ms"] = ("1800000", ConfigSource.DefaultConfig, false),

            // Offsets topic settings
            ["offsets.topic.replication.factor"] = ("1", ConfigSource.DefaultConfig, false),
            ["offsets.topic.num.partitions"] = ("50", ConfigSource.DefaultConfig, false),
            ["offsets.retention.minutes"] = ("10080", ConfigSource.DefaultConfig, false),

            // Transaction settings
            ["transaction.state.log.replication.factor"] = ("1", ConfigSource.DefaultConfig, false),
            ["transaction.state.log.num.partitions"] = ("50", ConfigSource.DefaultConfig, false),
            ["transactional.id.expiration.ms"] = ("604800000", ConfigSource.DefaultConfig, false),

            // Security settings
            ["security.inter.broker.protocol"] = (_config.Security.SaslEnabled ? "SASL_PLAINTEXT" : "PLAINTEXT", ConfigSource.StaticBrokerConfig, false),
            ["sasl.enabled.mechanisms"] = (string.Join(",", _config.Security.SaslMechanisms), ConfigSource.StaticBrokerConfig, false),
            ["ssl.endpoint.identification.algorithm"] = ("https", ConfigSource.DefaultConfig, false),

            // Quota settings
            ["quota.producer.default"] = (_config.Quotas.ProducerBytesPerSecond.ToString(), ConfigSource.StaticBrokerConfig, false),
            ["quota.consumer.default"] = (_config.Quotas.ConsumerBytesPerSecond.ToString(), ConfigSource.StaticBrokerConfig, false),

            // Cluster settings
            ["cluster.id"] = (_config.ClusterId ?? "surgewave-cluster", ConfigSource.StaticBrokerConfig, true),
            ["broker.rack"] = (_config.Rack ?? "", ConfigSource.StaticBrokerConfig, false),

            // Compression
            ["compression.type"] = ("producer", ConfigSource.DefaultConfig, false),

            // Background threads
            ["background.threads"] = ("10", ConfigSource.DefaultConfig, false),
            ["num.io.threads"] = ("8", ConfigSource.DefaultConfig, false),
            ["num.network.threads"] = ("3", ConfigSource.DefaultConfig, false),
            ["num.replica.fetchers"] = ("1", ConfigSource.DefaultConfig, false),

            // Request handling
            ["queued.max.requests"] = ("500", ConfigSource.DefaultConfig, false),
            ["request.timeout.ms"] = ("30000", ConfigSource.DefaultConfig, false),

            // Metadata
            ["metadata.max.age.ms"] = ("300000", ConfigSource.DefaultConfig, false),

            // Delete topic
            ["delete.topic.enable"] = ("true", ConfigSource.DefaultConfig, false),

            // Unclean leader election
            ["unclean.leader.election.enable"] = ("false", ConfigSource.DefaultConfig, false)
        };

        IEnumerable<string> configsToReturn = requestedConfigs == null || requestedConfigs.Count == 0 ? allConfigs.Keys : requestedConfigs;
        return configsToReturn.Where(n => allConfigs.ContainsKey(n)).Select(name =>
        {
            var config = allConfigs[name];

            // Check for dynamic override
            var effectiveValue = config.value;
            var effectiveSource = config.source;
            if (_dynamicConfig.IsDynamicallySet(name))
            {
                effectiveValue = _dynamicConfig.GetConfig(name) ?? config.value;
                effectiveSource = ConfigSource.DynamicBrokerConfig;
            }

            return new DescribeConfigsResponse.ConfigEntry
            {
                Name = name,
                Value = effectiveValue,
                ReadOnly = config.readOnly,
                IsDefault = effectiveSource == ConfigSource.DefaultConfig,
                IsSensitive = false,
                ConfigSource = effectiveSource,
                ConfigType = GetConfigType(name)
            };
        }).ToList();
    }

    private IncrementalAlterConfigsResponse HandleIncrementalAlterConfigs(IncrementalAlterConfigsRequest request)
    {
        var responses = new List<IncrementalAlterConfigsResponse.AlterConfigsResourceResponse>();

        foreach (var resource in request.Resources)
        {
            var resourceType = (ConfigResourceType)resource.ResourceType;

            if (resourceType == ConfigResourceType.Topic)
            {
                var metadata = _logManager.GetTopicMetadata(resource.ResourceName);
                if (metadata == null)
                {
                    responses.Add(new IncrementalAlterConfigsResponse.AlterConfigsResourceResponse
                    {
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        ErrorMessage = $"Topic '{resource.ResourceName}' does not exist",
                        ResourceType = resource.ResourceType,
                        ResourceName = resource.ResourceName
                    });
                    continue;
                }

                if (!request.ValidateOnly)
                {
                    var configUpdates = new Dictionary<string, string>();
                    var currentConfig = metadata.Config ?? new Dictionary<string, string>();
                    var errors = new List<string>();

                    foreach (var config in resource.Configs)
                    {
                        if (!ValidTopicConfigs.Contains(config.Name))
                        {
                            errors.Add($"Unknown topic config: {config.Name}");
                            continue;
                        }

                        switch (config.ConfigOperation)
                        {
                            case 0: // SET
                                if (config.Value != null)
                                    configUpdates[config.Name] = config.Value;
                                break;

                            case 1: // DELETE - reset to default by removing override
                                configUpdates[config.Name] = ""; // Empty value signals removal
                                break;

                            case 2: // APPEND - for list-type configs
                                if (config.Value != null)
                                {
                                    var existing = currentConfig.TryGetValue(config.Name, out var val) ? val : "";
                                    configUpdates[config.Name] = string.IsNullOrEmpty(existing)
                                        ? config.Value
                                        : $"{existing},{config.Value}";
                                }
                                break;

                            case 3: // SUBTRACT - for list-type configs
                                if (config.Value != null && currentConfig.TryGetValue(config.Name, out var currentVal))
                                {
                                    var items = currentVal.Split(',').Where(i => i != config.Value);
                                    configUpdates[config.Name] = string.Join(",", items);
                                }
                                break;

                            default:
                                errors.Add($"Unknown operation {config.ConfigOperation} for config {config.Name}");
                                break;
                        }
                    }

                    if (errors.Count > 0)
                    {
                        responses.Add(new IncrementalAlterConfigsResponse.AlterConfigsResourceResponse
                        {
                            ErrorCode = ErrorCode.InvalidConfig,
                            ErrorMessage = string.Join("; ", errors),
                            ResourceType = resource.ResourceType,
                            ResourceName = resource.ResourceName
                        });
                        continue;
                    }

                    if (configUpdates.Count > 0)
                        _logManager.UpdateTopicConfig(resource.ResourceName, configUpdates);
                }

                responses.Add(new IncrementalAlterConfigsResponse.AlterConfigsResourceResponse
                {
                    ErrorCode = ErrorCode.None,
                    ResourceType = resource.ResourceType,
                    ResourceName = resource.ResourceName
                });
            }
            else if (resourceType == ConfigResourceType.Broker)
            {
                var errors = new List<string>();

                if (!request.ValidateOnly)
                {
                    foreach (var config in resource.Configs)
                    {
                        switch (config.ConfigOperation)
                        {
                            case 0: // SET
                                var error = _dynamicConfig.SetConfig(config.Name, config.Value);
                                if (error != null)
                                    errors.Add(error);
                                break;

                            case 1: // DELETE - reset to default
                                _dynamicConfig.SetConfig(config.Name, null);
                                break;

                            case 2: // APPEND
                            case 3: // SUBTRACT
                                // For broker configs, APPEND/SUBTRACT only make sense for list configs
                                var currentValue = _dynamicConfig.GetConfig(config.Name);
                                if (currentValue != null && config.Value != null)
                                {
                                    if (config.ConfigOperation == 2)
                                    {
                                        var newValue = string.IsNullOrEmpty(currentValue)
                                            ? config.Value
                                            : $"{currentValue},{config.Value}";
                                        _dynamicConfig.SetConfig(config.Name, newValue);
                                    }
                                    else
                                    {
                                        var items = currentValue.Split(',').Where(i => i != config.Value);
                                        _dynamicConfig.SetConfig(config.Name, string.Join(",", items));
                                    }
                                }
                                break;

                            default:
                                errors.Add($"Unknown operation {config.ConfigOperation} for config {config.Name}");
                                break;
                        }
                    }
                }
                else
                {
                    foreach (var config in resource.Configs)
                    {
                        if (DynamicBrokerConfig.ReadOnlyConfigKeys.Contains(config.Name))
                            errors.Add($"Config '{config.Name}' is read-only");
                        else if (!DynamicBrokerConfig.DynamicConfigKeys.Contains(config.Name))
                            errors.Add($"Config '{config.Name}' is not a recognized dynamic broker config");
                    }
                }

                if (errors.Count > 0)
                {
                    responses.Add(new IncrementalAlterConfigsResponse.AlterConfigsResourceResponse
                    {
                        ErrorCode = ErrorCode.InvalidConfig,
                        ErrorMessage = string.Join("; ", errors),
                        ResourceType = resource.ResourceType,
                        ResourceName = resource.ResourceName
                    });
                }
                else
                {
                    responses.Add(new IncrementalAlterConfigsResponse.AlterConfigsResourceResponse
                    {
                        ErrorCode = ErrorCode.None,
                        ResourceType = resource.ResourceType,
                        ResourceName = resource.ResourceName
                    });
                }
            }
            else
            {
                responses.Add(new IncrementalAlterConfigsResponse.AlterConfigsResourceResponse
                {
                    ErrorCode = ErrorCode.InvalidConfig,
                    ErrorMessage = $"Unsupported resource type: {resource.ResourceType}",
                    ResourceType = resource.ResourceType,
                    ResourceName = resource.ResourceName
                });
            }
        }

        return new IncrementalAlterConfigsResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            Responses = responses
        };
    }

    private static ConfigType GetConfigType(string configName) => configName switch
    {
        // Numeric/long configs
        "retention.ms" or "retention.bytes" or "segment.bytes" or "segment.ms" or "max.message.bytes"
            or "min.insync.replicas" or "broker.id" or "node.id" or "num.partitions" or "default.replication.factor"
            or "socket.send.buffer.bytes" or "socket.receive.buffer.bytes" or "socket.request.max.bytes"
            or "max.connections.per.ip" or "log.segment.bytes" or "log.retention.hours" or "log.retention.ms"
            or "log.retention.bytes" or "message.max.bytes" or "replica.fetch.max.bytes"
            or "replica.lag.time.max.ms" or "replica.lag.max.messages" or "replica.fetch.wait.max.ms"
            or "leader.imbalance.check.interval.seconds" or "controlled.shutdown.max.retries"
            or "group.initial.rebalance.delay.ms" or "group.min.session.timeout.ms" or "group.max.session.timeout.ms"
            or "offsets.topic.replication.factor" or "offsets.topic.num.partitions" or "offsets.retention.minutes"
            or "transaction.state.log.replication.factor" or "transaction.state.log.num.partitions"
            or "transactional.id.expiration.ms" or "quota.producer.default" or "quota.consumer.default"
            or "background.threads" or "num.io.threads" or "num.network.threads" or "num.replica.fetchers"
            or "queued.max.requests" or "request.timeout.ms" or "metadata.max.age.ms"
            => ConfigType.Long,

        // Boolean configs
        "auto.create.topics.enable" or "auto.leader.rebalance.enable" or "delete.topic.enable"
            or "unclean.leader.election.enable"
            => ConfigType.Boolean,

        // String configs (default)
        _ => ConfigType.String
    };
}
