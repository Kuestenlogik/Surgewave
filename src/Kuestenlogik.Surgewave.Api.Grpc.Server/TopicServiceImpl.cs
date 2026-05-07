using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using CoreTopicPartition = Kuestenlogik.Surgewave.Core.Models.TopicPartition;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// gRPC TopicService implementation
/// </summary>
public class TopicServiceImpl : TopicService.TopicServiceBase
{
    private readonly LogManager _logManager;

    public TopicServiceImpl(LogManager logManager)
    {
        _logManager = logManager;
    }

    public override async Task<CreateTopicResponse> CreateTopic(CreateTopicRequest request, ServerCallContext context)
    {
        try
        {
            var config = request.Config.ToDictionary(kv => kv.Key, kv => kv.Value);

            var metadata = await _logManager.CreateTopicAsync(
                request.Topic,
                request.NumPartitions,
                (short)request.ReplicationFactor,
                config,
                context.CancellationToken);

            return new CreateTopicResponse
            {
                TopicInfo = MapTopicInfo(metadata),
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("already exists"))
        {
            return new CreateTopicResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.TopicAlreadyExists,
                    ErrorMessage = ex.Message
                }
            };
        }
        catch (Exception ex)
        {
            return new CreateTopicResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<DeleteTopicResponse> DeleteTopic(DeleteTopicRequest request, ServerCallContext context)
    {
        try
        {
            await _logManager.DeleteTopicAsync(request.Topic, context.CancellationToken);

            return new DeleteTopicResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("does not exist"))
        {
            return new DeleteTopicResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.UnknownTopicOrPartition,
                    ErrorMessage = ex.Message
                }
            };
        }
        catch (Exception ex)
        {
            return new DeleteTopicResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override Task<ListTopicsResponse> ListTopics(ListTopicsRequest request, ServerCallContext context)
    {
        var topicMetadata = _logManager.ListTopics()
            .Where(t => request.IncludeInternal || !t.Name.StartsWith("__", StringComparison.Ordinal));

        var response = new ListTopicsResponse();

        foreach (var t in topicMetadata)
        {
            // Keep backward compatibility with topics field
            response.Topics.Add(t.Name);

            // Add full topic info
            response.TopicInfos.Add(new TopicSummary
            {
                Name = t.Name,
                PartitionCount = t.PartitionCount,
                ReplicationFactor = t.ReplicationFactor,
                IsInternal = t.Name.StartsWith("__", StringComparison.Ordinal) ||
                            t.Name.StartsWith("_surgewave-", StringComparison.Ordinal)
            });
        }

        return Task.FromResult(response);
    }

    public override Task<DescribeTopicResponse> DescribeTopic(DescribeTopicRequest request, ServerCallContext context)
    {
        var response = new DescribeTopicResponse();

        // Support both single topic (HTTP path) and multiple topics (gRPC)
        IEnumerable<string> topicNames = request.Topics.Count > 0
            ? request.Topics
            : !string.IsNullOrEmpty(request.Topic)
                ? [request.Topic]
                : [];

        foreach (var topicName in topicNames)
        {
            var metadata = _logManager.GetTopicMetadata(topicName);

            if (metadata == null)
            {
                response.Topics.Add(new TopicDescription
                {
                    Status = new ResponseStatus
                    {
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        ErrorMessage = $"Topic '{topicName}' not found"
                    }
                });
            }
            else
            {
                response.Topics.Add(new TopicDescription
                {
                    TopicInfo = MapTopicInfo(metadata),
                    Status = new ResponseStatus { ErrorCode = ErrorCode.None }
                });
            }
        }

        return Task.FromResult(response);
    }

    public override Task<AlterConfigResponse> AlterConfig(AlterConfigRequest request, ServerCallContext context)
    {
        var response = new AlterConfigResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        };

        var resource = request.Resource;
        if (resource == null)
        {
            response.Status = new ResponseStatus
            {
                ErrorCode = ErrorCode.InvalidConfig,
                ErrorMessage = "Resource is required"
            };
            return Task.FromResult(response);
        }

        if (resource.Type == ConfigResourceType.Topic)
        {
            var configUpdates = request.Configs
                .Where(c => !string.IsNullOrEmpty(c.Value))
                .ToDictionary(c => c.Key, c => c.Value);

            var deleteKeys = request.Configs
                .Where(c => string.IsNullOrEmpty(c.Value))
                .Select(c => c.Key)
                .ToList();

            var success = _logManager.UpdateTopicConfig(
                resource.Name,
                configUpdates,
                deleteKeys.Count > 0 ? deleteKeys : null);

            if (!success)
            {
                response.Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.UnknownTopicOrPartition,
                    ErrorMessage = $"Topic '{resource.Name}' not found"
                };
            }
        }
        else if (resource.Type == ConfigResourceType.Broker)
        {
            // Broker config changes are handled by AdminServiceImpl
            response.Status = new ResponseStatus
            {
                ErrorCode = ErrorCode.InvalidConfig,
                ErrorMessage = "Use AdminService.AlterBrokerConfig for broker configuration"
            };
        }

        return Task.FromResult(response);
    }

    public override Task<DescribeConfigResponse> DescribeConfig(DescribeConfigRequest request, ServerCallContext context)
    {
        var response = new DescribeConfigResponse();

        foreach (var resource in request.Resources)
        {
            var result = new ConfigResourceResult
            {
                Resource = resource,
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };

            if (resource.Type == ConfigResourceType.Topic)
            {
                var metadata = _logManager.GetTopicMetadata(resource.Name);
                if (metadata != null)
                {
                    foreach (var (key, value) in metadata.Config)
                    {
                        if (request.ConfigKeys.Count == 0 || request.ConfigKeys.Contains(key))
                        {
                            result.Configs.Add(new ConfigEntryDetail
                            {
                                Key = key,
                                Value = value,
                                IsDefault = false,
                                IsReadOnly = false,
                                IsSensitive = false
                            });
                        }
                    }
                }
                else
                {
                    result.Status = new ResponseStatus
                    {
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        ErrorMessage = $"Topic '{resource.Name}' not found"
                    };
                }
            }

            response.Results.Add(result);
        }

        return Task.FromResult(response);
    }

    private TopicInfo MapTopicInfo(TopicMetadata metadata)
    {
        var info = new TopicInfo
        {
            Name = metadata.Name,
            NumPartitions = metadata.PartitionCount,
            ReplicationFactor = metadata.ReplicationFactor,
            Config = { metadata.Config }
        };

        for (int i = 0; i < metadata.PartitionCount; i++)
        {
            var log = _logManager.GetLog(new CoreTopicPartition
            {
                Topic = metadata.Name,
                Partition = i
            });

            info.Partitions.Add(new PartitionInfo
            {
                PartitionId = i,
                Leader = 0, // Current broker is always leader for now
                Replicas = { 0 },
                Isr = { 0 },
                HighWatermark = log?.HighWatermark ?? 0,
                LogStartOffset = log?.LogStartOffset ?? 0
            });
        }

        return info;
    }
}
