using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;
using PartitionInfo = Kuestenlogik.Surgewave.Api.Grpc.PartitionInfo;
using TopicInfo = Kuestenlogik.Surgewave.Api.Grpc.TopicInfo;

namespace Kuestenlogik.Surgewave.Gateway.Services;

/// <summary>
/// gRPC service implementation for topic operations.
/// Uses Surgewave native client to communicate with the broker.
/// </summary>
public sealed class TopicGatewayService : TopicService.TopicServiceBase
{
    private readonly ClusterRegistry _registry;
    private readonly ILogger<TopicGatewayService> _logger;

    public TopicGatewayService(ClusterRegistry registry, ILogger<TopicGatewayService> logger)
    {
        _registry = registry;
        _logger = logger;
    }

    public override async Task<CreateTopicResponse> CreateTopic(CreateTopicRequest request, ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            await client.Topics.CreateAsync(
                request.Topic,
                request.NumPartitions,
                (short)request.ReplicationFactor,
                context.CancellationToken);

            return new CreateTopicResponse
            {
                TopicInfo = new TopicInfo
                {
                    Name = request.Topic,
                    NumPartitions = request.NumPartitions,
                    ReplicationFactor = request.ReplicationFactor
                },
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to create topic {Topic}", request.Topic);
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
            var client = _registry.GetClient(request.ClusterId);
            await client.Topics.DeleteAsync(request.Topic, context.CancellationToken);

            return new DeleteTopicResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to delete topic {Topic}", request.Topic);
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

    public override async Task<ListTopicsResponse> ListTopics(ListTopicsRequest request, ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            var topics = await client.Topics.ListAsync(context.CancellationToken);
            var response = new ListTopicsResponse();

            foreach (var topic in topics)
            {
                if (!request.IncludeInternal && topic.Name.StartsWith("__", StringComparison.Ordinal))
                    continue;

                response.Topics.Add(topic.Name);
            }

            return response;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to list topics");
            return new ListTopicsResponse();
        }
    }

    public override async Task<DescribeTopicResponse> DescribeTopic(DescribeTopicRequest request, ServerCallContext context)
    {
        var client = _registry.GetClient(request.ClusterId);
        var response = new DescribeTopicResponse();

        foreach (var topicName in request.Topics)
        {
            try
            {
                var description = await client.Topics.DescribeAsync(topicName, context.CancellationToken);

                var topicInfo = new TopicInfo
                {
                    Name = description.Name,
                    NumPartitions = description.PartitionCount,
                    IsInternal = description.Name.StartsWith("__", StringComparison.Ordinal)
                };

                foreach (var partition in description.Partitions)
                {
                    topicInfo.Partitions.Add(new PartitionInfo
                    {
                        PartitionId = partition.PartitionId,
                        Leader = partition.Leader,
                        Replicas = { partition.Replicas },
                        Isr = { partition.Isr }
                    });
                }

                response.Topics.Add(new TopicDescription
                {
                    TopicInfo = topicInfo,
                    Status = new ResponseStatus { ErrorCode = ErrorCode.None }
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to describe topic {Topic}", topicName);
                response.Topics.Add(new TopicDescription
                {
                    Status = new ResponseStatus
                    {
                        ErrorCode = ErrorCode.UnknownTopicOrPartition,
                        ErrorMessage = ex.Message
                    }
                });
            }
        }

        return response;
    }

    public override async Task<AlterConfigResponse> AlterConfig(AlterConfigRequest request, ServerCallContext context)
    {
        try
        {
            if (request.Resource.Type != ConfigResourceType.Topic)
            {
                return new AlterConfigResponse
                {
                    Status = new ResponseStatus
                    {
                        ErrorCode = ErrorCode.InvalidConfig,
                        ErrorMessage = "Only topic configs are supported"
                    }
                };
            }

            var client = _registry.GetClient(request.ClusterId);
            var config = request.Configs.ToDictionary(c => c.Key, c => c.Value);
            await client.Topics.AlterConfigAsync(request.Resource.Name, config, context.CancellationToken);

            return new AlterConfigResponse
            {
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to alter config for {Resource}", request.Resource.Name);
            return new AlterConfigResponse
            {
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<DescribeConfigResponse> DescribeConfig(DescribeConfigRequest request, ServerCallContext context)
    {
        var client = _registry.GetClient(request.ClusterId);
        var response = new DescribeConfigResponse();

        foreach (var resource in request.Resources)
        {
            try
            {
                if (resource.Type != ConfigResourceType.Topic)
                {
                    response.Results.Add(new ConfigResourceResult
                    {
                        Resource = resource,
                        Status = new ResponseStatus
                        {
                            ErrorCode = ErrorCode.InvalidConfig,
                            ErrorMessage = "Only topic configs are supported"
                        }
                    });
                    continue;
                }

                var config = await client.Topics.DescribeConfigAsync(resource.Name, context.CancellationToken);
                var result = new ConfigResourceResult
                {
                    Resource = resource,
                    Status = new ResponseStatus { ErrorCode = ErrorCode.None }
                };

                foreach (var (key, value) in config)
                {
                    if (request.ConfigKeys.Count > 0 && !request.ConfigKeys.Contains(key))
                        continue;

                    result.Configs.Add(new ConfigEntryDetail
                    {
                        Key = key,
                        Value = value,
                        IsDefault = false,
                        IsReadOnly = false,
                        IsSensitive = false
                    });
                }

                response.Results.Add(result);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to describe config for {Resource}", resource.Name);
                response.Results.Add(new ConfigResourceResult
                {
                    Resource = resource,
                    Status = new ResponseStatus
                    {
                        ErrorCode = ErrorCode.Unknown,
                        ErrorMessage = ex.Message
                    }
                });
            }
        }

        return response;
    }
}
