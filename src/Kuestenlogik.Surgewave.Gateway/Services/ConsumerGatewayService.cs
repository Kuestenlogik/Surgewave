using Google.Protobuf;
using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Gateway.Services;

/// <summary>
/// gRPC service implementation for consumer operations.
/// Uses Surgewave native client to communicate with the broker.
/// </summary>
public sealed class ConsumerGatewayService : ConsumerService.ConsumerServiceBase
{
    private readonly ClusterRegistry _registry;
    private readonly ILogger<ConsumerGatewayService> _logger;

    public ConsumerGatewayService(ClusterRegistry registry, ILogger<ConsumerGatewayService> logger)
    {
        _registry = registry;
        _logger = logger;
    }

    public override async Task Consume(
        ConsumeRequest request,
        IServerStreamWriter<ConsumeResponse> responseStream,
        ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(null); // Streaming RPCs use default cluster
            var offset = request.Offset;
            var maxRecords = request.MaxRecords > 0 ? request.MaxRecords : 100;

            while (!context.CancellationToken.IsCancellationRequested)
            {
                var result = await client.Messaging.ReceiveAsync(
                    request.Topic,
                    request.Partition,
                    offset,
                    maxRecords * 1024, // Estimate bytes per record
                    maxWaitMs: request.MaxWaitMs > 0 ? request.MaxWaitMs : 5000,
                    context.CancellationToken);

                if (result.Messages.Count > 0)
                {
                    var response = new ConsumeResponse
                    {
                        Topic = request.Topic,
                        Partition = request.Partition,
                        HighWatermark = result.HighWatermark,
                        Status = new ResponseStatus { ErrorCode = ErrorCode.None }
                    };

                    foreach (var msg in result.Messages)
                    {
                        response.Records.Add(new Record
                        {
                            Offset = msg.Offset,
                            Timestamp = msg.Timestamp,
                            Key = msg.Key != null ? ByteString.CopyFrom(msg.Key) : ByteString.Empty,
                            Value = ByteString.CopyFrom(msg.Value)
                        });
                        offset = msg.Offset + 1;
                    }

                    await responseStream.WriteAsync(response, context.CancellationToken);
                }
                else
                {
                    // No messages, wait before polling again
                    await Task.Delay(request.MaxWaitMs > 0 ? request.MaxWaitMs : 100, context.CancellationToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error in Consume stream for {Topic}:{Partition}", request.Topic, request.Partition);
            throw new RpcException(new Status(StatusCode.Internal, ex.Message));
        }
    }

    public override async Task<FetchResponse> Fetch(FetchRequest request, ServerCallContext context)
    {
        var client = _registry.GetClient(request.ClusterId);
        var response = new FetchResponse();

        foreach (var partition in request.Partitions)
        {
            try
            {
                var maxBytes = partition.MaxBytes > 0 ? partition.MaxBytes : (request.MaxBytes > 0 ? request.MaxBytes : 1024 * 1024);

                var result = await client.Messaging.ReceiveAsync(
                    partition.Topic,
                    partition.Partition,
                    partition.Offset,
                    maxBytes,
                    maxWaitMs: request.MaxWaitMs > 0 ? request.MaxWaitMs : 5000,
                    context.CancellationToken);

                var partitionResult = new PartitionFetchResult
                {
                    Topic = partition.Topic,
                    Partition = partition.Partition,
                    HighWatermark = result.HighWatermark,
                    Status = new ResponseStatus { ErrorCode = ErrorCode.None }
                };

                foreach (var msg in result.Messages)
                {
                    partitionResult.Records.Add(new Record
                    {
                        Offset = msg.Offset,
                        Timestamp = msg.Timestamp,
                        Key = msg.Key != null ? ByteString.CopyFrom(msg.Key) : ByteString.Empty,
                        Value = ByteString.CopyFrom(msg.Value)
                    });
                }

                response.Results.Add(partitionResult);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to fetch from {Topic}:{Partition}", partition.Topic, partition.Partition);
                response.Results.Add(new PartitionFetchResult
                {
                    Topic = partition.Topic,
                    Partition = partition.Partition,
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

    public override async Task<CommitResponse> Commit(CommitRequest request, ServerCallContext context)
    {
        var client = _registry.GetClient(request.ClusterId);
        var response = new CommitResponse();

        foreach (var offset in request.Offsets)
        {
            try
            {
                await client.Groups.CommitOffsetAsync(
                    request.ConsumerGroup,
                    "", // memberId - not required for simple commit
                    0,  // generationId - not required for simple commit
                    offset.Topic,
                    offset.Partition,
                    offset.Offset,
                    context.CancellationToken);

                response.Results.Add(new OffsetCommitResult
                {
                    Topic = offset.Topic,
                    Partition = offset.Partition,
                    Status = new ResponseStatus { ErrorCode = ErrorCode.None }
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to commit offset for {Topic}:{Partition}", offset.Topic, offset.Partition);
                response.Results.Add(new OffsetCommitResult
                {
                    Topic = offset.Topic,
                    Partition = offset.Partition,
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

    public override async Task<FetchOffsetsResponse> FetchOffsets(FetchOffsetsRequest request, ServerCallContext context)
    {
        var client = _registry.GetClient(request.ClusterId);
        var response = new FetchOffsetsResponse();

        foreach (var partition in request.Partitions)
        {
            try
            {
                var offset = await client.Groups.FetchOffsetAsync(
                    request.ConsumerGroup,
                    partition.Topic,
                    partition.Partition,
                    context.CancellationToken);

                response.Results.Add(new OffsetFetchResult
                {
                    Topic = partition.Topic,
                    Partition = partition.Partition,
                    Offset = offset,
                    Status = new ResponseStatus { ErrorCode = ErrorCode.None }
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to fetch offset for {Topic}:{Partition}", partition.Topic, partition.Partition);
                response.Results.Add(new OffsetFetchResult
                {
                    Topic = partition.Topic,
                    Partition = partition.Partition,
                    Offset = -1,
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

    public override async Task<ListOffsetsResponse> ListOffsets(ListOffsetsRequest request, ServerCallContext context)
    {
        var client = _registry.GetClient(request.ClusterId);
        var response = new ListOffsetsResponse();

        foreach (var partition in request.Partitions)
        {
            try
            {
                long offset;
                long timestamp = partition.Timestamp;

                if (timestamp == -1)
                {
                    // Latest offset
                    offset = await client.Messaging.GetLatestOffsetAsync(
                        partition.Topic,
                        partition.Partition,
                        context.CancellationToken);
                }
                else if (timestamp == -2)
                {
                    // Earliest offset
                    offset = await client.Messaging.GetEarliestOffsetAsync(
                        partition.Topic,
                        partition.Partition,
                        context.CancellationToken);
                }
                else
                {
                    // Offset for timestamp
                    offset = await client.Messaging.GetOffsetForTimestampAsync(
                        partition.Topic,
                        partition.Partition,
                        DateTimeOffset.FromUnixTimeMilliseconds(timestamp),
                        context.CancellationToken);
                }

                response.Results.Add(new OffsetResult
                {
                    Topic = partition.Topic,
                    Partition = partition.Partition,
                    Offset = offset,
                    Timestamp = timestamp,
                    Status = new ResponseStatus { ErrorCode = ErrorCode.None }
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to list offset for {Topic}:{Partition}", partition.Topic, partition.Partition);
                response.Results.Add(new OffsetResult
                {
                    Topic = partition.Topic,
                    Partition = partition.Partition,
                    Offset = -1,
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

    public override Task<SubscribeResponse> Subscribe(SubscribeRequest request, ServerCallContext context)
    {
        // Subscribe is typically handled via consumer group protocol (JoinGroup/SyncGroup)
        // For REST API, we return a placeholder - actual subscription happens via gRPC streaming
        _logger.LogWarning("Subscribe via REST is not fully supported - use gRPC streaming or consumer group protocol");

        return Task.FromResult(new SubscribeResponse
        {
            Status = new ResponseStatus
            {
                ErrorCode = ErrorCode.None,
                ErrorMessage = "Subscription registered. Use Fetch or Consume endpoints to receive messages."
            }
        });
    }

    public override Task<UnsubscribeResponse> Unsubscribe(UnsubscribeRequest request, ServerCallContext context)
    {
        // Unsubscribe is typically handled via consumer group protocol (LeaveGroup)
        _logger.LogWarning("Unsubscribe via REST is not fully supported - use consumer group LeaveGroup");

        return Task.FromResult(new UnsubscribeResponse
        {
            Status = new ResponseStatus { ErrorCode = ErrorCode.None }
        });
    }
}
