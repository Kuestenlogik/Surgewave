using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Google.Protobuf;
using CoreTopicPartition = Kuestenlogik.Surgewave.Core.Models.TopicPartition;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// Delegate for parsing Kafka record batch into messages.
/// </summary>
public delegate List<Message> ParseRecordBatchDelegate(byte[] recordBatch);

/// <summary>
/// Delegate for committing consumer group offsets.
/// </summary>
public delegate void CommitOffsetDelegate(string groupId, string topic, int partition, long offset);

/// <summary>
/// Delegate for fetching committed offsets.
/// </summary>
public delegate long? GetCommittedOffsetDelegate(string groupId, string topic, int partition);

/// <summary>
/// gRPC ConsumerService implementation
/// </summary>
public class ConsumerServiceImpl : ConsumerService.ConsumerServiceBase
{
    private readonly LogManager _logManager;
    private readonly ParseRecordBatchDelegate _parseRecordBatch;
    private readonly CommitOffsetDelegate? _commitOffset;
    private readonly GetCommittedOffsetDelegate? _getCommittedOffset;

    public ConsumerServiceImpl(
        LogManager logManager,
        ParseRecordBatchDelegate parseRecordBatch,
        CommitOffsetDelegate? commitOffset = null,
        GetCommittedOffsetDelegate? getCommittedOffset = null)
    {
        _logManager = logManager;
        _parseRecordBatch = parseRecordBatch;
        _commitOffset = commitOffset;
        _getCommittedOffset = getCommittedOffset;
    }

    public override async Task Consume(
        ConsumeRequest request,
        IServerStreamWriter<ConsumeResponse> responseStream,
        ServerCallContext context)
    {
        var topicPartition = new CoreTopicPartition
        {
            Topic = request.Topic,
            Partition = request.Partition
        };

        var log = _logManager.GetLog(topicPartition);
        if (log == null)
        {
            await responseStream.WriteAsync(new ConsumeResponse
            {
                Topic = request.Topic,
                Partition = request.Partition,
                Status = ResponseStatusFactory.TopicNotFound(request.Topic, request.Partition)
            });
            return;
        }

        var currentOffset = request.Offset switch
        {
            -1 => log.HighWatermark,
            -2 => log.LogStartOffset,
            _ => request.Offset
        };

        var maxRecords = request.MaxRecords > 0 ? request.MaxRecords : 100;
        var maxWaitMs = request.MaxWaitMs > 0 ? request.MaxWaitMs : 5000;
        var maxBytes = 1024 * 1024;

        try
        {
            while (!context.CancellationToken.IsCancellationRequested)
            {
                var batches = await log.ReadBatchesAsync(currentOffset, maxBytes, context.CancellationToken);

                if (batches.Count == 0)
                {
                    await Task.Delay(Math.Min(100, maxWaitMs), context.CancellationToken);
                    continue;
                }

                var response = new ConsumeResponse
                {
                    Topic = request.Topic,
                    Partition = request.Partition,
                    HighWatermark = log.HighWatermark,
                    Status = ResponseStatusFactory.Success
                };

                var recordsSent = 0;
                foreach (var batch in batches)
                {
                    var messages = _parseRecordBatch(batch);

                    foreach (var message in messages)
                    {
                        if (recordsSent >= maxRecords) break;

                        var record = new Record
                        {
                            Key = ByteString.CopyFrom(message.Key.Span),
                            Value = ByteString.CopyFrom(message.Value.Span),
                            Timestamp = message.Timestamp,
                            Offset = message.Offset
                        };

                        var headers = HeaderSerializer.Deserialize(message.Headers.Span);
                        foreach (var (key, value) in headers)
                        {
                            record.Headers[key] = value;
                        }

                        response.Records.Add(record);
                        currentOffset = message.Offset + 1;
                        recordsSent++;
                    }

                    if (recordsSent >= maxRecords) break;
                }

                if (response.Records.Count > 0)
                {
                    await responseStream.WriteAsync(response, context.CancellationToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Client disconnected
        }
    }

    public override async Task<FetchResponse> Fetch(FetchRequest request, ServerCallContext context)
    {
        var response = new FetchResponse();

        foreach (var partition in request.Partitions)
        {
            var topicPartition = new CoreTopicPartition
            {
                Topic = partition.Topic,
                Partition = partition.Partition
            };

            var log = _logManager.GetLog(topicPartition);
            var result = new PartitionFetchResult
            {
                Topic = partition.Topic,
                Partition = partition.Partition
            };

            if (log == null)
            {
                result.Status = ResponseStatusFactory.TopicNotFound(partition.Topic, partition.Partition);
                response.Results.Add(result);
                continue;
            }

            var maxBytes = partition.MaxBytes > 0 ? partition.MaxBytes : request.MaxBytes;
            var batches = await log.ReadBatchesAsync(partition.Offset, maxBytes, context.CancellationToken);

            result.HighWatermark = log.HighWatermark;
            result.Status = ResponseStatusFactory.Success;

            foreach (var batch in batches)
            {
                var messages = _parseRecordBatch(batch);
                foreach (var message in messages)
                {
                    var record = new Record
                    {
                        Key = ByteString.CopyFrom(message.Key.Span),
                        Value = ByteString.CopyFrom(message.Value.Span),
                        Timestamp = message.Timestamp,
                        Offset = message.Offset
                    };

                    var headers = HeaderSerializer.Deserialize(message.Headers.Span);
                    foreach (var (key, value) in headers)
                    {
                        record.Headers[key] = value;
                    }

                    result.Records.Add(record);
                }
            }

            response.Results.Add(result);
        }

        return response;
    }

    public override Task<CommitResponse> Commit(CommitRequest request, ServerCallContext context)
    {
        var response = new CommitResponse();

        if (_commitOffset == null)
        {
            foreach (var offset in request.Offsets)
            {
                response.Results.Add(new OffsetCommitResult
                {
                    Topic = offset.Topic,
                    Partition = offset.Partition,
                    Status = ResponseStatusFactory.CoordinatorNotAvailable("Offset storage not configured")
                });
            }
            return Task.FromResult(response);
        }

        foreach (var offset in request.Offsets)
        {
            try
            {
                _commitOffset(request.ConsumerGroup, offset.Topic, offset.Partition, offset.Offset);
                response.Results.Add(new OffsetCommitResult
                {
                    Topic = offset.Topic,
                    Partition = offset.Partition,
                    Status = ResponseStatusFactory.Success
                });
            }
            catch (Exception ex)
            {
                response.Results.Add(new OffsetCommitResult
                {
                    Topic = offset.Topic,
                    Partition = offset.Partition,
                    Status = ResponseStatusFactory.FromException(ex)
                });
            }
        }

        return Task.FromResult(response);
    }

    public override Task<FetchOffsetsResponse> FetchOffsets(FetchOffsetsRequest request, ServerCallContext context)
    {
        var response = new FetchOffsetsResponse();

        foreach (var partition in request.Partitions)
        {
            var offset = _getCommittedOffset?.Invoke(request.ConsumerGroup, partition.Topic, partition.Partition) ?? -1;
            response.Results.Add(new OffsetFetchResult
            {
                Topic = partition.Topic,
                Partition = partition.Partition,
                Offset = offset,
                Status = ResponseStatusFactory.Success
            });
        }

        return Task.FromResult(response);
    }

    public override Task<ListOffsetsResponse> ListOffsets(ListOffsetsRequest request, ServerCallContext context)
    {
        var response = new ListOffsetsResponse();

        foreach (var partition in request.Partitions)
        {
            var topicPartition = new CoreTopicPartition
            {
                Topic = partition.Topic,
                Partition = partition.Partition
            };

            var log = _logManager.GetLog(topicPartition);
            var result = new OffsetResult
            {
                Topic = partition.Topic,
                Partition = partition.Partition
            };

            if (log == null)
            {
                result.Status = ResponseStatusFactory.Error(ErrorCode.UnknownTopicOrPartition, "");
            }
            else
            {
                result.Offset = partition.Timestamp switch
                {
                    -1 => log.HighWatermark,
                    -2 => log.LogStartOffset,
                    _ => log.LogStartOffset
                };
                result.Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                result.Status = ResponseStatusFactory.Success;
            }

            response.Results.Add(result);
        }

        return Task.FromResult(response);
    }

    public override Task<SubscribeResponse> Subscribe(SubscribeRequest request, ServerCallContext context)
    {
        var response = new SubscribeResponse
        {
            Status = ResponseStatusFactory.Success
        };

        foreach (var topic in request.Topics)
        {
            var metadata = _logManager.GetTopicMetadata(topic);
            if (metadata != null)
            {
                for (int i = 0; i < metadata.PartitionCount; i++)
                {
                    response.AssignedPartitions.Add(new TopicPartition
                    {
                        Topic = topic,
                        Partition = i
                    });
                }
            }
        }

        return Task.FromResult(response);
    }

    public override Task<UnsubscribeResponse> Unsubscribe(UnsubscribeRequest request, ServerCallContext context)
    {
        return Task.FromResult(new UnsubscribeResponse
        {
            Status = ResponseStatusFactory.Success
        });
    }

    /// <summary>
    /// Bidirectional streaming consume with flow control.
    /// Client sends control messages (START, PAUSE, RESUME, ACK, SEEK) to manage consumption.
    /// </summary>
    public override async Task ConsumeStream(
        IAsyncStreamReader<ConsumeStreamControl> requestStream,
        IServerStreamWriter<ConsumeResponse> responseStream,
        ServerCallContext context)
    {
        string? currentTopic = null;
        int currentPartition = 0;
        long currentOffset = 0;
        int maxRecords = 100;
        int maxWaitMs = 5000;
        bool isPaused = false;
        bool isStarted = false;

        // Background task for reading control messages
        var controlTask = Task.Run(async () =>
        {
            await foreach (var control in requestStream.ReadAllAsync(context.CancellationToken))
            {
                switch (control.Type)
                {
                    case ConsumeStreamControl.Types.ControlType.Start:
                        currentTopic = control.Topic;
                        currentPartition = control.Partition;
                        currentOffset = control.Offset;
                        maxRecords = control.MaxRecords > 0 ? control.MaxRecords : 100;
                        maxWaitMs = control.MaxWaitMs > 0 ? control.MaxWaitMs : 5000;
                        isStarted = true;
                        isPaused = false;
                        break;

                    case ConsumeStreamControl.Types.ControlType.Pause:
                        isPaused = true;
                        break;

                    case ConsumeStreamControl.Types.ControlType.Resume:
                        isPaused = false;
                        break;

                    case ConsumeStreamControl.Types.ControlType.Ack:
                        // Advance offset past the acknowledged position
                        currentOffset = control.Offset + 1;
                        break;

                    case ConsumeStreamControl.Types.ControlType.Seek:
                        currentOffset = control.Offset;
                        break;
                }
            }
        });

        try
        {
            while (!context.CancellationToken.IsCancellationRequested)
            {
                if (!isStarted || isPaused || currentTopic == null)
                {
                    await Task.Delay(50, context.CancellationToken);
                    continue;
                }

                var topicPartition = new CoreTopicPartition
                {
                    Topic = currentTopic,
                    Partition = currentPartition
                };

                var log = _logManager.GetLog(topicPartition);
                if (log == null)
                {
                    await responseStream.WriteAsync(new ConsumeResponse
                    {
                        Topic = currentTopic,
                        Partition = currentPartition,
                        Status = ResponseStatusFactory.TopicNotFound(currentTopic, currentPartition)
                    }, context.CancellationToken);
                    await Task.Delay(1000, context.CancellationToken);
                    continue;
                }

                var batches = await log.ReadBatchesAsync(currentOffset, 1024 * 1024, context.CancellationToken);

                if (batches.Count == 0)
                {
                    await Task.Delay(Math.Min(100, maxWaitMs), context.CancellationToken);
                    continue;
                }

                var response = new ConsumeResponse
                {
                    Topic = currentTopic,
                    Partition = currentPartition,
                    HighWatermark = log.HighWatermark,
                    Status = ResponseStatusFactory.Success
                };

                var recordsSent = 0;
                foreach (var batch in batches)
                {
                    if (recordsSent >= maxRecords) break;

                    var messages = _parseRecordBatch(batch);
                    foreach (var message in messages)
                    {
                        if (recordsSent >= maxRecords) break;

                        var record = new Record
                        {
                            Key = ByteString.CopyFrom(message.Key.Span),
                            Value = ByteString.CopyFrom(message.Value.Span),
                            Timestamp = message.Timestamp,
                            Offset = message.Offset
                        };

                        var headers = HeaderSerializer.Deserialize(message.Headers.Span);
                        foreach (var (key, value) in headers)
                        {
                            record.Headers[key] = value;
                        }

                        response.Records.Add(record);
                        currentOffset = message.Offset + 1;
                        recordsSent++;
                    }
                }

                if (response.Records.Count > 0)
                {
                    await responseStream.WriteAsync(response, context.CancellationToken);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Client disconnected or cancelled
        }
        finally
        {
            // Wait for control task to complete
            try
            {
                await controlTask;
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }
    }

}
