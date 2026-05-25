using Grpc.Net.Client;
using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Api.Grpc.Client;

/// <summary>
/// gRPC-based consumer client - language independent
/// </summary>
public sealed class GrpcConsumer : IAsyncDisposable
{
    private readonly GrpcChannel _channel;
    private readonly ConsumerService.ConsumerServiceClient _consumerClient;
    private readonly TopicService.TopicServiceClient _topicClient;

    public GrpcConsumer(string address)
    {
        _channel = GrpcChannel.ForAddress(address);
        _consumerClient = new ConsumerService.ConsumerServiceClient(_channel);
        _topicClient = new TopicService.TopicServiceClient(_channel);
    }

    /// <summary>
    /// Fetch messages from multiple partitions (pull model)
    /// </summary>
    public async Task<FetchResponse> FetchAsync(
        IEnumerable<(string topic, int partition, long offset)> partitions,
        int maxBytes = 1048576,
        int maxWaitMs = 1000,
        CancellationToken cancellationToken = default)
    {
        var request = new FetchRequest
        {
            MaxBytes = maxBytes,
            MaxWaitMs = maxWaitMs
        };

        foreach (var (topic, partition, offset) in partitions)
        {
            request.Partitions.Add(new TopicPartitionOffset
            {
                Topic = topic,
                Partition = partition,
                Offset = offset
            });
        }

        return await _consumerClient.FetchAsync(request, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Consume messages with server-side streaming (push model)
    /// </summary>
    public async IAsyncEnumerable<ConsumeResponse> ConsumeAsync(
        string topic,
        int partition,
        long offset,
        int maxRecords = 100,
        int maxWaitMs = 1000,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var request = new ConsumeRequest
        {
            Topic = topic,
            Partition = partition,
            Offset = offset,
            MaxRecords = maxRecords,
            MaxWaitMs = maxWaitMs
        };

        using var call = _consumerClient.Consume(request, cancellationToken: cancellationToken);

        await foreach (var response in call.ResponseStream.ReadAllAsync(cancellationToken))
        {
            yield return response;
        }
    }

    /// <summary>
    /// Open a bidirectional streaming consumer with flow control.
    /// Returns a stream handle that allows pausing, resuming, seeking, and acknowledging messages.
    /// </summary>
    public ConsumeStreamHandle OpenConsumeStream(CancellationToken cancellationToken = default)
    {
        var call = _consumerClient.ConsumeStream(cancellationToken: cancellationToken);
        return new ConsumeStreamHandle(call);
    }

    /// <summary>
    /// Commit offsets for a consumer group
    /// </summary>
    public async Task<CommitResponse> CommitAsync(
        string consumerGroup,
        IEnumerable<(string topic, int partition, long offset)> offsets,
        CancellationToken cancellationToken = default)
    {
        var request = new CommitRequest
        {
            ConsumerGroup = consumerGroup
        };

        foreach (var (topic, partition, offset) in offsets)
        {
            request.Offsets.Add(new OffsetCommit
            {
                Topic = topic,
                Partition = partition,
                Offset = offset
            });
        }

        return await _consumerClient.CommitAsync(request, cancellationToken: cancellationToken);
    }

    /// <summary>
    /// List all topics
    /// </summary>
    public async Task<List<string>> ListTopicsAsync(CancellationToken cancellationToken = default)
    {
        var response = await _topicClient.ListTopicsAsync(new ListTopicsRequest(), cancellationToken: cancellationToken);
        return response.Topics.ToList();
    }

    /// <summary>
    /// Describe a topic
    /// </summary>
    public async Task<TopicDescription?> DescribeTopicAsync(string topic, CancellationToken cancellationToken = default)
    {
        var request = new DescribeTopicRequest();
        request.Topics.Add(topic);

        var response = await _topicClient.DescribeTopicAsync(request, cancellationToken: cancellationToken);

        var topicResult = response.Topics.FirstOrDefault();
        return topicResult?.Status?.ErrorCode == ErrorCode.None ? topicResult : null;
    }

    public async ValueTask DisposeAsync()
    {
        await _channel.ShutdownAsync();
        _channel.Dispose();
    }
}
