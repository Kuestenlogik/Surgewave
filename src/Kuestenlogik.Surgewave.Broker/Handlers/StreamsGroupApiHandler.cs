using Kuestenlogik.Surgewave.Broker.StreamsGroups;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Handlers;

/// <summary>
/// Handler for streams group APIs (KIP-1071): StreamsGroupHeartbeat, StreamsGroupDescribe.
/// Topology-aware task assignment for Kafka Streams applications.
/// </summary>
public sealed class StreamsGroupApiHandler : IKafkaRequestHandler
{
    private readonly StreamsGroupCoordinator _coordinator;
    private readonly ILogger<StreamsGroupApiHandler> _logger;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.StreamsGroupHeartbeat,
        ApiKey.StreamsGroupDescribe
    ];

    public StreamsGroupApiHandler(
        StreamsGroupCoordinator coordinator,
        ILogger<StreamsGroupApiHandler> logger)
    {
        _coordinator = coordinator;
        _logger = logger;
    }

    public Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        KafkaResponse response = request switch
        {
            StreamsGroupHeartbeatRequest r => _coordinator.HandleStreamsGroupHeartbeat(r),
            StreamsGroupDescribeRequest r => _coordinator.HandleStreamsGroupDescribe(r),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by StreamsGroupApiHandler")
        };

        return Task.FromResult(response);
    }
}
