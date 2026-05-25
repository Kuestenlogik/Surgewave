using Kuestenlogik.Surgewave.Broker.ConsumerGroupV2;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Handlers;

/// <summary>
/// Handler for consumer group v2 APIs (KIP-848): ConsumerGroupHeartbeat, ConsumerGroupDescribe.
/// Server-side partition assignment — no SyncGroup needed.
/// </summary>
public sealed class ConsumerGroupV2ApiHandler : IKafkaRequestHandler
{
    private readonly ConsumerGroupV2Coordinator _coordinator;
    private readonly ILogger<ConsumerGroupV2ApiHandler> _logger;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.ConsumerGroupHeartbeat,
        ApiKey.ConsumerGroupDescribe
    ];

    public ConsumerGroupV2ApiHandler(
        ConsumerGroupV2Coordinator coordinator,
        ILogger<ConsumerGroupV2ApiHandler> logger)
    {
        _coordinator = coordinator;
        _logger = logger;
    }

    public Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        KafkaResponse response = request switch
        {
            ConsumerGroupHeartbeatRequest r => _coordinator.HandleConsumerGroupHeartbeat(r),
            ConsumerGroupDescribeRequest r => _coordinator.HandleConsumerGroupDescribe(r),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by ConsumerGroupV2ApiHandler")
        };

        return Task.FromResult(response);
    }
}
