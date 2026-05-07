using Kuestenlogik.Surgewave.Broker.ShareGroups;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Handlers;

/// <summary>
/// Handler for share group APIs (KIP-932): ShareGroupHeartbeat, ShareGroupDescribe,
/// ShareFetch, ShareAcknowledge, DescribeShareGroupOffsets, AlterShareGroupOffsets, DeleteShareGroupOffsets
/// </summary>
public sealed class ShareGroupApiHandler : IKafkaRequestHandler
{
    private readonly ShareGroupCoordinator _coordinator;
    private readonly ILogger<ShareGroupApiHandler> _logger;

    public IEnumerable<ApiKey> SupportedApiKeys =>
    [
        ApiKey.ShareGroupHeartbeat,
        ApiKey.ShareGroupDescribe,
        ApiKey.ShareFetch,
        ApiKey.ShareAcknowledge,
        ApiKey.DescribeShareGroupOffsets,
        ApiKey.AlterShareGroupOffsets,
        ApiKey.DeleteShareGroupOffsets
    ];

    public ShareGroupApiHandler(
        ShareGroupCoordinator coordinator,
        ILogger<ShareGroupApiHandler> logger)
    {
        _coordinator = coordinator;
        _logger = logger;
    }

    public async Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return request switch
        {
            ShareGroupHeartbeatRequest r => _coordinator.HandleShareGroupHeartbeat(r),
            ShareGroupDescribeRequest r => _coordinator.HandleShareGroupDescribe(r),
            ShareFetchRequest r => await _coordinator.HandleShareFetch(r, cancellationToken),
            ShareAcknowledgeRequest r => _coordinator.HandleShareAcknowledge(r),
            DescribeShareGroupOffsetsRequest r => _coordinator.HandleDescribeShareGroupOffsets(r),
            AlterShareGroupOffsetsRequest r => _coordinator.HandleAlterShareGroupOffsets(r),
            DeleteShareGroupOffsetsRequest r => _coordinator.HandleDeleteShareGroupOffsets(r),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by ShareGroupApiHandler")
        };
    }
}
