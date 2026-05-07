using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;

namespace Kuestenlogik.Surgewave.Gateway.Services;

/// <summary>
/// gRPC service implementation for producer operations.
/// Uses Surgewave native client to communicate with the broker.
/// </summary>
public sealed class ProducerGatewayService : ProducerService.ProducerServiceBase
{
    private readonly ClusterRegistry _registry;
    private readonly ILogger<ProducerGatewayService> _logger;

    public ProducerGatewayService(
        ClusterRegistry registry,
        ILogger<ProducerGatewayService> logger)
    {
        _registry = registry;
        _logger = logger;
    }

    public override async Task<ProduceResponse> Produce(ProduceRequest request, ServerCallContext context)
    {
        try
        {
            var client = _registry.GetClient(request.ClusterId);
            var key = request.Record?.Key.ToByteArray();
            var value = request.Record?.Value.ToByteArray() ?? [];

            var offset = await client.Messaging.SendAsync(
                request.Topic,
                request.Partition,
                key,
                value,
                context.CancellationToken);

            return new ProduceResponse
            {
                Topic = request.Topic,
                Partition = request.Partition,
                Offset = offset,
                Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Status = new ResponseStatus { ErrorCode = ErrorCode.None }
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to produce message to {Topic}", request.Topic);
            return new ProduceResponse
            {
                Topic = request.Topic,
                Partition = request.Partition,
                Status = new ResponseStatus
                {
                    ErrorCode = ErrorCode.Unknown,
                    ErrorMessage = ex.Message
                }
            };
        }
    }

    public override async Task<ProduceBatchResponse> ProduceBatch(
        IAsyncStreamReader<ProduceRequest> requestStream,
        ServerCallContext context)
    {
        var responses = new List<ProduceResponse>();
        var successCount = 0;
        var failCount = 0;

        await foreach (var request in requestStream.ReadAllAsync(context.CancellationToken))
        {
            var response = await Produce(request, context);
            responses.Add(response);

            if (response.Status.ErrorCode == ErrorCode.None)
                successCount++;
            else
                failCount++;
        }

        return new ProduceBatchResponse
        {
            TotalRecords = responses.Count,
            SuccessfulRecords = successCount,
            FailedRecords = failCount,
            Responses = { responses }
        };
    }

    public override async Task ProduceStream(
        IAsyncStreamReader<ProduceRequest> requestStream,
        IServerStreamWriter<ProduceResponse> responseStream,
        ServerCallContext context)
    {
        await foreach (var request in requestStream.ReadAllAsync(context.CancellationToken))
        {
            var response = await Produce(request, context);
            await responseStream.WriteAsync(response, context.CancellationToken);
        }
    }
}
