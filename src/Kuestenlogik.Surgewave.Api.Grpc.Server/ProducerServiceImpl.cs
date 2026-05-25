using Grpc.Core;
using Kuestenlogik.Surgewave.Api.Grpc;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Google.Protobuf;
using CoreTopicPartition = Kuestenlogik.Surgewave.Core.Models.TopicPartition;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// Delegate for serializing messages to Kafka record batch format.
/// </summary>
public delegate byte[] SerializeMessagesDelegate(List<Message> messages);

/// <summary>
/// gRPC ProducerService implementation
/// </summary>
public class ProducerServiceImpl : ProducerService.ProducerServiceBase
{
    private readonly LogManager _logManager;
    private readonly SerializeMessagesDelegate _serializeMessages;

    public ProducerServiceImpl(LogManager logManager, SerializeMessagesDelegate serializeMessages)
    {
        _logManager = logManager;
        _serializeMessages = serializeMessages;
    }

    public override async Task<ProduceResponse> Produce(ProduceRequest request, ServerCallContext context)
    {
        try
        {
            var topicPartition = new CoreTopicPartition
            {
                Topic = request.Topic,
                Partition = request.Partition
            };

            var log = _logManager.GetOrCreateLog(topicPartition);

            if (log == null)
            {
                return new ProduceResponse
                {
                    Topic = request.Topic,
                    Partition = request.Partition,
                    Offset = -1,
                    Status = ResponseStatusFactory.TopicNotFound(request.Topic, request.Partition)
                };
            }

            var record = request.Record;
            var message = new Message
            {
                Offset = 0,
                Timestamp = record?.Timestamp > 0 ? record.Timestamp : DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Key = record?.Key?.ToByteArray() ?? [],
                Value = record?.Value?.ToByteArray() ?? [],
                Headers = record?.Headers != null ? HeaderSerializer.Serialize(record.Headers) : []
            };

            var recordBatch = _serializeMessages([message]);
            var baseOffset = await log.AppendBatchAsync(recordBatch, context.CancellationToken);

            return new ProduceResponse
            {
                Topic = request.Topic,
                Partition = request.Partition,
                Offset = baseOffset,
                Timestamp = message.Timestamp,
                Status = ResponseStatusFactory.Success
            };
        }
        catch (Exception ex)
        {
            return new ProduceResponse
            {
                Topic = request.Topic,
                Partition = request.Partition,
                Offset = -1,
                Status = ResponseStatusFactory.FromException(ex)
            };
        }
    }

    public override async Task<ProduceBatchResponse> ProduceBatch(
        IAsyncStreamReader<ProduceRequest> requestStream,
        ServerCallContext context)
    {
        var responses = new List<ProduceResponse>();
        int total = 0;
        int successful = 0;
        int failed = 0;

        await foreach (var request in requestStream.ReadAllAsync(context.CancellationToken))
        {
            total++;
            var response = await Produce(request, context);
            responses.Add(response);

            if (response.Status?.ErrorCode == ErrorCode.None)
                successful++;
            else
                failed++;
        }

        return new ProduceBatchResponse
        {
            TotalRecords = total,
            SuccessfulRecords = successful,
            FailedRecords = failed,
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
