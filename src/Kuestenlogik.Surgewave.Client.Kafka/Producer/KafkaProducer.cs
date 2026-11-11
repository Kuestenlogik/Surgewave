using Kuestenlogik.Surgewave.Client.Kafka;
using Kuestenlogik.Surgewave.Client.Security;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Client.Producer;

/// <summary>
/// High-level producer client compatible with Kafka producers
/// </summary>
public sealed class KafkaProducer : IAsyncDisposable
{
    // Produce v3: the first version that carries record batches (magic v2), which is the only
    // record format the broker accepts, and still non-flexible.
    private const short ProduceVersion = 3;

    private static readonly RecordBatchSerializer s_batches = new(NullLogger<RecordBatchSerializer>.Instance);

    private readonly ProducerConfig _config;
    private readonly KafkaTransport.OpenedTransport _transport;
    private readonly Stream _stream;
    private readonly string _clientId;
    // After SASL handshake we've already used correlation ids 1 and 2;
    // start the regular request stream past those so a broker that
    // tracks per-connection ids never sees a duplicate.
    private int _correlationId = 100;
    private bool _disposed;

    public KafkaProducer(ProducerConfig config)
    {
        _config = config;
        _clientId = config.ClientId ?? $"kafka-producer-{Guid.NewGuid():N}";

        // Transport handles TCP connect → optional TLS → optional SASL;
        // returns a Stream the produce/fetch path uses identically.
        _transport = KafkaTransport.Open(config.BootstrapServers, _clientId, config.Ssl, config.Sasl);
        _stream = _transport.Stream;
    }

    /// <summary>
    /// Send a record asynchronously
    /// </summary>
    public async Task<RecordMetadata> SendAsync(ProducerRecord record, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var request = new ProduceRequest
        {
            ApiKey = ApiKey.Produce,
            ApiVersion = ProduceVersion,
            CorrelationId = Interlocked.Increment(ref _correlationId),
            ClientId = _clientId,
            RequiredAcks = _config.RequiredAcks,
            TimeoutMs = _config.RequestTimeoutMs,
            TopicData =
            [
                new ProduceRequest.TopicProduceData
                {
                    Name = record.Topic,
                    PartitionData =
                    [
                        new ProduceRequest.PartitionProduceData
                        {
                            Index = record.Partition ?? 0,
                            Records = SerializeRecord(record)
                        }
                    ]
                }
            ]
        };

        var body = await KafkaWire.RoundTripAsync(_stream, request, nameof(ApiKey.Produce), cancellationToken)
            .ConfigureAwait(false);
        ProduceResponse response;
        try
        {
            response = ProduceResponse.ReadFrom(body, ProduceVersion, request.CorrelationId);
        }
        catch (InvalidDataException ex)
        {
            throw new BrokerResponseException("Malformed Produce response from broker", ex);
        }

        var topic = response.Responses.FirstOrDefault()
            ?? throw new BrokerResponseException("Broker returned empty topic response", nameof(ApiKey.Produce));
        var partition = topic.PartitionResponses.FirstOrDefault()
            ?? throw new BrokerResponseException(
                $"Broker returned empty partition response for topic '{topic.Name}'", nameof(ApiKey.Produce));

        if (partition.ErrorCode != ErrorCode.None)
        {
            throw new ProduceException(partition.ErrorCode, topic.Name ?? record.Topic, partition.Index);
        }

        return new RecordMetadata(topic.Name ?? record.Topic, partition.Index, partition.BaseOffset, partition.LogAppendTimeMs);
    }

    /// <summary>
    /// The record as a Kafka record batch (magic v2) of one record, headers included. The
    /// broker assigns the offset; the timestamp is the record's own or now.
    /// </summary>
    private static byte[] SerializeRecord(ProducerRecord record)
    {
        var timestamp = record.Timestamp ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return s_batches.SerializeMessages(
        [
            new Message
            {
                Offset = 0,
                Timestamp = timestamp,
                Key = record.Key ?? ReadOnlyMemory<byte>.Empty,
                Value = record.Value,
                Headers = KafkaWire.EncodeHeaders(record.Headers),
            }
        ]);
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _transport.Dispose();
        _disposed = true;
        return ValueTask.CompletedTask;
    }
}
