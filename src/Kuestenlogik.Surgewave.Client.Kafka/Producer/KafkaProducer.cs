using System.Buffers;
using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Client.Security;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;

namespace Kuestenlogik.Surgewave.Client.Producer;

/// <summary>
/// High-level producer client compatible with Kafka producers
/// </summary>
public sealed class KafkaProducer : IAsyncDisposable
{
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

        // Serialize the record
        var recordBatch = SerializeRecord(record);

        // Create produce request
        var request = new ProduceRequest
        {
            ApiKey = ApiKey.Produce,
            ApiVersion = 3,
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
                            Records = recordBatch
                        }
                    ]
                }
            ]
        };

        // Write request - combine size prefix + payload into single write to reduce syscalls
        using var writer = new KafkaProtocolWriter();
        request.WriteTo(writer);

        var requestSpan = writer.WrittenSpan;
        var totalWriteLength = 4 + requestSpan.Length;
        var combinedBuffer = ArrayPool<byte>.Shared.Rent(totalWriteLength);
        try
        {
            BinaryPrimitives.WriteInt32BigEndian(combinedBuffer, requestSpan.Length);
            requestSpan.CopyTo(combinedBuffer.AsSpan(4));
            await _stream.WriteAsync(combinedBuffer.AsMemory(0, totalWriteLength), cancellationToken);
            await _stream.FlushAsync(cancellationToken);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(combinedBuffer);
        }

        // Read response - use pooled buffer for response data
        var responseSizeBuffer = ArrayPool<byte>.Shared.Rent(4);
        int responseSize;
        try
        {
            await _stream.ReadExactlyAsync(responseSizeBuffer.AsMemory(0, 4), cancellationToken);
            responseSize = BinaryPrimitives.ReadInt32BigEndian(responseSizeBuffer.AsSpan(0, 4));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(responseSizeBuffer);
        }
        var responseBytes = ArrayPool<byte>.Shared.Rent(responseSize);
        try
        {
            await _stream.ReadExactlyAsync(responseBytes.AsMemory(0, responseSize), cancellationToken);

            // Parse response
            using var responseStream = new MemoryStream(responseBytes, 0, responseSize, writable: false);
            using var reader = new BinaryReader(responseStream);

            var correlationId = reader.ReadInt32();
            var topicCount = reader.ReadInt32();

            if (topicCount == 0)
            {
                throw new BrokerResponseException("Broker returned empty topic response", "Produce");
            }

            var topic = ReadString(reader);
            var partitionCount = reader.ReadInt32();

            if (partitionCount == 0)
            {
                throw new BrokerResponseException($"Broker returned empty partition response for topic '{topic}'", "Produce");
            }

            var partition = reader.ReadInt32();
            var errorCode = (ErrorCode)reader.ReadInt16();
            var baseOffset = reader.ReadInt64();
            var logAppendTime = reader.ReadInt64();

            if (errorCode != ErrorCode.None)
            {
                throw new ProduceException(errorCode, topic, partition);
            }

            return new RecordMetadata(topic, partition, baseOffset, logAppendTime);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(responseBytes);
        }
    }

    private byte[] SerializeRecord(ProducerRecord record)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream);

        // Simplified record batch format
        var timestamp = record.Timestamp ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        writer.Write(0L); // Base offset (will be set by broker)
        writer.Write(timestamp);
        writer.Write(record.Key?.Length ?? 0);
        if (record.Key != null)
        {
            writer.Write(record.Key);
        }

        writer.Write(record.Value.Length);
        writer.Write(record.Value);

        // Headers (simplified)
        writer.Write(0); // No headers for now

        return stream.ToArray();
    }

    private string ReadString(BinaryReader reader)
    {
        var length = reader.ReadInt16();
        if (length < 0) return string.Empty;

        var bytes = reader.ReadBytes(length);
        return System.Text.Encoding.UTF8.GetString(bytes);
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _transport.Dispose();
        _disposed = true;
        return ValueTask.CompletedTask;
    }
}
