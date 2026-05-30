using System.Text;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Telemetry;

/// <summary>
/// Mirrors KIP-714 client-telemetry payloads into a Surgewave topic so an
/// observability pipeline (OTLP collector, Vector, Loki, custom OTLP
/// processor) can consume the raw OTLP MetricsData blobs as a Kafka
/// stream. The sink is opt-in via <see cref="ClientTelemetryConfig.TopicSinkEnabled"/>;
/// when off, only the logging/meter ingestor runs.
/// </summary>
/// <remarks>
/// Wire layout per record:
/// <list type="bullet">
///   <item>key   = UTF-8 of client-instance-id (so a single client lands on
///         one partition and downstream tools can fan out per client)</item>
///   <item>value = the raw OTLP MetricsData bytes received from the client,
///         exactly as the client compressed them (the broker does not
///         re-compress or transcode)</item>
///   <item>headers carry <c>client.id</c>, <c>compression</c> ("none", "gzip",
///         "snappy", "lz4", "zstd"), <c>terminating</c> ("0"/"1"), and
///         <c>subscription.id</c> as ASCII so collectors can route without
///         decoding the OTLP body</item>
/// </list>
/// Auto-creation is idempotent — repeated <c>CreateTopicAsync</c> calls
/// after a broker restart are caught and treated as success. Failures here
/// are logged and never propagate; the in-memory ingestor remains
/// authoritative for "is telemetry flowing".
/// </remarks>
public sealed class TelemetryTopicSink
{
    private readonly LogManager _logManager;
    private readonly RecordBatchSerializer _serializer;
    private readonly ClientTelemetryConfig _config;
    private readonly ILogger<TelemetryTopicSink> _logger;
    private readonly SemaphoreSlim _topicCreationGuard = new(1, 1);
    private bool _topicReady;

    public TelemetryTopicSink(
        LogManager logManager,
        RecordBatchSerializer serializer,
        ClientTelemetryConfig config,
        ILogger<TelemetryTopicSink> logger)
    {
        _logManager = logManager;
        _serializer = serializer;
        _config = config;
        _logger = logger;
    }

    public string TopicName => _config.TopicName;

    public async ValueTask<long> WriteAsync(TelemetryPushEvent push, CancellationToken cancellationToken = default)
    {
        try
        {
            await EnsureTopicAsync(cancellationToken).ConfigureAwait(false);

            var key = Encoding.UTF8.GetBytes(push.ClientInstanceId.ToString("N"));
            // Headers are optional in the wire schema, but forwarding them
            // dramatically reduces the work an OTLP collector has to do —
            // it can route by client.id without decoding the proto at all.
            var headers = BuildHeaders(push);

            var message = new Message
            {
                Offset = 0,
                Timestamp = push.ReceivedAt.ToUnixTimeMilliseconds(),
                Key = key,
                Value = push.MetricsPayload,
                Headers = headers,
            };

            var batchBytes = _serializer.SerializeMessages([message]);
            var topicPartition = new TopicPartition { Topic = _config.TopicName, Partition = 0 };
            return await _logManager.AppendBatchAsync(topicPartition, batchBytes, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return -1;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to mirror telemetry push from {ClientInstanceId} to topic {Topic}",
                push.ClientInstanceId, _config.TopicName);
            return -1;
        }
    }

    private async Task EnsureTopicAsync(CancellationToken cancellationToken)
    {
        if (_topicReady) return;
        await _topicCreationGuard.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_topicReady) return;
            try
            {
                await _logManager.CreateTopicAsync(
                    _config.TopicName,
                    partitionCount: 1,
                    replicationFactor: 1,
                    new Dictionary<string, string>
                    {
                        ["retention.ms"] = _config.RetentionMs.ToString(System.Globalization.CultureInfo.InvariantCulture),
                        ["cleanup.policy"] = "delete",
                    },
                    cancellationToken).ConfigureAwait(false);

                _logger.LogInformation(
                    "Telemetry topic created: {Topic} (retentionMs={RetentionMs})",
                    _config.TopicName, _config.RetentionMs);
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase))
            {
                // Topic survived a broker restart — idempotent path.
            }
            _topicReady = true;
        }
        finally
        {
            _topicCreationGuard.Release();
        }
    }

    /// <summary>
    /// Encode a small Kafka v2 record-headers block. Each header is
    /// <c>(key-length-varint, key-bytes, value-length-varint, value-bytes)</c>;
    /// the outer record carries the count as a varint as well. The serializer
    /// expects headers in the native wire block layout (int32 count + int32-
    /// prefixed pairs) — the RecordBatch serializer translates them into the
    /// Kafka v2 record-headers section on its own.
    /// </summary>
    private static ReadOnlyMemory<byte> BuildHeaders(TelemetryPushEvent push)
    {
        var headers = new Dictionary<string, byte[]>(4)
        {
            ["client.id"] = Encoding.UTF8.GetBytes(push.ClientId),
            ["compression"] = Encoding.UTF8.GetBytes(CompressionName(push.CompressionType)),
            ["terminating"] = Encoding.UTF8.GetBytes(push.Terminating ? "1" : "0"),
            ["subscription.id"] = Encoding.UTF8.GetBytes(push.SubscriptionId.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        };
        var buffer = new byte[Kuestenlogik.Surgewave.Protocol.Native.Serialization.NativeMessageHeaderCodec.EncodedSize(headers)];
        Kuestenlogik.Surgewave.Protocol.Native.Serialization.NativeMessageHeaderCodec.Encode(headers, buffer);
        return buffer;
    }

    private static string CompressionName(sbyte code) => code switch
    {
        0 => "none",
        1 => "gzip",
        2 => "snappy",
        3 => "lz4",
        4 => "zstd",
        _ => $"unknown({code})",
    };
}
