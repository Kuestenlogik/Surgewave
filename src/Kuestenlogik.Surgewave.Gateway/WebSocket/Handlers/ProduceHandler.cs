using System.Text;
using Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;

namespace Kuestenlogik.Surgewave.Gateway.WebSocket.Handlers;

/// <summary>
/// Handles produce WebSocket messages.
/// </summary>
public sealed class ProduceHandler
{
    private readonly ClusterRegistry _clusterRegistry;
    private readonly ILogger<ProduceHandler> _logger;

    public ProduceHandler(
        ClusterRegistry clusterRegistry,
        ILogger<ProduceHandler> logger)
    {
        _clusterRegistry = clusterRegistry;
        _logger = logger;
    }

    /// <summary>
    /// Handles a single produce request.
    /// </summary>
    public async Task HandleProduceAsync(
        WebSocketSession session,
        string? requestId,
        byte[] rawData)
    {
        WebSocketMessage<ProducePayload>? message;
        try
        {
            message = WebSocketMessageSerializer.Deserialize<ProducePayload>(rawData);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to deserialize produce message");
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Invalid produce message format");
            return;
        }

        if (message?.Payload == null)
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Missing payload");
            return;
        }

        var payload = message.Payload;

        if (string.IsNullOrEmpty(payload.Topic))
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Topic is required");
            return;
        }

        if (string.IsNullOrEmpty(payload.Value))
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Value is required");
            return;
        }

        // Get the client for the session's cluster
        if (!_clusterRegistry.TryGetClient(session.ClusterId, out var client) || client == null)
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.UnknownCluster, $"Unknown cluster: {session.ClusterId}");
            return;
        }

        try
        {
            // Decode base64 if needed or use string directly
            var key = DecodeIfBase64(payload.Key);
            var value = DecodeIfBase64(payload.Value) ?? string.Empty;

            var partition = payload.Partition ?? 0;

            _logger.LogDebug(
                "Producing message to topic {Topic}, partition {Partition} on session {SessionId}",
                payload.Topic, partition, session.SessionId);

            var offset = await client.Messaging.SendAsync(
                payload.Topic,
                partition,
                key,
                value,
                session.CancellationToken);

            var response = WebSocketMessageSerializer.CreateProduceResponse(
                requestId,
                success: true,
                topic: payload.Topic,
                partition: partition,
                offset: offset,
                timestamp: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

            await session.SendAsync(response);

            _logger.LogDebug(
                "Produced message to topic {Topic}, partition {Partition}, offset {Offset}",
                payload.Topic, partition, offset);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to produce message to topic {Topic}", payload.Topic);
            await SendErrorAsync(session, requestId, WebSocketErrorCode.ProduceFailed, ex.Message);
        }
    }

    /// <summary>
    /// Handles a batch produce request.
    /// </summary>
    public async Task HandleProduceBatchAsync(
        WebSocketSession session,
        string? requestId,
        byte[] rawData)
    {
        WebSocketMessage<ProduceBatchPayload>? message;
        try
        {
            message = WebSocketMessageSerializer.Deserialize<ProduceBatchPayload>(rawData);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to deserialize produce batch message");
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Invalid produce batch message format");
            return;
        }

        if (message?.Payload == null)
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Missing payload");
            return;
        }

        var payload = message.Payload;

        if (string.IsNullOrEmpty(payload.Topic))
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Topic is required");
            return;
        }

        if (payload.Records == null || payload.Records.Length == 0)
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.InvalidMessage, "Records array is required");
            return;
        }

        // Get the client for the session's cluster
        if (!_clusterRegistry.TryGetClient(session.ClusterId, out var client) || client == null)
        {
            await SendErrorAsync(session, requestId, WebSocketErrorCode.UnknownCluster, $"Unknown cluster: {session.ClusterId}");
            return;
        }

        try
        {
            // Group records by partition
            var recordsByPartition = payload.Records
                .GroupBy(r => r.Partition ?? 0)
                .ToList();

            var results = new List<ProduceBatchResult>();
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            foreach (var group in recordsByPartition)
            {
                var partition = group.Key;
                var records = group.ToList();

                // Convert records to batch format
                var batchMessages = new List<(string? Key, string Value)>(records.Count);
                foreach (var record in records)
                {
                    var key = DecodeIfBase64(record.Key);
                    var value = DecodeIfBase64(record.Value) ?? string.Empty;
                    batchMessages.Add((key, value));
                }

                _logger.LogDebug(
                    "Producing batch of {Count} messages to topic {Topic}, partition {Partition}",
                    batchMessages.Count, payload.Topic, partition);

                var offset = await client.Messaging.SendBatchAsync(
                    payload.Topic,
                    partition,
                    batchMessages,
                    session.CancellationToken);

                // Create results for each message in this partition
                for (int i = 0; i < records.Count; i++)
                {
                    results.Add(new ProduceBatchResult
                    {
                        Partition = partition,
                        Offset = offset + i, // Approximate offsets
                        Timestamp = timestamp
                    });
                }
            }

            var response = new WebSocketMessage<ProduceBatchResponsePayload>
            {
                Type = WebSocketMessageType.ProduceBatchResponse,
                Id = requestId,
                Payload = new ProduceBatchResponsePayload
                {
                    Success = true,
                    Topic = payload.Topic,
                    Results = [.. results]
                }
            };

            await session.SendAsync(response);

            _logger.LogDebug(
                "Produced batch of {Count} records to topic {Topic}",
                payload.Records.Length, payload.Topic);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to produce batch to topic {Topic}", payload.Topic);
            await SendErrorAsync(session, requestId, WebSocketErrorCode.ProduceFailed, ex.Message);
        }
    }

    private static string? DecodeIfBase64(string? input)
    {
        if (string.IsNullOrEmpty(input))
            return null;

        // Try to detect if it's base64 by checking if it only contains valid base64 chars
        // For simplicity, we'll try to decode and fall back to using the string directly
        try
        {
            var bytes = Convert.FromBase64String(input);
            return Encoding.UTF8.GetString(bytes);
        }
        catch
        {
            // If not valid base64, use the string as-is
            return input;
        }
    }

    private static async Task SendErrorAsync(WebSocketSession session, string? requestId, string code, string message)
    {
        var error = WebSocketMessageSerializer.CreateError(requestId, code, message);
        await session.SendAsync(error);
    }
}
