using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Queue;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Kuestenlogik.Surgewave.Protocol.Amqp;

/// <summary>
/// AMQP 0.9.1 protocol adapter that bridges AMQP clients to Surgewave topics.
/// Runs as a hosted service, listening on a configurable TCP port (default 5672).
/// Disabled by default — enable via Surgewave:Amqp:Enabled=true.
/// </summary>
/// <remarks>
/// <para><b>Delivery semantics:</b> When a <see cref="IQueueViewManager"/> is registered in DI the adapter
/// uses QueueView semantics (RabbitMQ-compatible visibility timeouts and requeue).
/// Without QueueViewManager it falls back to log-based offset tracking (like Kafka).</para>
/// <list type="bullet">
///   <item><b>Basic.Ack</b> → QueueView.Ack (or offset commit in fallback mode)</item>
///   <item><b>Basic.Nack requeue=true</b> → QueueView.Nack(requeue:true) — message redelivered</item>
///   <item><b>Basic.Nack requeue=false</b> → QueueView.Nack(requeue:false) — message dropped</item>
///   <item><b>Basic.Reject requeue=true</b> → QueueView.Nack(requeue:true)</item>
///   <item><b>Basic.Reject requeue=false</b> → QueueView.RejectAsync → DLQ routing</item>
/// </list>
/// <para>In fallback mode (no QueueView), Basic.Nack/Reject with requeue=true is logged as a warning
/// and is not supported because log-based systems cannot re-insert messages.</para>
///
/// <para><b>Supported AMQP methods:</b></para>
/// <list type="bullet">
///   <item>Connection.Start / StartOk / Tune / TuneOk / Open / OpenOk / Close / CloseOk</item>
///   <item>Channel.Open / OpenOk / Close / CloseOk</item>
///   <item>Exchange.Declare / DeclareOk</item>
///   <item>Queue.Declare / DeclareOk / Bind / BindOk</item>
///   <item>Basic.Publish / Consume / ConsumeOk / Deliver / Ack / Nack / Reject</item>
///   <item>Heartbeat (both directions)</item>
/// </list>
/// </remarks>
public sealed class AmqpBrokerAdapter : BackgroundService
{
    // AMQP 0.9.1 protocol header bytes: "AMQP" + {0, 0, 9, 1}
    private static readonly byte[] ProtocolHeader = [0x41, 0x4D, 0x51, 0x50, 0x00, 0x00, 0x09, 0x01];

    // AMQP class/method IDs
    private const ushort ClassConnection = 10;
    private const ushort ClassChannel = 20;
    private const ushort ClassExchange = 40;
    private const ushort ClassQueue = 50;
    private const ushort ClassBasic = 60;

    private const ushort MethodConnectionStartOk = 11;
    private const ushort MethodConnectionTuneOk = 31;
    private const ushort MethodConnectionOpen = 40;
    private const ushort MethodConnectionClose = 50;
    private const ushort MethodConnectionCloseOk = 51;

    private const ushort MethodChannelOpen = 10;
    private const ushort MethodChannelClose = 40;

    private const ushort MethodExchangeDeclare = 10;

    private const ushort MethodQueueDeclare = 10;
    private const ushort MethodQueueBind = 20;

    private const ushort MethodBasicPublish = 40;
    private const ushort MethodBasicConsume = 20;
    private const ushort MethodBasicAck = 80;
    private const ushort MethodBasicNack = 120;
    private const ushort MethodBasicReject = 90;

    private readonly AmqpConfig _config;
    private readonly LogManager _logManager;
    private readonly IQueueViewManager? _queueViewManager;
    private readonly ILogger<AmqpBrokerAdapter> _logger;
    private TcpListener? _listener;
    private int _activeConnections;

    public AmqpBrokerAdapter(
        IOptions<AmqpConfig> config,
        LogManager logManager,
        ILogger<AmqpBrokerAdapter> logger,
        IQueueViewManager? queueViewManager = null)
    {
        _config = config.Value;
        _logManager = logManager;
        _queueViewManager = queueViewManager;
        _logger = logger;
    }

    /// <summary>
    /// Gets the number of currently active AMQP connections.
    /// </summary>
    public int ActiveConnections => _activeConnections;

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.Enabled)
        {
            _logger.LogDebug("AMQP protocol adapter is disabled");
            return;
        }

        _listener = new TcpListener(IPAddress.Any, _config.Port);
        _listener.Start();

        _logger.LogInformation(
            "AMQP 0.9.1 protocol adapter listening on port {Port} (max connections: {MaxConnections})",
            _config.Port, _config.MaxConnections);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await _listener.AcceptTcpClientAsync(stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                if (_activeConnections >= _config.MaxConnections)
                {
                    _logger.LogWarning(
                        "AMQP max connections reached ({Max}), dropping incoming connection",
                        _config.MaxConnections);
                    client.Dispose();
                    continue;
                }

                _ = HandleClientAsync(client, stoppingToken);
            }
        }
        finally
        {
            _listener.Stop();
            _logger.LogInformation("AMQP protocol adapter stopped");
        }
    }

    // -------------------------------------------------------------------------
    // Per-connection lifecycle
    // -------------------------------------------------------------------------

    private async Task HandleClientAsync(TcpClient client, CancellationToken ct)
    {
        Interlocked.Increment(ref _activeConnections);
        var remoteEndPoint = client.Client.RemoteEndPoint?.ToString() ?? "unknown";

        _logger.LogInformation("AMQP client connected: {Endpoint} (active: {Active})",
            remoteEndPoint, _activeConnections);

        try
        {
            using (client)
            {
                var stream = client.GetStream();
                await ServeConnectionAsync(stream, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { /* shutdown */ }
        catch (IOException ex)
        {
            _logger.LogDebug("AMQP client {Endpoint} disconnected: {Message}", remoteEndPoint, ex.Message);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "AMQP client {Endpoint} error", remoteEndPoint);
        }
        finally
        {
            Interlocked.Decrement(ref _activeConnections);
            _logger.LogInformation("AMQP client disconnected: {Endpoint} (active: {Active})",
                remoteEndPoint, _activeConnections);
        }
    }

    private async Task ServeConnectionAsync(Stream stream, CancellationToken ct)
    {
        // Read and validate the 8-byte AMQP protocol header
        var headerBuf = new byte[8];
        int totalRead = 0;
        while (totalRead < 8)
        {
            var r = await stream.ReadAsync(headerBuf.AsMemory(totalRead, 8 - totalRead), ct)
                .ConfigureAwait(false);
            if (r == 0) return;
            totalRead += r;
        }

        if (!headerBuf.AsSpan().SequenceEqual(ProtocolHeader))
        {
            // Echo back the correct header so the client knows what protocol version we expect
            await stream.WriteAsync(ProtocolHeader, ct).ConfigureAwait(false);
            await stream.FlushAsync(ct).ConfigureAwait(false);
            return;
        }

        var reader = new AmqpFrameReader(stream, _config.MaxFrameSize);
        var writer = new AmqpFrameWriter(stream);

        // Initiate handshake: send Connection.Start
        await writer.WriteConnectionStartAsync(0, ct).ConfigureAwait(false);

        // Per-connection channel map and in-progress publish state
        var channels = new Dictionary<ushort, AmqpChannelState>();
        var pending = new AmqpPendingPublish();
        var deliveryTagCounter = new AmqpDeliveryTagCounter();

        while (!ct.IsCancellationRequested)
        {
            var frame = await reader.ReadFrameAsync(ct).ConfigureAwait(false);
            if (frame is null)
                break;

            // Heartbeat: echo it back
            if (frame.Type == AmqpFrameType.Heartbeat)
            {
                await writer.WriteHeartbeatAsync(ct).ConfigureAwait(false);
                continue;
            }

            // Content body frame: accumulate body for the pending publish
            if (frame.Type == AmqpFrameType.Body && pending.IsActive && pending.Body is not null)
            {
                var bodyOffset = (int)(pending.Body.Length - pending.RemainingBytes);
                frame.Payload.AsSpan().CopyTo(pending.Body.AsSpan(bodyOffset));
                pending.RemainingBytes -= frame.Payload.Length;

                if (pending.RemainingBytes <= 0)
                {
                    await ProduceToSurgewaveAsync(channels, pending.Channel,
                        pending.Exchange!, pending.RoutingKey!, pending.Body, ct)
                        .ConfigureAwait(false);
                    pending.Reset();
                }
                continue;
            }

            // Content header frame: determine body size for the pending publish
            if (frame.Type == AmqpFrameType.Header && pending.IsActive)
            {
                if (frame.Payload.Length >= 12)
                {
                    var bodySize = (long)BinaryPrimitives.ReadUInt64BigEndian(frame.Payload.AsSpan(4, 8));
                    pending.Body = new byte[bodySize];
                    pending.RemainingBytes = bodySize;

                    if (bodySize == 0)
                    {
                        await ProduceToSurgewaveAsync(channels, pending.Channel,
                            pending.Exchange!, pending.RoutingKey!, [], ct)
                            .ConfigureAwait(false);
                        pending.Reset();
                    }
                }
                continue;
            }

            if (frame.Type != AmqpFrameType.Method)
                continue;

            var payload = frame.Payload;
            if (payload.Length < 4) continue;

            var classId = BinaryPrimitives.ReadUInt16BigEndian(payload.AsSpan(0, 2));
            var methodId = BinaryPrimitives.ReadUInt16BigEndian(payload.AsSpan(2, 2));
            var args = payload[4..];

            switch (classId)
            {
                case ClassConnection:
                    if (!await HandleConnectionMethodAsync(methodId, args, writer, ct).ConfigureAwait(false))
                        return;
                    break;

                case ClassChannel:
                    await HandleChannelMethodAsync(methodId, frame.Channel, channels, writer, ct)
                        .ConfigureAwait(false);
                    break;

                case ClassExchange:
                    await HandleExchangeMethodAsync(methodId, frame.Channel, args, channels, writer, ct)
                        .ConfigureAwait(false);
                    break;

                case ClassQueue:
                    await HandleQueueMethodAsync(methodId, frame.Channel, args, channels, writer, ct)
                        .ConfigureAwait(false);
                    break;

                case ClassBasic:
                    await HandleBasicMethodAsync(
                        methodId, frame.Channel, args, channels, writer, pending,
                        deliveryTagCounter, ct).ConfigureAwait(false);
                    break;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Method handlers
    // -------------------------------------------------------------------------

    /// <returns>False when the connection should be closed.</returns>
    private async Task<bool> HandleConnectionMethodAsync(
        ushort methodId, byte[] args,
        AmqpFrameWriter writer, CancellationToken ct)
    {
        switch (methodId)
        {
            case MethodConnectionStartOk:
                await writer.WriteConnectionTuneAsync(
                    0,
                    (ushort)_config.MaxChannels,
                    (uint)_config.MaxFrameSize,
                    (ushort)_config.HeartbeatInterval,
                    ct).ConfigureAwait(false);
                break;

            case MethodConnectionTuneOk:
                // Client accepted our Tune — wait for Open
                break;

            case MethodConnectionOpen:
                var vhost = ReadShortString(args, 0, out _);
                _logger.LogDebug("AMQP client opening virtual-host: {VHost}", vhost);
                await writer.WriteConnectionOpenOkAsync(0, ct).ConfigureAwait(false);
                break;

            case MethodConnectionClose:
                await writer.WriteConnectionCloseOkAsync(0, ct).ConfigureAwait(false);
                return false;

            case MethodConnectionCloseOk:
                return false;
        }
        return true;
    }

    private async Task HandleChannelMethodAsync(
        ushort methodId, ushort channel,
        Dictionary<ushort, AmqpChannelState> channels,
        AmqpFrameWriter writer, CancellationToken ct)
    {
        switch (methodId)
        {
            case MethodChannelOpen:
                channels[channel] = new AmqpChannelState(channel);
                await writer.WriteChannelOpenOkAsync(channel, ct).ConfigureAwait(false);
                break;

            case MethodChannelClose:
                channels.Remove(channel);
                await writer.WriteChannelCloseOkAsync(channel, ct).ConfigureAwait(false);
                break;
        }
    }

    private async Task HandleExchangeMethodAsync(
        ushort methodId, ushort channel, byte[] args,
        Dictionary<ushort, AmqpChannelState> channels,
        AmqpFrameWriter writer, CancellationToken ct)
    {
        if (methodId != MethodExchangeDeclare) return;

        // ticket(2) + exchange-name(short-string) + type(short-string) + flags(1)
        var offset = 2;
        var exchangeName = ReadShortString(args, offset, out int len);
        offset += 1 + len;
        var typeName = ReadShortString(args, offset, out _);

        var exchangeType = typeName.ToLowerInvariant() switch
        {
            "fanout" => AmqpExchangeType.Fanout,
            "topic" => AmqpExchangeType.Topic,
            "headers" => AmqpExchangeType.Headers,
            _ => AmqpExchangeType.Direct,
        };

        if (channels.TryGetValue(channel, out var ch))
            ch.Exchanges[exchangeName] = exchangeType;

        _logger.LogDebug("AMQP Exchange.Declare: {Name} type={Type}", exchangeName, typeName);
        await writer.WriteExchangeDeclareOkAsync(channel, ct).ConfigureAwait(false);
    }

    private async Task HandleQueueMethodAsync(
        ushort methodId, ushort channel, byte[] args,
        Dictionary<ushort, AmqpChannelState> channels,
        AmqpFrameWriter writer, CancellationToken ct)
    {
        if (!channels.TryGetValue(channel, out var ch)) return;

        switch (methodId)
        {
            case MethodQueueDeclare:
            {
                // ticket(2) + queue-name(short-string) + flags(1) + arguments(table)
                var queueName = ReadShortString(args, 2, out _);
                if (string.IsNullOrEmpty(queueName))
                    queueName = $"amq.queue.{Guid.NewGuid():N}";

                var surgewaveGroup = AmqpTopicMapper.MapQueueToConsumerGroup(queueName);
                ch.Queues[queueName] = surgewaveGroup;

                _logger.LogDebug("AMQP Queue.Declare: {Queue} → Surgewave group {Group}", queueName, surgewaveGroup);
                await writer.WriteQueueDeclareOkAsync(channel, queueName, 0, 0, ct).ConfigureAwait(false);
                break;
            }

            case MethodQueueBind:
            {
                // ticket(2) + queue(short-string) + exchange(short-string) + routing-key(short-string)
                var offset = 2;
                var queueName = ReadShortString(args, offset, out int len1); offset += 1 + len1;
                var exchangeName = ReadShortString(args, offset, out int len2); offset += 1 + len2;
                var routingKey = ReadShortString(args, offset, out _);

                ch.Bindings[queueName] = (exchangeName, routingKey);

                _logger.LogDebug("AMQP Queue.Bind: {Queue} ← {Exchange}/{Key}",
                    queueName, exchangeName, routingKey);
                await writer.WriteQueueBindOkAsync(channel, ct).ConfigureAwait(false);
                break;
            }
        }
    }

    private async Task HandleBasicMethodAsync(
        ushort methodId, ushort channel, byte[] args,
        Dictionary<ushort, AmqpChannelState> channels,
        AmqpFrameWriter writer,
        AmqpPendingPublish pending,
        AmqpDeliveryTagCounter deliveryTagCounter,
        CancellationToken ct)
    {
        switch (methodId)
        {
            case MethodBasicPublish:
            {
                // ticket(2) + exchange(short-string) + routing-key(short-string) + flags(1)
                var offset = 2;
                pending.Exchange = ReadShortString(args, offset, out int len1);
                offset += 1 + len1;
                pending.RoutingKey = ReadShortString(args, offset, out _);
                pending.Channel = channel;
                // Header + Body frames follow
                break;
            }

            case MethodBasicConsume:
            {
                // ticket(2) + queue(short-string) + consumer-tag(short-string) + flags(1)
                var offset = 2;
                var queueName = ReadShortString(args, offset, out int len1);
                offset += 1 + len1;
                var consumerTag = ReadShortString(args, offset, out _);

                if (string.IsNullOrEmpty(consumerTag))
                    consumerTag = $"amq.ctag.{Guid.NewGuid():N}";

                if (channels.TryGetValue(channel, out var ch))
                    ch.Consumers[consumerTag] = queueName;

                _logger.LogDebug("AMQP Basic.Consume: tag={Tag} queue={Queue}", consumerTag, queueName);
                await writer.WriteBasicConsumeOkAsync(channel, consumerTag, ct).ConfigureAwait(false);

                _ = ConsumeLoopAsync(channel, consumerTag, queueName,
                    channels, writer, deliveryTagCounter.Current, ct);
                break;
            }

            case MethodBasicAck:
            {
                // deliveryTag(8) + multiple(1)
                var deliveryTag = BinaryPrimitives.ReadUInt64BigEndian(args);
                var multiple = args.Length > 8 && args[8] != 0;

                if (channels.TryGetValue(channel, out var ackCh))
                {
                    if (_queueViewManager != null)
                    {
                        // QueueView path: look up message IDs and ack them
                        if (multiple)
                        {
                            var tagsToAck = ackCh.DeliveryTagToMessageId
                                .Where(kv => kv.Key <= deliveryTag)
                                .ToList();
                            foreach (var kv in tagsToAck)
                            {
                                var view = _queueViewManager.Get(
                                    ExtractTopicFromMessageId(kv.Value));
                                view?.Ack(kv.Value);
                                ackCh.DeliveryTagToMessageId.Remove(kv.Key);
                            }
                        }
                        else if (ackCh.DeliveryTagToMessageId.Remove(deliveryTag, out var msgId))
                        {
                            var view = _queueViewManager.Get(ExtractTopicFromMessageId(msgId));
                            view?.Ack(msgId);
                        }
                    }
                    else
                    {
                        // Fallback: offset-commit path
                        if (multiple)
                        {
                            var toRemove = ackCh.DeliveryTagToOffset
                                .Where(kv => kv.Key <= deliveryTag)
                                .ToList();
                            foreach (var kv in toRemove)
                            {
                                ackCh.CommittedOffsets[(kv.Value.Topic, kv.Value.Partition)] = kv.Value.Offset;
                                ackCh.DeliveryTagToOffset.Remove(kv.Key);
                            }
                        }
                        else if (ackCh.DeliveryTagToOffset.Remove(deliveryTag, out var mapped))
                        {
                            ackCh.CommittedOffsets[(mapped.Topic, mapped.Partition)] = mapped.Offset;
                        }
                    }
                }

                _logger.LogDebug("AMQP Basic.Ack deliveryTag={Tag} multiple={Multiple} channel={Channel}",
                    deliveryTag, multiple, channel);
                break;
            }

            case MethodBasicNack:
            {
                // deliveryTag(8) + flags(1): bit 0 = multiple, bit 1 = requeue
                var deliveryTag = BinaryPrimitives.ReadUInt64BigEndian(args);
                var flags = args.Length > 8 ? args[8] : (byte)0;
                var requeue = (flags & 0x02) != 0;

                if (channels.TryGetValue(channel, out var nackCh))
                {
                    if (_queueViewManager != null)
                    {
                        // QueueView path: nack with or without requeue
                        if (nackCh.DeliveryTagToMessageId.Remove(deliveryTag, out var msgId))
                        {
                            var view = _queueViewManager.Get(ExtractTopicFromMessageId(msgId));
                            view?.Nack(msgId, requeue);

                            _logger.LogDebug(
                                "AMQP Basic.Nack deliveryTag={Tag} requeue={Requeue} channel={Channel}",
                                deliveryTag, requeue, channel);
                        }
                    }
                    else
                    {
                        // Fallback: offset-commit path (requeue not supported)
                        if (requeue)
                        {
                            _logger.LogWarning(
                                "AMQP Basic.Nack with requeue=true is not supported without QueueViewManager " +
                                "(Surgewave uses log-based offsets). Message will not be requeued. " +
                                "DeliveryTag={Tag} Channel={Channel}",
                                deliveryTag, channel);
                        }
                        else
                        {
                            _logger.LogDebug("AMQP Basic.Nack deliveryTag={Tag} channel={Channel} (message dropped)",
                                deliveryTag, channel);
                        }

                        nackCh.DeliveryTagToOffset.Remove(deliveryTag);
                    }
                }
                break;
            }

            case MethodBasicReject:
            {
                // deliveryTag(8) + requeue(1)
                var deliveryTag = BinaryPrimitives.ReadUInt64BigEndian(args);
                var requeue = args.Length > 8 && args[8] != 0;

                if (channels.TryGetValue(channel, out var rejectCh))
                {
                    if (_queueViewManager != null)
                    {
                        if (rejectCh.DeliveryTagToMessageId.Remove(deliveryTag, out var msgId))
                        {
                            var view = _queueViewManager.Get(ExtractTopicFromMessageId(msgId));
                            if (view != null)
                            {
                                if (requeue)
                                {
                                    // Reject with requeue → nack, make eligible for redelivery
                                    view.Nack(msgId, requeue: true);
                                    _logger.LogDebug(
                                        "AMQP Basic.Reject requeue=true: {Tag} re-queued via QueueView",
                                        deliveryTag);
                                }
                                else
                                {
                                    // Reject without requeue → DLQ routing
                                    _ = Task.Run(async () =>
                                        await view.RejectAsync(msgId, ct).ConfigureAwait(false));
                                    _logger.LogDebug(
                                        "AMQP Basic.Reject requeue=false: {Tag} routed to DLQ via QueueView",
                                        deliveryTag);
                                }
                            }
                        }
                    }
                    else
                    {
                        // Fallback: offset-commit path (requeue not supported)
                        if (requeue)
                        {
                            _logger.LogWarning(
                                "AMQP Basic.Reject with requeue=true is not supported without QueueViewManager " +
                                "(Surgewave uses log-based offsets). Message will not be requeued. " +
                                "DeliveryTag={Tag} Channel={Channel}",
                                deliveryTag, channel);
                        }

                        rejectCh.DeliveryTagToOffset.Remove(deliveryTag);
                    }
                }
                break;
            }
        }
    }

    // -------------------------------------------------------------------------
    // Surgewave produce / consume
    // -------------------------------------------------------------------------

    private async Task ProduceToSurgewaveAsync(
        Dictionary<ushort, AmqpChannelState> channels,
        ushort channel,
        string exchangeName,
        string routingKey,
        byte[] body,
        CancellationToken ct)
    {
        if (body.Length == 0) return;

        var exchangeType = AmqpExchangeType.Direct;
        if (channels.TryGetValue(channel, out var ch) &&
            ch.Exchanges.TryGetValue(exchangeName, out var et))
        {
            exchangeType = et;
        }

        var surgewaveTopic = AmqpTopicMapper.MapToSurgewaveTopic(exchangeName, routingKey, exchangeType);

        try
        {
            var tp = new TopicPartition { Topic = surgewaveTopic, Partition = 0 };
            await _logManager.AppendBatchAsync(tp, body).ConfigureAwait(false);

            _logger.LogDebug(
                "AMQP PUBLISH {Exchange}/{RoutingKey} → Surgewave topic {Topic} ({Bytes} bytes)",
                exchangeName, routingKey, surgewaveTopic, body.Length);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to produce AMQP message to Surgewave topic {Topic}", surgewaveTopic);
        }
    }

    private async Task ConsumeLoopAsync(
        ushort channel,
        string consumerTag,
        string queueName,
        Dictionary<ushort, AmqpChannelState> channels,
        AmqpFrameWriter writer,
        ulong startDeliveryTag,
        CancellationToken ct)
    {
        // Resolve Surgewave topic: queue → consumer group → use group name as topic
        var surgewaveTopic = queueName;
        if (channels.TryGetValue(channel, out var ch) &&
            ch.Queues.TryGetValue(queueName, out var mapped))
        {
            surgewaveTopic = mapped;
        }

        var tp = new TopicPartition { Topic = surgewaveTopic, Partition = 0 };
        var deliveryTag = startDeliveryTag;

        _logger.LogDebug("AMQP consume loop started: tag={Tag} topic={Topic}", consumerTag, surgewaveTopic);

        // QueueView path: use visibility-timeout + requeue semantics
        if (_queueViewManager != null)
        {
            var log = _logManager.GetOrCreateLog(tp);
            var view = _queueViewManager.GetOrCreate(surgewaveTopic, log);

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var messages = await view.ReceiveAsync(
                        partition: 0,
                        maxMessages: 10,
                        consumerId: consumerTag,
                        ct: ct).ConfigureAwait(false);

                    if (messages.Count == 0)
                    {
                        await Task.Delay(100, ct).ConfigureAwait(false);
                        continue;
                    }

                    foreach (var msg in messages)
                    {
                        deliveryTag++;

                        // Map deliveryTag → QueueView messageId for Ack/Nack/Reject
                        if (channels.TryGetValue(channel, out var chState))
                            chState.DeliveryTagToMessageId[deliveryTag] = msg.MessageId;

                        await writer.WriteBasicDeliverAsync(
                            channel, consumerTag, deliveryTag,
                            redelivered: msg.DeliveryCount > 1,
                            exchange: "",
                            routingKey: queueName,
                            body: msg.Body,
                            ct: ct).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "AMQP consume loop (QueueView) error: tag={Tag}", consumerTag);
                    await Task.Delay(1000, ct).ConfigureAwait(false);
                }
            }
        }
        else
        {
            // Fallback path: direct log reads with offset tracking (no requeue support)
            long readOffset = 0;

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var batches = await _logManager
                        .ReadBatchesAsync(tp, readOffset, maxBytes: 65536, cancellationToken: ct)
                        .ConfigureAwait(false);

                    if (batches.Count == 0)
                    {
                        await Task.Delay(100, ct).ConfigureAwait(false);
                        continue;
                    }

                    foreach (var batch in batches)
                    {
                        deliveryTag++;

                        // Track deliveryTag → Surgewave offset for Ack-based offset commit
                        if (channels.TryGetValue(channel, out var chState))
                            chState.DeliveryTagToOffset[deliveryTag] = (surgewaveTopic, 0, readOffset);

                        await writer.WriteBasicDeliverAsync(
                            channel, consumerTag, deliveryTag,
                            redelivered: false,
                            exchange: "",
                            routingKey: queueName,
                            body: batch,
                            ct: ct).ConfigureAwait(false);
                        readOffset++;
                    }
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "AMQP consume loop (fallback) error: tag={Tag}", consumerTag);
                    await Task.Delay(1000, ct).ConfigureAwait(false);
                }
            }
        }

        _logger.LogDebug("AMQP consume loop ended: tag={Tag}", consumerTag);
    }

    /// <summary>
    /// Extracts the Surgewave topic name from a QueueView message ID string.
    /// Message IDs are formatted as "{topic}-{partition}-{offset}".
    /// Strips the trailing "-{partition}-{offset}" suffix.
    /// </summary>
    private static string ExtractTopicFromMessageId(string messageId)
    {
        // Format: "{topic}-{partition}-{offset}"
        // topic itself may contain hyphens, so find the last two hyphens
        var lastDash = messageId.LastIndexOf('-');
        if (lastDash <= 0) return messageId;
        var secondLastDash = messageId.LastIndexOf('-', lastDash - 1);
        if (secondLastDash <= 0) return messageId;
        return messageId[..secondLastDash];
    }

    // -------------------------------------------------------------------------
    // Parsing helpers
    // -------------------------------------------------------------------------

    private static string ReadShortString(byte[] buf, int offset, out int length)
    {
        if (offset >= buf.Length)
        {
            length = 0;
            return string.Empty;
        }
        var len = buf[offset];
        length = len;
        if (offset + 1 + len > buf.Length)
            return string.Empty;
        return Encoding.UTF8.GetString(buf, offset + 1, len);
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        _listener?.Stop();
        _listener?.Dispose();
        base.Dispose();
    }
}
