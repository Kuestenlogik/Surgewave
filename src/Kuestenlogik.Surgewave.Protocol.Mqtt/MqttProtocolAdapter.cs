using System.Buffers;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MQTTnet;
using MQTTnet.Protocol;
using MQTTnet.Server;

namespace Kuestenlogik.Surgewave.Protocol.Mqtt;

/// <summary>
/// MQTT protocol adapter that bridges MQTT publish/subscribe to Surgewave topics
/// using MQTTnet's embedded server. Runs as a hosted service, listening on a
/// configurable TCP port (default 1883). Disabled by default -- enable via
/// Surgewave:Mqtt:Enabled=true.
/// </summary>
public sealed class MqttProtocolAdapter : BackgroundService
{
    private readonly MqttConfig _config;
    private readonly LogManager _logManager;
    private readonly ILogger<MqttProtocolAdapter> _logger;
    private MqttServer? _mqttServer;
    private int _activeClients;

    public MqttProtocolAdapter(
        IOptions<MqttConfig> config,
        LogManager logManager,
        ILogger<MqttProtocolAdapter> logger)
    {
        _config = config.Value;
        _logManager = logManager;
        _logger = logger;
    }

    /// <summary>
    /// Gets the number of currently connected MQTT clients.
    /// </summary>
    public int ActiveClients => _activeClients;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_config.Enabled)
        {
            _logger.LogDebug("MQTT protocol adapter is disabled");
            return;
        }

        var serverOptions = new MqttServerOptionsBuilder()
            .WithDefaultEndpoint()
            .WithDefaultEndpointPort(_config.Port)
            .WithMaxPendingMessagesPerClient(_config.MaxClients)
            .WithKeepAlive()
            .Build();

        var factory = new MqttServerFactory();
        _mqttServer = factory.CreateMqttServer(serverOptions);

        // Validate incoming connections
        _mqttServer.ValidatingConnectionAsync += OnValidatingConnectionAsync;

        // When an MQTT client publishes a message, produce to Surgewave
        _mqttServer.InterceptingPublishAsync += OnInterceptingPublishAsync;

        // Track connected/disconnected clients
        _mqttServer.ClientConnectedAsync += OnClientConnectedAsync;
        _mqttServer.ClientDisconnectedAsync += OnClientDisconnectedAsync;

        // Optionally intercept subscriptions for logging
        _mqttServer.InterceptingSubscriptionAsync += OnInterceptingSubscriptionAsync;

        await _mqttServer.StartAsync();

        _logger.LogInformation(
            "MQTT protocol adapter listening on port {Port} (max clients: {MaxClients})",
            _config.Port, _config.MaxClients);

        // Wait until cancellation is requested
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown
        }
        finally
        {
            await _mqttServer.StopAsync();
            _logger.LogInformation("MQTT protocol adapter stopped");
        }
    }

    private Task OnValidatingConnectionAsync(ValidatingConnectionEventArgs e)
    {
        // Enforce maximum client limit
        if (_activeClients >= _config.MaxClients)
        {
            _logger.LogWarning(
                "MQTT max clients reached ({MaxClients}), rejecting connection from {ClientId}",
                _config.MaxClients, e.ClientId);
            e.ReasonCode = MqttConnectReasonCode.QuotaExceeded;
            return Task.CompletedTask;
        }

        // Enforce authentication when anonymous access is disabled
        if (!_config.AllowAnonymous && string.IsNullOrEmpty(e.UserName))
        {
            _logger.LogWarning("MQTT client {ClientId} rejected: authentication required", e.ClientId);
            e.ReasonCode = MqttConnectReasonCode.NotAuthorized;
            return Task.CompletedTask;
        }

        _logger.LogDebug("MQTT client {ClientId} connection accepted", e.ClientId);
        return Task.CompletedTask;
    }

    private async Task OnInterceptingPublishAsync(InterceptingPublishEventArgs e)
    {
        // Skip internal server messages (e.g., from $SYS topics)
        if (string.IsNullOrEmpty(e.ApplicationMessage.Topic))
            return;

        var mqttTopic = e.ApplicationMessage.Topic;
        var surgewaveTopic = MapMqttToSurgewaveTopic(mqttTopic);

        // Extract payload from ReadOnlySequence<byte>
        var payload = e.ApplicationMessage.Payload.ToArray();

        if (payload.Length == 0)
            return;

        try
        {
            var tp = new TopicPartition { Topic = surgewaveTopic, Partition = 0 };
            await _logManager.AppendBatchAsync(tp, payload).ConfigureAwait(false);

            _logger.LogDebug(
                "MQTT PUBLISH from {ClientId}: {MqttTopic} -> {SurgewaveTopic} ({Bytes} bytes)",
                e.ClientId, mqttTopic, surgewaveTopic, payload.Length);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex,
                "Failed to produce MQTT message to Surgewave topic {SurgewaveTopic} from {ClientId}",
                surgewaveTopic, e.ClientId);
        }
    }

    private Task OnClientConnectedAsync(ClientConnectedEventArgs e)
    {
        Interlocked.Increment(ref _activeClients);
        _logger.LogInformation("MQTT client connected: {ClientId} (active: {ActiveClients})",
            e.ClientId, _activeClients);
        return Task.CompletedTask;
    }

    private Task OnClientDisconnectedAsync(ClientDisconnectedEventArgs e)
    {
        Interlocked.Decrement(ref _activeClients);
        _logger.LogInformation("MQTT client disconnected: {ClientId} (active: {ActiveClients})",
            e.ClientId, _activeClients);
        return Task.CompletedTask;
    }

    private Task OnInterceptingSubscriptionAsync(InterceptingSubscriptionEventArgs e)
    {
        _logger.LogDebug("MQTT client {ClientId} subscribing to {TopicFilter}",
            e.ClientId, e.TopicFilter.Topic);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Maps an MQTT topic to a Surgewave topic name.
    /// Replaces MQTT '/' separators with '.' and prepends the configured prefix.
    /// For example, "sensors/temp" becomes "mqtt.sensors.temp" with prefix "mqtt.".
    /// </summary>
    internal static string MapMqttToSurgewaveTopic(string mqttTopic, string topicPrefix)
    {
        var normalized = mqttTopic.Replace('/', '.');
        return string.Concat(topicPrefix, normalized);
    }

    private string MapMqttToSurgewaveTopic(string mqttTopic)
        => MapMqttToSurgewaveTopic(mqttTopic, _config.TopicPrefix);

    /// <inheritdoc />
    public override void Dispose()
    {
        _mqttServer?.Dispose();
        base.Dispose();
    }
}
