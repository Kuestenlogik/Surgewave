using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Transport;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// Manages connection lifecycle for Surgewave native client.
/// Handles transport creation, connection, and disposal.
/// </summary>
public sealed class SurgewaveConnectionManager : IAsyncDisposable
{
    // Ensure TCP transport is registered
    static SurgewaveConnectionManager()
    {
        TcpTransportRegistration.Register();
    }

    private readonly TransportOptions _transportOptions;
    private readonly SurgewaveTransportType _transportType;
    private readonly ILogger _logger;
    private ISurgewaveTransport? _transport;
    private bool _enableCompression = true;

    /// <summary>
    /// Creates a connection manager with host and port.
    /// </summary>
    public SurgewaveConnectionManager(string host, int port, bool enablePipelining = true)
        : this(host, port, SurgewaveTransportType.Tcp, enablePipelining)
    {
    }

    /// <summary>
    /// Creates a connection manager with specific transport type.
    /// </summary>
    public SurgewaveConnectionManager(
        string host,
        int port,
        SurgewaveTransportType transportType,
        bool enablePipelining = true,
        ILogger? logger = null)
    {
        _transportOptions = new TransportOptions
        {
            Host = host,
            Port = port,
            EnablePipelining = enablePipelining
        };
        _transportType = transportType;
        _logger = logger ?? NullLogger.Instance;
    }

    /// <summary>
    /// Creates a connection manager with a pre-existing transport.
    /// </summary>
    public SurgewaveConnectionManager(ISurgewaveTransport transport)
    {
        _transport = transport;
        _transportOptions = new TransportOptions { Host = "", Port = 0 };
        _transportType = transport.TransportType;
        _logger = NullLogger.Instance;
    }

    /// <summary>
    /// Whether the transport is currently connected.
    /// </summary>
    public bool IsConnected => _transport?.IsConnected ?? false;

    /// <summary>
    /// Enable or disable compression for large payloads (default: enabled).
    /// </summary>
    public bool CompressionEnabled
    {
        get => _enableCompression;
        set => _enableCompression = value;
    }

    /// <summary>
    /// Maximum number of connection attempts before giving up.
    /// </summary>
    public int MaxConnectRetries { get; set; } = 10;

    /// <summary>
    /// Base delay between connection retries (doubles each attempt, capped at 10s).
    /// </summary>
    public TimeSpan ConnectRetryBaseDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Connect to the Surgewave broker with automatic retry on failure.
    /// </summary>
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        _transport ??= await SurgewaveTransportFactory.CreateAsync(_transportOptions, _transportType);

        for (int attempt = 1; ; attempt++)
        {
            try
            {
                await _transport.ConnectAsync(cancellationToken);
                return;
            }
            catch (Exception ex) when (attempt < MaxConnectRetries && !cancellationToken.IsCancellationRequested)
            {
                var delay = TimeSpan.FromTicks(
                    Math.Min(ConnectRetryBaseDelay.Ticks * (1L << Math.Min(attempt - 1, 4)),
                             TimeSpan.FromSeconds(10).Ticks));

                _logger.LogWarning(ex,
                    "Surgewave broker nicht erreichbar ({Host}:{Port}), Versuch {Attempt}/{MaxRetries}. Retry in {Delay}s",
                    _transportOptions.Host, _transportOptions.Port, attempt, MaxConnectRetries, delay.TotalSeconds);

                await Task.Delay(delay, cancellationToken);

                // Recreate transport for a fresh connection attempt
                await _transport.DisposeAsync();
                _transport = await SurgewaveTransportFactory.CreateAsync(_transportOptions, _transportType);
            }
        }
    }

    /// <summary>
    /// Send a request and receive a response.
    /// </summary>
    internal async Task<(SurgewaveResponseHeader Header, ReadOnlyMemory<byte> Payload)> SendRequestAsync(
        SurgewaveOpCode opCode,
        ReadOnlyMemory<byte> payload,
        CancellationToken cancellationToken)
    {
        EnsureConnected();
        var result = await _transport!.SendRequestAsync(opCode, payload, _enableCompression, cancellationToken);
        return (result.Header, result.Payload);
    }

    /// <summary>
    /// Register a handler for unsolicited server-push messages (streaming subscriptions).
    /// </summary>
    internal void RegisterPushHandler(SurgewaveOpCode opCode, Func<SurgewaveResponseHeader, ReadOnlyMemory<byte>, Task> handler)
    {
        EnsureConnected();
        _transport!.RegisterPushHandler(opCode, handler);
    }

    /// <summary>
    /// Remove a previously registered push handler.
    /// </summary>
    internal void UnregisterPushHandler(SurgewaveOpCode opCode)
    {
        _transport?.UnregisterPushHandler(opCode);
    }

    /// <summary>
    /// Throws if not connected.
    /// </summary>
    internal void EnsureConnected()
    {
        if (_transport == null || !_transport.IsConnected)
        {
            throw new InvalidOperationException("Client is not connected. Call ConnectAsync first.");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_transport != null)
        {
            await _transport.DisposeAsync();
            _transport = null;
        }
    }
}
