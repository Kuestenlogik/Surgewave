using System.Diagnostics.CodeAnalysis;

namespace Kuestenlogik.Surgewave.Transport;

/// <summary>
/// Factory for creating Surgewave transports.
/// </summary>
/// <remarks>
/// All Create methods return transports that the caller is responsible for disposing.
/// </remarks>
[SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Caller owns returned transports")]
public static class SurgewaveTransportFactory
{
    private static Func<TransportOptions, ISurgewaveTransport>? _tcpFactory;
    private static Func<TransportOptions, ISurgewaveTransport>? _sharedMemoryFactory;
    private static Func<TransportOptions, ISurgewaveTransport>? _quicFactory;
    private static Func<TransportOptions, ValueTask<bool>>? _localBrokerDetector;

    /// <summary>
    /// Register the TCP transport factory.
    /// </summary>
    public static void RegisterTcpTransport(Func<TransportOptions, ISurgewaveTransport> factory)
    {
        _tcpFactory = factory;
    }

    /// <summary>
    /// Register the shared memory transport factory.
    /// </summary>
    public static void RegisterSharedMemoryTransport(Func<TransportOptions, ISurgewaveTransport> factory)
    {
        _sharedMemoryFactory = factory;
    }

    /// <summary>
    /// Register the QUIC transport factory.
    /// </summary>
    public static void RegisterQuicTransport(Func<TransportOptions, ISurgewaveTransport> factory)
    {
        _quicFactory = factory;
    }

    /// <summary>
    /// Register the local broker detector for auto-detection.
    /// </summary>
    public static void RegisterLocalBrokerDetector(Func<TransportOptions, ValueTask<bool>> detector)
    {
        _localBrokerDetector = detector;
    }

    /// <summary>
    /// Create a transport for the specified options and type.
    /// </summary>
    public static async ValueTask<ISurgewaveTransport> CreateAsync(
        TransportOptions options,
        SurgewaveTransportType transportType = SurgewaveTransportType.Auto)
    {
        return transportType switch
        {
            SurgewaveTransportType.Tcp => CreateTcpTransport(options),
            SurgewaveTransportType.SharedMemory => CreateSharedMemoryTransport(options),
            SurgewaveTransportType.Quic => CreateQuicTransport(options),
            SurgewaveTransportType.Auto => await CreateAutoTransportAsync(options),
            _ => throw new ArgumentOutOfRangeException(nameof(transportType))
        };
    }

    /// <summary>
    /// Create a QUIC transport.
    /// </summary>
    public static ISurgewaveTransport CreateQuicTransport(TransportOptions options)
    {
        if (_quicFactory == null)
        {
            throw new InvalidOperationException(
                "QUIC transport not registered. Add a reference to Kuestenlogik.Surgewave.Transport.Quic and call RegisterQuicTransport.");
        }
        return _quicFactory(options);
    }

    /// <summary>
    /// Create a TCP transport.
    /// </summary>
    public static ISurgewaveTransport CreateTcpTransport(TransportOptions options)
    {
        if (_tcpFactory == null)
        {
            throw new InvalidOperationException(
                "TCP transport not registered. Add a reference to Kuestenlogik.Surgewave.Transport.Tcp and call RegisterTcpTransport.");
        }
        return _tcpFactory(options);
    }

    /// <summary>
    /// Create a shared memory transport.
    /// </summary>
    public static ISurgewaveTransport CreateSharedMemoryTransport(TransportOptions options)
    {
        if (_sharedMemoryFactory == null)
        {
            throw new InvalidOperationException(
                "Shared memory transport not registered. Add a reference to Kuestenlogik.Surgewave.Transport.SharedMemory and call RegisterSharedMemoryTransport.");
        }
        return _sharedMemoryFactory(options);
    }

    /// <summary>
    /// Create the best available transport (auto-detect).
    /// Uses SharedMemory if broker is local, otherwise TCP.
    /// </summary>
    public static async ValueTask<ISurgewaveTransport> CreateAutoTransportAsync(TransportOptions options)
    {
        // Try shared memory if available and broker is local
        if (_sharedMemoryFactory != null && _localBrokerDetector != null)
        {
            var isLocal = await _localBrokerDetector(options);
            if (isLocal)
            {
                return _sharedMemoryFactory(options);
            }
        }

        // Fall back to TCP
        return CreateTcpTransport(options);
    }
}
