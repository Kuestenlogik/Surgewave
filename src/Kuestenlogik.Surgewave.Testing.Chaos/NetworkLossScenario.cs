using Kuestenlogik.Surgewave.Testing.Network;

namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// A chaos-testing scenario that interposes a lossy proxy between a client
/// and a broker endpoint so tests can observe how the system behaves under
/// configurable packet loss and added latency.
/// </summary>
/// <remarks>
/// <para>
/// Unlike <see cref="LatencyInjectionScenario"/>, this scenario does not hook
/// into <see cref="ChaosEngine"/>'s internal fault injection surface. Packet
/// loss must be simulated on the actual wire — the scenario owns a
/// <see cref="LossyUdpProxy"/> (real UDP datagram drops) or
/// <see cref="LossyTcpProxy"/> (latency only) from <c>Kuestenlogik.Surgewave.Testing.Network</c>
/// and forwards traffic through it.
/// </para>
/// <para>
/// Typical test usage:
/// <code>
/// await using var loss = NetworkLossScenario.CreateForUdp(
///     listenPort: 19000, upstreamPort: 9094, dropRate: 0.02, latencyMs: 10);
/// // Point a QUIC client at 127.0.0.1:19000 instead of :9094 — all its
/// // datagrams now experience 2 % packet loss and 10 ms one-way latency.
/// </code>
/// </para>
/// </remarks>
public sealed class NetworkLossScenario : IAsyncDisposable
{
    private readonly LossyUdpProxy? _udpProxy;
    private readonly LossyTcpProxy? _tcpProxy;
    private bool _disposed;

    // Private UDP ctor — owns the proxy via direct field assignment so the
    // CA2000 ownership tracker sees the lifecycle correctly.
    private NetworkLossScenario(int listenPort, int upstreamPort, double dropRate, int latencyMs)
    {
        _udpProxy = new LossyUdpProxy(listenPort, upstreamPort, dropRate, latencyMs);
        _ = _udpProxy.Start();
    }

    // Private TCP ctor — disambiguated by taking an int latency only and no drop rate.
    private NetworkLossScenario(int listenPort, int upstreamPort, int latencyMs)
    {
        _tcpProxy = new LossyTcpProxy(listenPort, upstreamPort, latencyMs);
        // LossyTcpProxy.Start() binds the listener synchronously and spawns
        // the accept loop on a background task, so awaiting the returned
        // Task.CompletedTask in a ctor would be pointless.
        _ = _tcpProxy.Start();
    }

    /// <summary>
    /// The local port clients should connect to in order to go through the proxy.
    /// </summary>
    public int ListenPort => _udpProxy?.ListenPort ?? _tcpProxy!.ListenPort;

    /// <summary>
    /// Number of datagrams dropped by the UDP proxy (0 for TCP scenarios).
    /// </summary>
    public long DroppedDatagrams => _udpProxy?.TotalDropped ?? 0;

    /// <summary>
    /// Number of datagrams successfully forwarded by the UDP proxy (0 for TCP scenarios).
    /// </summary>
    public long ForwardedDatagrams => _udpProxy?.TotalForwarded ?? 0;

    /// <summary>
    /// Starts a UDP-level loss + latency injection scenario. Packet drops are
    /// applied on real UDP datagrams, so protocols layered on top (QUIC,
    /// DTLS, ...) will engage their retransmission logic exactly as on a real
    /// lossy network path.
    /// </summary>
    /// <param name="listenPort">Local UDP port clients should send to.</param>
    /// <param name="upstreamPort">UDP port of the real broker/server.</param>
    /// <param name="dropRate">Per-datagram drop probability, 0.0 to 1.0.</param>
    /// <param name="latencyMs">One-way added latency in milliseconds.</param>
    public static NetworkLossScenario CreateForUdp(
        int listenPort,
        int upstreamPort,
        double dropRate,
        int latencyMs = 0)
        => new(listenPort, upstreamPort, dropRate, latencyMs);

    /// <summary>
    /// Starts a TCP-level latency injection scenario. Packet loss is not
    /// simulated here — application-level byte drops are not equivalent to
    /// IP-layer TCP loss; use <c>tc netem</c> or Clumsy at the kernel level
    /// for that. Only added one-way latency is injected.
    /// </summary>
    /// <param name="listenPort">Local TCP port clients should connect to.</param>
    /// <param name="upstreamPort">TCP port of the real broker/server.</param>
    /// <param name="latencyMs">One-way added latency in milliseconds.</param>
    public static NetworkLossScenario CreateForTcp(
        int listenPort,
        int upstreamPort,
        int latencyMs)
        => new(listenPort, upstreamPort, latencyMs);

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_udpProxy is not null)
        {
            await _udpProxy.DisposeAsync().ConfigureAwait(false);
        }
        if (_tcpProxy is not null)
        {
            await _tcpProxy.DisposeAsync().ConfigureAwait(false);
        }
    }
}
