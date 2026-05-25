namespace Kuestenlogik.Surgewave.Transport;

/// <summary>
/// Specifies the transport type for Surgewave client-broker communication.
/// </summary>
public enum SurgewaveTransportType
{
    /// <summary>
    /// Automatically select the best transport.
    /// Uses SharedMemory if broker is on same machine, otherwise TCP.
    /// </summary>
    Auto = 0,

    /// <summary>
    /// Use TCP/IP sockets for network communication.
    /// Works across machines, ~45us latency.
    /// </summary>
    Tcp = 1,

    /// <summary>
    /// Use shared memory for same-machine IPC.
    /// Sub-microsecond latency, requires broker on same machine.
    /// </summary>
    SharedMemory = 2,

    /// <summary>
    /// Use raw QUIC (UDP + TLS 1.3) for network communication.
    /// 0-RTT reconnect, per-stream flow control, packet-loss resilient.
    /// </summary>
    Quic = 3
}
