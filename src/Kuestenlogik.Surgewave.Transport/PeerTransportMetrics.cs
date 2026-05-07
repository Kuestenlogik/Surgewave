using System.Diagnostics.Metrics;

namespace Kuestenlogik.Surgewave.Transport;

/// <summary>
/// Metrics for peer-to-peer transport connections (Raft, replication, geo-replication).
/// Published as <c>surgewave.transport.peer.*</c> via <see cref="System.Diagnostics.Metrics.Meter"/>.
/// </summary>
public sealed class PeerTransportMetrics
{
    public static readonly PeerTransportMetrics Instance = new();

    private readonly Meter _meter = new("surgewave.transport.peer", "1.0.0");
    private readonly Counter<long> _connectionsOpened;
    private readonly Counter<long> _connectionsClosed;
    private readonly UpDownCounter<int> _activeConnections;
    private readonly Counter<long> _streamsOpened;
    private readonly Counter<long> _streamsClosed;
    private readonly Counter<long> _bytesSent;
    private readonly Counter<long> _bytesReceived;
    private readonly Counter<long> _errors;

    private PeerTransportMetrics()
    {
        _connectionsOpened = _meter.CreateCounter<long>(
            "surgewave.transport.peer.connections.opened",
            description: "Total peer connections opened");
        _connectionsClosed = _meter.CreateCounter<long>(
            "surgewave.transport.peer.connections.closed",
            description: "Total peer connections closed");
        _activeConnections = _meter.CreateUpDownCounter<int>(
            "surgewave.transport.peer.connections.active",
            description: "Currently active peer connections");
        _streamsOpened = _meter.CreateCounter<long>(
            "surgewave.transport.peer.streams.opened",
            description: "Total peer streams opened (per-RPC on QUIC, per-connection on TCP)");
        _streamsClosed = _meter.CreateCounter<long>(
            "surgewave.transport.peer.streams.closed",
            description: "Total peer streams closed");
        _bytesSent = _meter.CreateCounter<long>(
            "surgewave.transport.peer.bytes.sent",
            unit: "By",
            description: "Total bytes sent to peers");
        _bytesReceived = _meter.CreateCounter<long>(
            "surgewave.transport.peer.bytes.received",
            unit: "By",
            description: "Total bytes received from peers");
        _errors = _meter.CreateCounter<long>(
            "surgewave.transport.peer.errors",
            description: "Total peer transport errors");
    }

    public void RecordConnectionOpened(string transport) =>
        _connectionsOpened.Add(1, new KeyValuePair<string, object?>("transport", transport));

    public void RecordConnectionClosed(string transport) =>
        _connectionsClosed.Add(1, new KeyValuePair<string, object?>("transport", transport));

    public void IncrementActiveConnections(string transport) =>
        _activeConnections.Add(1, new KeyValuePair<string, object?>("transport", transport));

    public void DecrementActiveConnections(string transport) =>
        _activeConnections.Add(-1, new KeyValuePair<string, object?>("transport", transport));

    public void RecordStreamOpened(string transport) =>
        _streamsOpened.Add(1, new KeyValuePair<string, object?>("transport", transport));

    public void RecordStreamClosed(string transport) =>
        _streamsClosed.Add(1, new KeyValuePair<string, object?>("transport", transport));

    public void RecordBytesSent(long bytes, string transport) =>
        _bytesSent.Add(bytes, new KeyValuePair<string, object?>("transport", transport));

    public void RecordBytesReceived(long bytes, string transport) =>
        _bytesReceived.Add(bytes, new KeyValuePair<string, object?>("transport", transport));

    public void RecordError(string transport) =>
        _errors.Add(1, new KeyValuePair<string, object?>("transport", transport));
}
