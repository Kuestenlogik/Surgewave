using System.Diagnostics.Metrics;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

public class PeerTransportMetricsTests
{
    [Fact]
    public void RecordConnectionOpened_IncrementsCounter()
    {
        long observed = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (inst, l) =>
        {
            if (inst.Name == "surgewave.transport.peer.connections.opened") l.EnableMeasurementEvents(inst);
        };
        listener.SetMeasurementEventCallback<long>((inst, value, tags, state) => observed += value);
        listener.Start();

        PeerTransportMetrics.Instance.RecordConnectionOpened("test");

        listener.RecordObservableInstruments();
        Assert.True(observed >= 1);
    }

    [Fact]
    public void ActiveConnections_IncrementAndDecrement()
    {
        int netActive = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (inst, l) =>
        {
            if (inst.Name == "surgewave.transport.peer.connections.active") l.EnableMeasurementEvents(inst);
        };
        listener.SetMeasurementEventCallback<int>((inst, value, tags, state) => netActive += value);
        listener.Start();

        PeerTransportMetrics.Instance.IncrementActiveConnections("test");
        PeerTransportMetrics.Instance.IncrementActiveConnections("test");
        PeerTransportMetrics.Instance.DecrementActiveConnections("test");

        Assert.Equal(1, netActive);
    }

    [Fact]
    public void RecordBytesSent_AccumulatesCorrectly()
    {
        long totalBytes = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (inst, l) =>
        {
            if (inst.Name == "surgewave.transport.peer.bytes.sent") l.EnableMeasurementEvents(inst);
        };
        listener.SetMeasurementEventCallback<long>((inst, value, tags, state) => totalBytes += value);
        listener.Start();

        PeerTransportMetrics.Instance.RecordBytesSent(1024, "tcp");
        PeerTransportMetrics.Instance.RecordBytesSent(2048, "quic");

        Assert.True(totalBytes >= 3072);
    }

    [Fact]
    public void TransportTag_IsPresent()
    {
        string? observedTransport = null;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (inst, l) =>
        {
            if (inst.Name == "surgewave.transport.peer.errors") l.EnableMeasurementEvents(inst);
        };
        listener.SetMeasurementEventCallback<long>((inst, value, tags, state) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "transport") observedTransport = tag.Value?.ToString();
            }
        });
        listener.Start();

        PeerTransportMetrics.Instance.RecordError("quic");

        Assert.Equal("quic", observedTransport);
    }
}
