using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Connect;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Coverage-push batch — Kafka-Connect-shaped payloads in
/// Protocol.Native (<c>ListConnectors</c>, <c>ConnectorConfig</c>,
/// <c>ConnectorStatus</c>, <c>ConnectorInfo</c>). These mirror the
/// admin RPCs that <c>surgewave connect ...</c> + the Control UI use
/// to display worker / task state, so framing regressions show up as
/// "empty connector list" in the field — cheap to catch here.
///
/// <see cref="ConnectorTaskStatusPayload"/> has no standalone
/// Read/Write — its fields are inlined by the containing Status /
/// Info payloads — so its coverage flows through them.
/// </summary>
public sealed class ConnectPayloadRoundTripTests
{
    private static T RoundTrip<T>(int sizeEstimate, Action<byte[]> write, Func<byte[], T> read)
    {
        var buffer = new byte[sizeEstimate + 16];
        write(buffer);
        return read(buffer);
    }

    // ───────────────────────────────────────────────────────────────
    // ListConnectorsPayload
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void ListConnectorsPayload_RoundTrip_PreservesOrdering()
    {
        var original = new ListConnectorsPayload
        {
            Connectors = new[] { "s3-sink", "jdbc-source", "elastic-sink" },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ListConnectorsPayload.Read(ref r); });

        Assert.Equal(new[] { "s3-sink", "jdbc-source", "elastic-sink" }, parsed.Connectors);
    }

    [Fact]
    public void ListConnectorsPayload_EmptyList_RoundTrips()
    {
        var original = new ListConnectorsPayload { Connectors = Array.Empty<string>() };
        var parsed = RoundTrip(
            16,
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ListConnectorsPayload.Read(ref r); });
        Assert.Empty(parsed.Connectors);
    }

    // ───────────────────────────────────────────────────────────────
    // ConnectorConfigPayload
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void ConnectorConfigPayload_RoundTrip_PreservesEveryKeyValuePair()
    {
        var original = new ConnectorConfigPayload
        {
            Config = new Dictionary<string, string>
            {
                ["connector.class"] = "io.confluent.connect.s3.S3SinkConnector",
                ["tasks.max"] = "4",
                ["topics"] = "events,audit,metrics",
                ["s3.bucket.name"] = "kl-prod-events",
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConnectorConfigPayload.Read(ref r); });

        Assert.Equal(4, parsed.Config.Count);
        Assert.Equal("io.confluent.connect.s3.S3SinkConnector", parsed.Config["connector.class"]);
        Assert.Equal("4", parsed.Config["tasks.max"]);
        Assert.Equal("events,audit,metrics", parsed.Config["topics"]);
        Assert.Equal("kl-prod-events", parsed.Config["s3.bucket.name"]);
    }

    [Fact]
    public void ConnectorConfigPayload_EmptyConfig_RoundTrips()
    {
        var original = new ConnectorConfigPayload { Config = new Dictionary<string, string>() };
        var parsed = RoundTrip(
            16,
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConnectorConfigPayload.Read(ref r); });
        Assert.Empty(parsed.Config);
    }

    // ───────────────────────────────────────────────────────────────
    // ConnectorStatusPayload (carries task statuses WITH trace strings)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void ConnectorStatusPayload_RoundTrip_PreservesTasksAndTraces()
    {
        var original = new ConnectorStatusPayload
        {
            Name = "s3-sink",
            Type = "sink",
            State = "RUNNING",
            WorkerId = "worker-1",
            Tasks = new[]
            {
                new ConnectorTaskStatusPayload { Id = 0, State = "RUNNING", WorkerId = "worker-1", Trace = null },
                new ConnectorTaskStatusPayload { Id = 1, State = "FAILED",  WorkerId = "worker-2", Trace = "java.io.IOException: connection closed" },
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConnectorStatusPayload.Read(ref r); });

        Assert.Equal("s3-sink", parsed.Name);
        Assert.Equal("sink", parsed.Type);
        Assert.Equal("RUNNING", parsed.State);
        Assert.Equal("worker-1", parsed.WorkerId);
        Assert.Equal(2, parsed.Tasks.Count);

        // Task 0: running, no trace
        Assert.Equal(0, parsed.Tasks[0].Id);
        Assert.Equal("RUNNING", parsed.Tasks[0].State);
        Assert.Null(parsed.Tasks[0].Trace);

        // Task 1: failed, with trace
        Assert.Equal(1, parsed.Tasks[1].Id);
        Assert.Equal("FAILED", parsed.Tasks[1].State);
        Assert.Equal("worker-2", parsed.Tasks[1].WorkerId);
        Assert.Equal("java.io.IOException: connection closed", parsed.Tasks[1].Trace);
    }

    [Fact]
    public void ConnectorStatusPayload_NoTasks_RoundTrips()
    {
        var original = new ConnectorStatusPayload
        {
            Name = "unassigned",
            Type = "source",
            State = "UNASSIGNED",
            WorkerId = "",
            Tasks = Array.Empty<ConnectorTaskStatusPayload>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConnectorStatusPayload.Read(ref r); });

        Assert.Equal("UNASSIGNED", parsed.State);
        Assert.Empty(parsed.Tasks);
    }

    // ───────────────────────────────────────────────────────────────
    // ConnectorInfoPayload (config + tasks; Info-side strips trace)
    // ───────────────────────────────────────────────────────────────

    [Fact]
    public void ConnectorInfoPayload_RoundTrip_PreservesConfigAndTasks()
    {
        var original = new ConnectorInfoPayload
        {
            Name = "jdbc-source",
            Type = "source",
            State = "RUNNING",
            WorkerId = "worker-3",
            Config = new Dictionary<string, string>
            {
                ["connection.url"] = "jdbc:postgresql://db/orders",
                ["table.whitelist"] = "orders,line_items",
            },
            Tasks = new[]
            {
                new ConnectorTaskStatusPayload { Id = 0, State = "RUNNING", WorkerId = "worker-3", Trace = null },
                new ConnectorTaskStatusPayload { Id = 1, State = "RUNNING", WorkerId = "worker-4", Trace = null },
            },
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConnectorInfoPayload.Read(ref r); });

        Assert.Equal("jdbc-source", parsed.Name);
        Assert.Equal("source", parsed.Type);
        Assert.Equal(2, parsed.Config.Count);
        Assert.Equal("jdbc:postgresql://db/orders", parsed.Config["connection.url"]);

        Assert.Equal(2, parsed.Tasks.Count);
        Assert.Equal(0, parsed.Tasks[0].Id);
        Assert.Equal("worker-4", parsed.Tasks[1].WorkerId);
        // ConnectorInfo strips trace at Read time (see the source — it
        // sets Trace = null unconditionally). Pin that — distinguishes
        // Info from Status on the same wire format.
        Assert.Null(parsed.Tasks[0].Trace);
        Assert.Null(parsed.Tasks[1].Trace);
    }

    [Fact]
    public void ConnectorInfoPayload_EmptyConfigAndTasks_RoundTrips()
    {
        var original = new ConnectorInfoPayload
        {
            Name = "draft-connector",
            Type = "unknown",
            State = "UNKNOWN",
            WorkerId = "",
            Config = new Dictionary<string, string>(),
            Tasks = Array.Empty<ConnectorTaskStatusPayload>(),
        };
        var parsed = RoundTrip(
            original.EstimateSize(),
            buf => { var w = new SurgewavePayloadWriter(buf); original.Write(ref w); },
            buf => { var r = new SurgewavePayloadReader(buf); return ConnectorInfoPayload.Read(ref r); });

        Assert.Equal("UNKNOWN", parsed.State);
        Assert.Empty(parsed.Config);
        Assert.Empty(parsed.Tasks);
    }
}
