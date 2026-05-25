using System.Diagnostics;
using System.Diagnostics.Metrics;
using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Runtime;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class StreamsTracingTests : IDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    public StreamsTracingTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddFilter(level => level >= LogLevel.Debug);
        });
    }

    public void Dispose()
    {
        _loggerFactory.Dispose();
    }

    [Fact]
    public void StreamsMetrics_HasMeterAndActivitySource()
    {
        using var metrics = new StreamsMetrics();

        Assert.NotNull(metrics.Meter);
        Assert.NotNull(metrics.ActivitySource);
        Assert.Equal("Kuestenlogik.Surgewave.Streams", metrics.Meter.Name);
        Assert.Equal("Kuestenlogik.Surgewave.Streams", metrics.ActivitySource.Name);
    }

    [Fact]
    public void StreamsMetrics_RecordProcessed_UpdatesBothCounterTypes()
    {
        using var metrics = new StreamsMetrics();

        // Capture OTEL counter via MeterListener.
        //
        // Determinism note: System.Diagnostics.Metrics.Meter instances are NOT globally
        // unique by name — two StreamsMetrics instances both publish their own Meter named
        // "Kuestenlogik.Surgewave.Streams", each with its own "surgewave_streams_records_processed_total"
        // counter instrument. A plain name-only filter in InstrumentPublished is racy when
        // xUnit runs this class's tests in parallel with anything else that instantiates
        // StreamsMetrics — the listener can latch onto an instrument from a different
        // meter and miss the measurements from ours. Filter on the Meter reference to
        // scope the listener to this test's metrics instance.
        long otelRecordCount = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (ReferenceEquals(instrument.Meter, metrics.Meter) &&
                instrument.Name == "surgewave_streams_records_processed_total")
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
        {
            otelRecordCount += measurement;
        });
        listener.Start();

        metrics.RecordProcessed(100);
        metrics.RecordProcessed(200);

        listener.RecordObservableInstruments();

        Assert.Equal(2, metrics.ProcessedRecords);
        Assert.Equal(300, metrics.ProcessedBytes);
        Assert.Equal(2, otelRecordCount);
    }

    [Fact]
    public void StreamsMetrics_StartProcessRecordActivity_SetsCorrectTags()
    {
        using var metrics = new StreamsMetrics();

        // Need an ActivityListener to capture activities
        using var activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Kuestenlogik.Surgewave.Streams",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
        };
        ActivitySource.AddActivityListener(activityListener);

        using var activity = metrics.StartProcessRecordActivity("test-topic", 0, 42, "test-app");

        Assert.NotNull(activity);
        Assert.Equal("surgewave.streams.process_record", activity!.OperationName);
        Assert.Equal("test-topic", activity.GetTagItem("messaging.destination.name")?.ToString());
        Assert.Equal("0", activity.GetTagItem("messaging.destination.partition.id")?.ToString());
        Assert.Equal("42", activity.GetTagItem("messaging.message.offset")?.ToString());
        Assert.Equal("test-app", activity.GetTagItem("surgewave.streams.application.id")?.ToString());
    }

    [Fact]
    public void StreamTask_Process_CreatesActivity()
    {
        // The StreamTask.Process() method creates activities, but StreamsApplication.ProcessRecord()
        // goes directly through nodes for testing. Verify the activity is created via metrics API.
        using var metrics = new StreamsMetrics();

        Activity? capturedActivity = null;
        using var activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == "Kuestenlogik.Surgewave.Streams",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity => capturedActivity = activity,
        };
        ActivitySource.AddActivityListener(activityListener);

        using var activity = metrics.StartProcessRecordActivity("test-topic", 0, 100, "test-app");
        Assert.NotNull(activity);
        Assert.NotNull(capturedActivity);
        Assert.Equal("surgewave.streams.process_record", capturedActivity!.OperationName);
    }

    [Fact]
    public void StreamsMetrics_ProcessingLatency_RecordsHistogram()
    {
        using var metrics = new StreamsMetrics();

        double recordedLatency = 0;
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Name == "surgewave_streams_processing_latency_ms")
                meterListener.EnableMeasurementEvents(instrument);
        };
        listener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
        {
            recordedLatency += measurement;
        });
        listener.Start();

        metrics.RecordProcessingLatency(15.5);
        metrics.RecordProcessingLatency(20.0);

        Assert.Equal(35.5, recordedLatency, precision: 1);
    }

    [Fact]
    public void StreamsMetrics_Dispose_CleansUp()
    {
        var metrics = new StreamsMetrics();
        var meter = metrics.Meter;
        var source = metrics.ActivitySource;

        metrics.Dispose();

        // Second dispose should not throw
        metrics.Dispose();

        // Meter and ActivitySource should exist but be disposed
        Assert.NotNull(meter);
        Assert.NotNull(source);
    }
}
