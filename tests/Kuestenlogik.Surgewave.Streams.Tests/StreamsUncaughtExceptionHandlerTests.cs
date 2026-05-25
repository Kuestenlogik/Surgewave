using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.ExceptionHandling;
using Kuestenlogik.Surgewave.Streams.Runtime;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class StreamsUncaughtExceptionHandlerTests
{
    [Fact]
    public void DefaultHandler_ReturnsReplaceThread()
    {
        var handler = LogAndReplaceUncaughtExceptionHandler.Instance;
        var response = handler.Handle("test-thread-0", new InvalidOperationException("test"));
        Assert.Equal(StreamsUncaughtExceptionResponse.ReplaceThread, response);
    }

    [Fact]
    public void ShutdownClientHandler_ReturnsShutdownClient()
    {
        var handler = LogAndShutdownClientHandler.Instance;
        var response = handler.Handle("test-thread-0", new InvalidOperationException("test"));
        Assert.Equal(StreamsUncaughtExceptionResponse.ShutdownClient, response);
    }

    [Fact]
    public void ShutdownApplicationHandler_ReturnsShutdownApplication()
    {
        var handler = LogAndShutdownApplicationHandler.Instance;
        var response = handler.Handle("test-thread-0", new InvalidOperationException("test"));
        Assert.Equal(StreamsUncaughtExceptionResponse.ShutdownApplication, response);
    }

    [Fact]
    public void StreamsConfig_DefaultHandler_IsReplaceThread()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "test",
            BootstrapServers = "dummy:9092"
        };

        Assert.IsType<LogAndReplaceUncaughtExceptionHandler>(config.UncaughtExceptionHandler);
    }

    [Fact]
    public void StreamsConfig_CustomHandler_CanBeConfigured()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "test",
            BootstrapServers = "dummy:9092",
            UncaughtExceptionHandler = LogAndShutdownClientHandler.Instance
        };

        Assert.IsType<LogAndShutdownClientHandler>(config.UncaughtExceptionHandler);
    }

    [Fact]
    public void CustomHandler_LambdaBased_Works()
    {
        var exceptions = new List<(string thread, Exception ex)>();
        var handler = new DelegateUncaughtExceptionHandler((thread, ex) =>
        {
            exceptions.Add((thread, ex));
            return StreamsUncaughtExceptionResponse.ShutdownClient;
        });

        var testEx = new TimeoutException("connection lost");
        var response = handler.Handle("stream-thread-1", testEx);

        Assert.Equal(StreamsUncaughtExceptionResponse.ShutdownClient, response);
        Assert.Single(exceptions);
        Assert.Equal("stream-thread-1", exceptions[0].thread);
        Assert.Equal("connection lost", exceptions[0].ex.Message);
    }

    [Fact]
    public void ConditionalHandler_RetryableVsFatal()
    {
        var handler = new DelegateUncaughtExceptionHandler((_, ex) =>
            ex is TimeoutException or System.IO.IOException
                ? StreamsUncaughtExceptionResponse.ReplaceThread
                : StreamsUncaughtExceptionResponse.ShutdownClient);

        Assert.Equal(StreamsUncaughtExceptionResponse.ReplaceThread,
            handler.Handle("t-0", new TimeoutException()));
        Assert.Equal(StreamsUncaughtExceptionResponse.ReplaceThread,
            handler.Handle("t-0", new System.IO.IOException()));
        Assert.Equal(StreamsUncaughtExceptionResponse.ShutdownClient,
            handler.Handle("t-0", new InvalidOperationException()));
    }

    [Fact]
    public void StreamsApplication_WithCustomHandler_Builds()
    {
        var builder = new StreamsBuilder();
        builder.Stream<string, int>("input").ForEach((k, v) => { });
        var topology = builder.Build();

        var config = new StreamsConfig
        {
            ApplicationId = "test",
            BootstrapServers = "dummy:9092",
            UncaughtExceptionHandler = new DelegateUncaughtExceptionHandler((_, _) =>
                StreamsUncaughtExceptionResponse.ShutdownClient)
        };

        var app = new StreamsApplication(config, topology, NullLoggerFactory.Instance);
        Assert.NotNull(app);
    }

    [Fact]
    public void StreamsMetrics_UncaughtExceptionCounter()
    {
        var metrics = new StreamsMetrics();
        Assert.Equal(0, metrics.UncaughtExceptions);

        metrics.RecordUncaughtException();
        metrics.RecordUncaughtException();
        Assert.Equal(2, metrics.UncaughtExceptions);
    }

    [Fact]
    public void StreamsMetrics_ThreadReplacementCounter()
    {
        var metrics = new StreamsMetrics();
        Assert.Equal(0, metrics.ThreadReplacements);

        metrics.RecordThreadReplacement();
        Assert.Equal(1, metrics.ThreadReplacements);
    }
}

/// <summary>
/// Lambda-based handler for testing and simple use cases.
/// </summary>
public sealed class DelegateUncaughtExceptionHandler : IStreamsUncaughtExceptionHandler
{
    private readonly Func<string, Exception, StreamsUncaughtExceptionResponse> _handler;

    public DelegateUncaughtExceptionHandler(Func<string, Exception, StreamsUncaughtExceptionResponse> handler)
    {
        _handler = handler;
    }

    public StreamsUncaughtExceptionResponse Handle(string threadName, Exception exception)
        => _handler(threadName, exception);
}
