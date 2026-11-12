using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;
using Kuestenlogik.Surgewave.Broker.Native;
using Kuestenlogik.Surgewave.Broker.Native.Handlers;
using Kuestenlogik.Surgewave.Broker.Native.Operations;
using Kuestenlogik.Surgewave.Broker.Tests.Fakes;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Transport;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// A native request is answered or its connection is closed (#281): whatever a handler throws, the client that
/// sent the request hears of it.
/// </summary>
/// <remarks>
/// Every test runs the broker's own connection loop over a loopback socket and asks it with the native client's
/// TCP transport, so "the client hears of it" is what a client gets. The bound on each wait is how long a test
/// may take to fail, not something a test waits out: before #281 the answer never came.
/// </remarks>
public sealed class NativeRequestAnswerTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    public static TheoryData<string> UnexpectedExceptions => new()
    {
        "io",
        "custom",
        "format",
        "cancelled-by-the-handler",
        "thrown-before-the-first-await",
    };

    private static Task Throw(string kind) => kind switch
    {
        "io" => Task.FromException(new IOException(SecretDetail)),
        "custom" => Task.FromException(new RefusedByTheTest(SecretDetail)),
        "format" => Task.FromException(new FormatException(SecretDetail)),
        // A handler's own timeout, not the broker stopping: the connection's token is not cancelled.
        "cancelled-by-the-handler" => Task.FromException(new OperationCanceledException(SecretDetail)),
        "thrown-before-the-first-await" => throw new RefusedByTheTest(SecretDetail),
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    private const string SecretDetail = "C:\\data\\broker-7\\segment-00000000000000000042.log is not for a client to read";

    [Theory]
    [MemberData(nameof(UnexpectedExceptions))]
    public async Task AHandlerThatThrowsIsAnsweredWithAnInternalErrorAndTheConnectionStaysUsable(string kind)
    {
        var log = new RecordingLogger();
        await using var broker = new NativeLoopbackBroker(
            [
                new DelegatingHandler(SurgewaveOpCode.CreateTopic, (_, _, _) => Throw(kind)),
                new DelegatingHandler(SurgewaveOpCode.Ping, (context, _, ct) => context.SendResponseAsync(
                    context.Header.RequestId, SurgewaveOpCode.Pong, SurgewaveErrorCode.None, ReadOnlyMemory<byte>.Empty, ct)),
            ],
            log);
        await using var client = await ConnectAsync(broker.Port);

        var (header, payload) = await client.SendRequestAsync(SurgewaveOpCode.CreateTopic, ReadOnlyMemory<byte>.Empty)
            .AsTask().WaitAsync(Bound, TestContext.Current.CancellationToken);

        Assert.Equal(SurgewaveOpCode.Error, header.OpCode);
        Assert.Equal(SurgewaveErrorCode.InternalError, header.ErrorCode);
        var message = ReadErrorMessage(payload);
        Assert.Contains("CreateTopic", message, StringComparison.Ordinal);
        Assert.DoesNotContain("segment-", message, StringComparison.Ordinal);
        Assert.DoesNotContain("   at ", message, StringComparison.Ordinal);

        // The exception is the operator's: logged once, as an error, with the request it belongs to.
        var entry = Assert.Single(log.Entries, e => e.Level == LogLevel.Error);
        Assert.NotNull(entry.Exception);
        Assert.Contains(SecretDetail, entry.Exception.Message, StringComparison.Ordinal);
        Assert.Contains("CreateTopic", entry.Message, StringComparison.Ordinal);
        Assert.Contains(header.RequestId.ToString(System.Globalization.CultureInfo.InvariantCulture), entry.Message, StringComparison.Ordinal);

        // The frame of the failed request was read completely, so the next one is read where it starts.
        var (next, _) = await client.SendRequestAsync(SurgewaveOpCode.Ping, ReadOnlyMemory<byte>.Empty)
            .AsTask().WaitAsync(Bound, TestContext.Current.CancellationToken);
        Assert.Equal(SurgewaveOpCode.Pong, next.OpCode);
        Assert.Equal(SurgewaveErrorCode.None, next.ErrorCode);
    }

    /// <summary>
    /// The three shapes <see cref="OperationExecutor"/> runs, each failing in the step a real operation fails in:
    /// reading the request, and carrying it out.
    /// </summary>
    [Theory]
    [InlineData(SurgewaveOpCode.DescribeTopic)]
    [InlineData(SurgewaveOpCode.CreateTopic)]
    [InlineData(SurgewaveOpCode.ListTopics)]
    [InlineData(SurgewaveOpCode.DeleteTopic)]
    public async Task AnOperationThatThrowsIsAnsweredWithAnInternalError(SurgewaveOpCode opCode)
    {
        await using var broker = new NativeLoopbackBroker([new ThrowingOperations()], new RecordingLogger());
        await using var client = await ConnectAsync(broker.Port);

        var (header, payload) = await client.SendRequestAsync(opCode, ReadOnlyMemory<byte>.Empty)
            .AsTask().WaitAsync(Bound, TestContext.Current.CancellationToken);

        Assert.Equal(SurgewaveOpCode.Error, header.OpCode);
        Assert.Equal(SurgewaveErrorCode.InternalError, header.ErrorCode);
        Assert.DoesNotContain("segment-", ReadErrorMessage(payload), StringComparison.Ordinal);
    }

    /// <summary>What an operation refuses on purpose keeps its own code and its own words.</summary>
    [Fact]
    public async Task ARefusalAnOperationStatesKeepsItsCodeAndItsMessage()
    {
        var log = new RecordingLogger();
        await using var broker = new NativeLoopbackBroker([new ThrowingOperations()], log);
        await using var client = await ConnectAsync(broker.Port);

        var (header, payload) = await client.SendRequestAsync(SurgewaveOpCode.DescribeConfig, ReadOnlyMemory<byte>.Empty)
            .AsTask().WaitAsync(Bound, TestContext.Current.CancellationToken);

        Assert.Equal(SurgewaveOpCode.Error, header.OpCode);
        Assert.Equal(SurgewaveErrorCode.TopicNotFound, header.ErrorCode);
        Assert.Equal("no such topic", ReadErrorMessage(payload));
        Assert.DoesNotContain(log.Entries, e => e.Level >= LogLevel.Error);
    }

    /// <summary>
    /// A broker that stops cancels what its handlers are doing. That is not answered: the connection is closed,
    /// and every request that was waiting on it fails for that reason — one close, not one error answer per request.
    /// </summary>
    [Fact]
    public async Task ABrokerThatStopsClosesTheConnectionInsteadOfAnsweringEachPendingRequest()
    {
        var log = new RecordingLogger();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var broker = new NativeLoopbackBroker(
            [
                new DelegatingHandler(SurgewaveOpCode.Fetch, async (_, _, ct) =>
                {
                    entered.TrySetResult();
                    await Task.Delay(Timeout.Infinite, ct);
                }),
            ],
            log);
        await using var client = await ConnectAsync(broker.Port);

        var pending = Enumerable.Range(0, 5)
            .Select(_ => client.SendRequestAsync(SurgewaveOpCode.Fetch, ReadOnlyMemory<byte>.Empty).AsTask())
            .ToArray();
        await entered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);

        await broker.StopAsync();

        foreach (var request in pending)
        {
            await Assert.ThrowsAnyAsync<IOException>(
                () => request.WaitAsync(Bound, TestContext.Current.CancellationToken));
        }

        Assert.DoesNotContain(log.Entries, e => e.Level >= LogLevel.Error);
    }

    /// <summary>
    /// A handler that fails after it has answered gets a second, error answer for the same request. Responses are
    /// whole frames, so the client reads it, finds nobody waiting for it and drops it: the connection goes on.
    /// </summary>
    [Fact]
    public async Task AHandlerThatThrowsAfterItAnsweredDoesNotBreakTheConnection()
    {
        await using var broker = new NativeLoopbackBroker(
            [
                new DelegatingHandler(SurgewaveOpCode.Fetch, async (context, _, ct) =>
                {
                    await context.SendResponseAsync(context.Header.RequestId, SurgewaveOpCode.FetchResponse,
                        SurgewaveErrorCode.None, new byte[16], ct);
                    throw new RefusedByTheTest(SecretDetail);
                }),
            ],
            new RecordingLogger());
        await using var client = await ConnectAsync(broker.Port);

        var (first, _) = await client.SendRequestAsync(SurgewaveOpCode.Fetch, ReadOnlyMemory<byte>.Empty)
            .AsTask().WaitAsync(Bound, TestContext.Current.CancellationToken);
        Assert.Equal(SurgewaveOpCode.FetchResponse, first.OpCode);

        var (second, _) = await client.SendRequestAsync(SurgewaveOpCode.Fetch, ReadOnlyMemory<byte>.Empty)
            .AsTask().WaitAsync(Bound, TestContext.Current.CancellationToken);
        Assert.Equal(SurgewaveOpCode.FetchResponse, second.OpCode);
    }

    /// <summary>
    /// An answer that cannot be written ends the connection: the handler returns to its caller, which closes the
    /// socket. The requests are read on a second task, and it used to go on waiting for the client's next one.
    /// </summary>
    [Fact]
    public async Task AnAnswerThatCannotBeWrittenEndsTheConnection()
    {
        var handler = new SurgewaveNativeHandler(
            new NativeRequestDispatcher([new DelegatingHandler(SurgewaveOpCode.CreateTopic, (_, _, _) => Throw("custom"))]),
            new BrokerConfig(),
            new RecordingLogger());

        // The version byte and one request; after them the client sends nothing, as one that waits for its answer.
        var request = new byte[1 + SurgewaveNativeProtocol.HeaderSize];
        request[0] = SurgewaveNativeProtocol.Version;
        new SurgewaveRequestHeader { RequestId = 7, OpCode = SurgewaveOpCode.CreateTopic, PayloadLength = 0 }.WriteTo(request.AsSpan(1));
        await using var stream = new ReadsThenWaitsAndFailsItsSecondWrite(request);

        var connection = handler.HandleConnectionAsync(stream, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<IOException>(() => connection.WaitAsync(Bound, TestContext.Current.CancellationToken));
    }

    /// <summary>
    /// A client that goes while its request is being carried out is not a request that failed: the handler's write
    /// of the answer fails, the connection ends, and no error is logged for the request.
    /// </summary>
    [Fact]
    public async Task AClientThatWentBeforeItsAnswerIsNotLoggedAsAFailedRequest()
    {
        var log = new RecordingLogger();
        var handler = new SurgewaveNativeHandler(
            new NativeRequestDispatcher([new DelegatingHandler(SurgewaveOpCode.Fetch, (context, _, ct) => context.SendResponseAsync(
                context.Header.RequestId, SurgewaveOpCode.FetchResponse, SurgewaveErrorCode.None, new byte[16], ct))]),
            new BrokerConfig(),
            log);

        var request = new byte[1 + SurgewaveNativeProtocol.HeaderSize];
        request[0] = SurgewaveNativeProtocol.Version;
        new SurgewaveRequestHeader { RequestId = 7, OpCode = SurgewaveOpCode.Fetch, PayloadLength = 0 }.WriteTo(request.AsSpan(1));
        await using var stream = new ReadsThenWaitsAndFailsItsSecondWrite(request);

        var connection = handler.HandleConnectionAsync(stream, TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<IOException>(() => connection.WaitAsync(Bound, TestContext.Current.CancellationToken));
        Assert.DoesNotContain(log.Entries, e => e.Level >= LogLevel.Error);
    }

    /// <summary>
    /// The handlers log under their own categories through the host's factory: the data handler's line that it
    /// creates a topic was written to a NullLogger and never appeared.
    /// </summary>
    [Fact]
    public async Task TheDataHandlerLogsThroughTheHostsLoggerFactory()
    {
        var logs = new RecordingLoggerFactory();
        await using var runtime = await Kuestenlogik.Surgewave.Runtime.SurgewaveRuntime.CreateBuilder()
            .WithPort(0)
            .WithStorageEngine(Kuestenlogik.Surgewave.Core.Storage.StorageEngines.Memory)
            .WithAutoCreateTopics()
            .WithLogging(logs)
            .Build()
            .StartAsync(TestContext.Current.CancellationToken);
        await using var client = await ConnectAsync(runtime.Port);

        // ListOffsets for a topic that is not there: topic, partition, timestamp (-1, the end).
        var topic = $"created-on-first-use-{Guid.NewGuid():N}";
        var payload = new byte[2 + topic.Length + 4 + 8];
        BinaryPrimitives.WriteInt16BigEndian(payload, (short)topic.Length);
        Encoding.ASCII.GetBytes(topic, payload.AsSpan(2));
        BinaryPrimitives.WriteInt64BigEndian(payload.AsSpan(2 + topic.Length + 4), -1);
        var (header, _) = await client.SendRequestAsync(SurgewaveOpCode.ListOffsets, payload)
            .AsTask().WaitAsync(Bound, TestContext.Current.CancellationToken);
        Assert.Equal(SurgewaveErrorCode.None, header.ErrorCode);

        Assert.Contains(logs.Entries, e =>
            e.Category == typeof(NativeDataHandler).FullName && e.Message.Contains(topic, StringComparison.Ordinal));
    }

    private static async Task<TcpTransport> ConnectAsync(int port)
    {
        var transport = new TcpTransport(new TransportOptions { Host = "127.0.0.1", Port = port, EnableCompression = false });
        try
        {
            await transport.ConnectAsync(TestContext.Current.CancellationToken).AsTask()
                .WaitAsync(Bound, TestContext.Current.CancellationToken);
            return transport;
        }
        catch
        {
            await transport.DisposeAsync();
            throw;
        }
    }

    private static string ReadErrorMessage(ReadOnlyMemory<byte> payload)
    {
        var length = BinaryPrimitives.ReadInt16BigEndian(payload.Span);
        return Encoding.UTF8.GetString(payload.Span.Slice(2, length));
    }

    private sealed class RefusedByTheTest(string message) : Exception(message);

    private sealed class DelegatingHandler(
        SurgewaveOpCode opCode,
        Func<NativeRequestContext, ReadOnlyMemory<byte>, CancellationToken, Task> handle) : INativeRequestHandler
    {
        public IEnumerable<SurgewaveOpCode> SupportedOpCodes => [opCode];

        public Task HandleAsync(NativeRequestContext context, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
            => handle(context, payload, cancellationToken);
    }

    /// <summary>Operations of each shape the executor knows, failing where the test's name says.</summary>
    private sealed class ThrowingOperations : NativeHandlerBase
    {
        public ThrowingOperations()
        {
            Register<Empty, Empty>(SurgewaveOpCode.DescribeTopic, _ => new Operation(SurgewaveOpCode.DescribeTopic, failsReading: true));
            RegisterVoid<Empty>(SurgewaveOpCode.CreateTopic, _ => new Operation(SurgewaveOpCode.CreateTopic, failsReading: false));
            RegisterNoRequest<Empty>(SurgewaveOpCode.ListTopics, _ => new Operation(SurgewaveOpCode.ListTopics, failsReading: false));
            RegisterVoid<Empty>(SurgewaveOpCode.DeleteTopic, _ => new Operation(SurgewaveOpCode.DeleteTopic, failsReading: true));
            Register<Empty, Empty>(SurgewaveOpCode.DescribeConfig, _ => new Operation(SurgewaveOpCode.DescribeConfig, failsReading: false, refuses: true));
        }

        private readonly struct Empty;

        private sealed class Operation(SurgewaveOpCode opCode, bool failsReading, bool refuses = false) :
            IOperationHandler<Empty, Empty>, IVoidOperationHandler<Empty>, INoRequestOperationHandler<Empty>
        {
            public SurgewaveOpCode OpCode => opCode;

            public Empty ParseRequest(ref SurgewavePayloadReader reader)
                => failsReading ? throw new ArgumentOutOfRangeException(nameof(reader), SecretDetail) : default;

            public void ValidateRequest(in Empty request)
            {
            }

            public Task<Empty> ExecuteAsync(Empty request, CancellationToken cancellationToken) => Fail<Empty>();

            Task IVoidOperationHandler<Empty>.ExecuteAsync(Empty request, CancellationToken cancellationToken) => Fail<Empty>();

            public Task<Empty> ExecuteAsync(CancellationToken cancellationToken) => Fail<Empty>();

            public void WriteResponse(IPayloadWriter writer, in Empty response)
            {
            }

            private Task<T> Fail<T>() => refuses
                ? Task.FromException<T>(new SurgewaveOperationException(SurgewaveErrorCode.TopicNotFound, "no such topic"))
                : Task.FromException<T>(new RefusedByTheTest(SecretDetail));
        }
    }

    /// <summary>
    /// A connection whose client has sent what it has to say and waits: reads give out the bytes they were given
    /// and then wait until they are cancelled; the first write (the handshake's answer) succeeds, the second fails.
    /// </summary>
    private sealed class ReadsThenWaitsAndFailsItsSecondWrite(byte[] incoming) : Stream
    {
        private int _read;
        private int _writes;

        public override bool CanRead => true;
        public override bool CanWrite => true;
        public override bool CanSeek => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (_read == incoming.Length)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }

            var count = Math.Min(buffer.Length, incoming.Length - _read);
            incoming.AsMemory(_read, count).CopyTo(buffer);
            _read += count;
            return count;
        }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => ++_writes == 1
                ? ValueTask.CompletedTask
                : ValueTask.FromException(new IOException("the client does not take the answer"));

        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public override void Flush() { }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
    }

    private sealed class RecordingLoggerFactory : ILoggerFactory
    {
        public ConcurrentQueue<(string Category, LogLevel Level, string Message)> Entries { get; } = new();

        public void AddProvider(ILoggerProvider provider) { }

        public ILogger CreateLogger(string categoryName) => new CategoryLogger(categoryName, Entries);

        public void Dispose() { }

        private sealed class CategoryLogger(string category, ConcurrentQueue<(string Category, LogLevel Level, string Message)> entries) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
                => entries.Enqueue((category, logLevel, formatter(state, exception)));
        }
    }

    private sealed class RecordingLogger : ILogger<SurgewaveNativeHandler>
    {
        public ConcurrentQueue<(LogLevel Level, string Message, Exception? Exception)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Entries.Enqueue((logLevel, formatter(state, exception), exception));
    }
}
