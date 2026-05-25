using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Processors;
using Kuestenlogik.Surgewave.Streams.Runtime;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class AsyncLifecycleTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;

    public AsyncLifecycleTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(b => b.AddFilter(l => l >= LogLevel.Debug));
        _logger = _loggerFactory.CreateLogger<AsyncLifecycleTests>();
    }

    public async ValueTask DisposeAsync()
    {
        _loggerFactory.Dispose();
        await Task.CompletedTask;
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Test 1: IAsyncProcessorLifecycle.OnCloseAsync is called during shutdown
    // ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ShutdownAsync_CallsOnCloseAsync_OnAsyncLifecycleNode()
    {
        var closeCalled = false;
        var config = MakeConfig("async-lifecycle-close");
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        var node = new AsyncLifecycleNode("ASYNC-NODE-1",
            onInit: _ => Task.CompletedTask,
            onClose: _ => { closeCalled = true; return Task.CompletedTask; });

        node.Init(context);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);
        await orchestrator.ShutdownAsync([node], context);

        Assert.True(closeCalled, "OnCloseAsync should have been called");
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Test 2: Shutdown phases fire in correct order
    // ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ShutdownAsync_PhasesFireInCorrectOrder()
    {
        var phases = new List<ShutdownPhase>();
        var config = MakeConfig("async-phases");
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);
        orchestrator.PhaseChanged += p => phases.Add(p);

        await orchestrator.ShutdownAsync([], context);

        var expected = new[]
        {
            ShutdownPhase.Starting,
            ShutdownPhase.TasksSuspending,
            ShutdownPhase.StoresFlushing,
            ShutdownPhase.NodesStopping,
            ShutdownPhase.Completed
        };

        Assert.Equal(expected, phases);
    }

    [Fact]
    public void Shutdown_PhasesFireInCorrectOrder_Sync()
    {
        var phases = new List<ShutdownPhase>();
        var config = MakeConfig("sync-phases");
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);
        orchestrator.PhaseChanged += p => phases.Add(p);

        orchestrator.Shutdown([], context);

        var expected = new[]
        {
            ShutdownPhase.Starting,
            ShutdownPhase.TasksSuspending,
            ShutdownPhase.StoresFlushing,
            ShutdownPhase.NodesStopping,
            ShutdownPhase.Completed
        };

        Assert.Equal(expected, phases);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Test 3: Mixed sync/async lifecycle processors
    // ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ShutdownAsync_MixedSyncAndAsyncLifecycle_BothClosed()
    {
        var syncClosed = false;
        var asyncClosed = false;

        var config = MakeConfig("mixed-lifecycle");
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        // Sync lifecycle node
        var syncNode = new SyncLifecycleNode("SYNC-NODE",
            onClose: _ => syncClosed = true);
        syncNode.Init(context);

        // Async lifecycle node — child of syncNode so topology is linear
        var asyncNode = new AsyncLifecycleNode("ASYNC-NODE",
            onInit: _ => Task.CompletedTask,
            onClose: _ => { asyncClosed = true; return Task.CompletedTask; });
        asyncNode.Init(context);
        syncNode.AddChild(asyncNode);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);
        await orchestrator.ShutdownAsync([syncNode], context);

        Assert.True(syncClosed, "Sync OnClose should have been called");
        Assert.True(asyncClosed, "Async OnCloseAsync should have been called");
    }

    [Fact]
    public async Task ShutdownAsync_AsyncNodeTakesPrecedenceOverSync_WhenBothImplemented()
    {
        var asyncClosed = false;
        var syncClosed = false;
        var config = MakeConfig("precedence");
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        // Node that implements both — async should win
        var node = new BothLifecycleNode("BOTH-NODE",
            onAsyncClose: _ => { asyncClosed = true; return Task.CompletedTask; },
            onSyncClose: _ => syncClosed = true);
        node.Init(context);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);
        await orchestrator.ShutdownAsync([node], context);

        Assert.True(asyncClosed, "Async OnCloseAsync should have been called");
        Assert.False(syncClosed, "Sync OnClose should NOT be called when async is present");
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Test 4: Shutdown timeout
    // ──────────────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ShutdownAsync_HangingNode_DoesNotBlockPastTimeout()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "async-timeout",
            BootstrapServers = "localhost:9092",
            ShutdownTimeout = TimeSpan.FromMilliseconds(200)
        };
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        // Node whose OnCloseAsync hangs forever
        var hangingNode = new AsyncLifecycleNode("HANGING-NODE",
            onInit: _ => Task.CompletedTask,
            onClose: async ctx =>
            {
                await Task.Delay(Timeout.Infinite, ctx.ShutdownToken)
                    .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            });
        hangingNode.Init(context);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await orchestrator.ShutdownAsync([hangingNode], context);
        sw.Stop();

        // Should complete well within a generous upper bound (2 s),
        // confirming the per-node timeout kicked in rather than hanging forever.
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(2),
            $"Shutdown took too long: {sw.Elapsed}");
    }

    [Fact]
    public async Task ShutdownAsync_OnInitAsync_CalledDuringInitializeLifecycles()
    {
        var initCalled = false;
        var config = MakeConfig("async-init");
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        var node = new AsyncLifecycleNode("ASYNC-INIT-NODE",
            onInit: _ => { initCalled = true; return Task.CompletedTask; },
            onClose: _ => Task.CompletedTask);
        node.Init(context);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);
        orchestrator.InitializeLifecycles([node], context);

        Assert.True(initCalled, "OnInitAsync should have been called via InitializeLifecycles");

        // Clean up
        await orchestrator.ShutdownAsync([node], context);
    }

    // ──────────────────────────────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────────────────────────────

    private static StreamsConfig MakeConfig(string appId) => new()
    {
        ApplicationId = appId,
        BootstrapServers = "localhost:9092",
        ShutdownTimeout = TimeSpan.FromSeconds(5)
    };

    /// <summary>Minimal node implementing IAsyncProcessorLifecycle.</summary>
    private sealed class AsyncLifecycleNode : ProcessorNode, IAsyncProcessorLifecycle
    {
        private readonly Func<ProcessorLifecycleContext, Task> _onInit;
        private readonly Func<ProcessorLifecycleContext, Task> _onClose;

        public AsyncLifecycleNode(string name,
            Func<ProcessorLifecycleContext, Task> onInit,
            Func<ProcessorLifecycleContext, Task> onClose)
            : base(name)
        {
            _onInit = onInit;
            _onClose = onClose;
        }

        public override void Init(ProcessorContext context) => Context = context;
        public override void Process(byte[] key, byte[] value, long timestamp) => ForwardToChildren(key, value, timestamp);
        public override void Close() { }

        public Task OnInitAsync(ProcessorLifecycleContext context) => _onInit(context);
        public Task OnCloseAsync(ProcessorLifecycleContext context) => _onClose(context);
    }

    /// <summary>Minimal node implementing only IProcessorLifecycle (sync).</summary>
    private sealed class SyncLifecycleNode : ProcessorNode, IProcessorLifecycle
    {
        private readonly Action<ProcessorLifecycleContext> _onClose;

        public SyncLifecycleNode(string name, Action<ProcessorLifecycleContext> onClose) : base(name)
        {
            _onClose = onClose;
        }

        public override void Init(ProcessorContext context) => Context = context;
        public override void Process(byte[] key, byte[] value, long timestamp) => ForwardToChildren(key, value, timestamp);
        public override void Close() { }

        public void OnInit(ProcessorLifecycleContext context) { }
        public void OnClose(ProcessorLifecycleContext context) => _onClose(context);
    }

    /// <summary>Node that implements both async and sync lifecycle — async should win.</summary>
    private sealed class BothLifecycleNode : ProcessorNode, IAsyncProcessorLifecycle, IProcessorLifecycle
    {
        private readonly Func<ProcessorLifecycleContext, Task> _onAsyncClose;
        private readonly Action<ProcessorLifecycleContext> _onSyncClose;

        public BothLifecycleNode(string name,
            Func<ProcessorLifecycleContext, Task> onAsyncClose,
            Action<ProcessorLifecycleContext> onSyncClose)
            : base(name)
        {
            _onAsyncClose = onAsyncClose;
            _onSyncClose = onSyncClose;
        }

        public override void Init(ProcessorContext context) => Context = context;
        public override void Process(byte[] key, byte[] value, long timestamp) => ForwardToChildren(key, value, timestamp);
        public override void Close() { }

        // IAsyncProcessorLifecycle
        public Task OnInitAsync(ProcessorLifecycleContext context) => Task.CompletedTask;
        public Task OnCloseAsync(ProcessorLifecycleContext context) => _onAsyncClose(context);

        // IProcessorLifecycle
        public void OnInit(ProcessorLifecycleContext context) { }
        public void OnClose(ProcessorLifecycleContext context) => _onSyncClose(context);
    }
}
