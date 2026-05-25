using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Processors;
using Kuestenlogik.Surgewave.Streams.Runtime;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

/// <summary>
/// Advanced tests for async lifecycle processors covering multiple nodes,
/// init-before-processing ordering, close-all-even-if-one-throws,
/// and graceful degradation under slow init.
/// </summary>
public sealed class AsyncLifecycleAdvancedTests : IAsyncDisposable
{
    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger _logger;

    public AsyncLifecycleAdvancedTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(b => b.AddFilter(l => l >= LogLevel.Debug));
        _logger = _loggerFactory.CreateLogger<AsyncLifecycleAdvancedTests>();
    }

    public async ValueTask DisposeAsync()
    {
        _loggerFactory.Dispose();
        await Task.CompletedTask;
    }

    // ── Multiple async lifecycle processors in a topology ────────────────────

    [Fact]
    public async Task MultipleAsyncNodes_AllReceiveOnCloseAsync()
    {
        var config = MakeConfig("multi-async-close");
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        var closeLog = new List<string>();

        var node1 = new AsyncLifecycleNode("NODE-1",
            onInit: _ => Task.CompletedTask,
            onClose: _ => { lock (closeLog) closeLog.Add("NODE-1"); return Task.CompletedTask; });
        var node2 = new AsyncLifecycleNode("NODE-2",
            onInit: _ => Task.CompletedTask,
            onClose: _ => { lock (closeLog) closeLog.Add("NODE-2"); return Task.CompletedTask; });
        var node3 = new AsyncLifecycleNode("NODE-3",
            onInit: _ => Task.CompletedTask,
            onClose: _ => { lock (closeLog) closeLog.Add("NODE-3"); return Task.CompletedTask; });

        // Chain: node1 → node2 → node3
        node1.AddChild(node2);
        node2.AddChild(node3);
        node1.Init(context);
        node2.Init(context);
        node3.Init(context);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);
        await orchestrator.ShutdownAsync([node1], context);

        // All three must have been closed (in reverse topology: node3, node2, node1)
        Assert.Equal(3, closeLog.Count);
        Assert.Contains("NODE-1", closeLog);
        Assert.Contains("NODE-2", closeLog);
        Assert.Contains("NODE-3", closeLog);
    }

    [Fact]
    public async Task MultipleAsyncNodes_OnInitAsync_AllCalledViaInitializeLifecycles()
    {
        var config = MakeConfig("multi-async-init");
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        var initLog = new List<string>();

        var node1 = new AsyncLifecycleNode("INIT-NODE-1",
            onInit: _ => { lock (initLog) initLog.Add("INIT-NODE-1"); return Task.CompletedTask; },
            onClose: _ => Task.CompletedTask);
        var node2 = new AsyncLifecycleNode("INIT-NODE-2",
            onInit: _ => { lock (initLog) initLog.Add("INIT-NODE-2"); return Task.CompletedTask; },
            onClose: _ => Task.CompletedTask);

        node1.AddChild(node2);
        node1.Init(context);
        node2.Init(context);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);
        orchestrator.InitializeLifecycles([node1], context);

        Assert.Equal(2, initLog.Count);
        Assert.Contains("INIT-NODE-1", initLog);
        Assert.Contains("INIT-NODE-2", initLog);

        await orchestrator.ShutdownAsync([node1], context);
    }

    // ── OnInitAsync called before any records processed ───────────────────────

    [Fact]
    public void OnInitAsync_CalledBeforeProcess_ViaInitializeLifecycles()
    {
        var config = MakeConfig("init-before-process");
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        bool initCalled = false;
        bool processCalled = false;
        bool initCalledBeforeProcess = false;

        var node = new TrackingAsyncLifecycleNode("TRACKING-NODE",
            onInit: _ =>
            {
                initCalled = true;
                return Task.CompletedTask;
            },
            onProcess: () =>
            {
                processCalled = true;
                initCalledBeforeProcess = initCalled; // capture whether init ran first
            });

        node.Init(context);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);
        orchestrator.InitializeLifecycles([node], context);

        Assert.True(initCalled, "OnInitAsync must be called via InitializeLifecycles");

        // Now simulate processing
        node.Process(Array.Empty<byte>(), Array.Empty<byte>(), 0L);

        Assert.True(processCalled);
        Assert.True(initCalledBeforeProcess, "Init must complete before Process is called");
    }

    // ── OnCloseAsync called for all processors even if one throws ─────────────

    [Fact]
    public async Task OnCloseAsync_CalledForAllNodes_EvenIfOneThrows()
    {
        var config = MakeConfig("close-all-on-throw");
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        var closedNodes = new List<string>();

        var node1 = new AsyncLifecycleNode("CLOSE-NODE-1",
            onInit: _ => Task.CompletedTask,
            onClose: _ => { lock (closedNodes) closedNodes.Add("CLOSE-NODE-1"); return Task.CompletedTask; });

        // Node 2 throws during close
        var node2 = new AsyncLifecycleNode("CLOSE-NODE-2",
            onInit: _ => Task.CompletedTask,
            onClose: _ => throw new InvalidOperationException("Simulated close failure"));

        var node3 = new AsyncLifecycleNode("CLOSE-NODE-3",
            onInit: _ => Task.CompletedTask,
            onClose: _ => { lock (closedNodes) closedNodes.Add("CLOSE-NODE-3"); return Task.CompletedTask; });

        // Linear chain: node1 → node2 → node3
        node1.AddChild(node2);
        node2.AddChild(node3);
        node1.Init(context);
        node2.Init(context);
        node3.Init(context);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);

        // ShutdownAsync must NOT propagate the exception — it logs and continues
        await orchestrator.ShutdownAsync([node1], context);

        // node1 and node3 must still have been closed despite node2 throwing
        Assert.Contains("CLOSE-NODE-1", closedNodes);
        Assert.Contains("CLOSE-NODE-3", closedNodes);
    }

    [Fact]
    public async Task OnCloseAsync_AllNodesThrow_ShutdownCompletes()
    {
        var config = MakeConfig("all-throw-close");
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        var node1 = new AsyncLifecycleNode("THROW-1",
            onInit: _ => Task.CompletedTask,
            onClose: _ => throw new InvalidOperationException("node1 close error"));
        var node2 = new AsyncLifecycleNode("THROW-2",
            onInit: _ => Task.CompletedTask,
            onClose: _ => throw new InvalidOperationException("node2 close error"));

        node1.AddChild(node2);
        node1.Init(context);
        node2.Init(context);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);

        // Must complete without exception even when all nodes throw
        var ex = await Record.ExceptionAsync(() =>
            orchestrator.ShutdownAsync([node1], context));

        Assert.Null(ex);
    }

    // ── Graceful degradation: slow init, verify timeout ───────────────────────

    [Fact]
    public async Task OnInitAsync_SlowInit_DoesNotBlockForeverDuringShutdown()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "slow-init-timeout",
            BootstrapServers = "localhost:9092",
            ShutdownTimeout = TimeSpan.FromMilliseconds(300)
        };
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        var initStarted = false;

        // Node that hangs in OnCloseAsync to simulate a slow shutdown
        var slowNode = new AsyncLifecycleNode("SLOW-INIT-NODE",
            onInit: _ => { initStarted = true; return Task.CompletedTask; },
            onClose: async ctx =>
            {
                await Task.Delay(Timeout.Infinite, ctx.ShutdownToken)
                    .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            });
        slowNode.Init(context);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);
        orchestrator.InitializeLifecycles([slowNode], context);

        Assert.True(initStarted, "OnInitAsync should have run during InitializeLifecycles");

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await orchestrator.ShutdownAsync([slowNode], context);
        sw.Stop();

        _output.WriteLine($"Slow-init shutdown took {sw.ElapsedMilliseconds}ms");

        // Shutdown must not hang past a generous upper bound
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(3),
            $"Shutdown should complete within 3s; took {sw.Elapsed}");
    }

    [Fact]
    public async Task MultipleSlowNodes_ShutdownDoesNotExceedMaxTimeout()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "multi-slow-shutdown",
            BootstrapServers = "localhost:9092",
            ShutdownTimeout = TimeSpan.FromMilliseconds(500)
        };
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        // Three nodes that each hang in OnCloseAsync
        var nodes = Enumerable.Range(1, 3).Select(i =>
        {
            var n = new AsyncLifecycleNode($"SLOW-{i}",
                onInit: _ => Task.CompletedTask,
                onClose: async ctx =>
                {
                    await Task.Delay(Timeout.Infinite, ctx.ShutdownToken)
                        .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
                });
            n.Init(context);
            return n;
        }).ToList();

        // Chain the nodes
        for (int i = 0; i < nodes.Count - 1; i++)
            nodes[i].AddChild(nodes[i + 1]);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await orchestrator.ShutdownAsync([nodes[0]], context);
        sw.Stop();

        _output.WriteLine($"Multi-slow-node shutdown took {sw.ElapsedMilliseconds}ms");

        // Should complete well before 10 seconds regardless of slow nodes
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10),
            $"Shutdown took too long: {sw.Elapsed}");
    }

    // ── Shutdown phases still fire when nodes present ─────────────────────────

    [Fact]
    public async Task ShutdownAsync_WithAsyncNodes_PhasesFireInOrder()
    {
        var config = MakeConfig("phases-with-nodes");
        var context = new ProcessorContext(config, new StreamsMetrics(), _logger);

        var phases = new List<ShutdownPhase>();

        var node = new AsyncLifecycleNode("PHASE-NODE",
            onInit: _ => Task.CompletedTask,
            onClose: _ => Task.CompletedTask);
        node.Init(context);

        var orchestrator = new ShutdownOrchestrator(_logger, config.ShutdownTimeout);
        orchestrator.PhaseChanged += p => phases.Add(p);

        await orchestrator.ShutdownAsync([node], context);

        Assert.Equal(
            new[]
            {
                ShutdownPhase.Starting,
                ShutdownPhase.TasksSuspending,
                ShutdownPhase.StoresFlushing,
                ShutdownPhase.NodesStopping,
                ShutdownPhase.Completed
            },
            phases);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static StreamsConfig MakeConfig(string appId) => new()
    {
        ApplicationId = appId,
        BootstrapServers = "localhost:9092",
        ShutdownTimeout = TimeSpan.FromSeconds(5)
    };

    private sealed class AsyncLifecycleNode : ProcessorNode, IAsyncProcessorLifecycle
    {
        private readonly Func<ProcessorLifecycleContext, Task> _onInit;
        private readonly Func<ProcessorLifecycleContext, Task> _onClose;

        public AsyncLifecycleNode(
            string name,
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

    /// <summary>Node that tracks when Process is called relative to Init.</summary>
    private sealed class TrackingAsyncLifecycleNode : ProcessorNode, IAsyncProcessorLifecycle
    {
        private readonly Func<ProcessorLifecycleContext, Task> _onInit;
        private readonly Action _onProcess;

        public TrackingAsyncLifecycleNode(
            string name,
            Func<ProcessorLifecycleContext, Task> onInit,
            Action onProcess)
            : base(name)
        {
            _onInit = onInit;
            _onProcess = onProcess;
        }

        public override void Init(ProcessorContext context) => Context = context;

        public override void Process(byte[] key, byte[] value, long timestamp)
        {
            _onProcess();
            ForwardToChildren(key, value, timestamp);
        }

        public override void Close() { }
        public Task OnInitAsync(ProcessorLifecycleContext context) => _onInit(context);
        public Task OnCloseAsync(ProcessorLifecycleContext context) => Task.CompletedTask;
    }
}
