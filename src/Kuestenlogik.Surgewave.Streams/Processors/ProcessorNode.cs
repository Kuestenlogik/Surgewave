using System.Diagnostics;
using Kuestenlogik.Surgewave.Streams.Monitoring;

namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Node in the processor topology.
/// </summary>
public abstract class ProcessorNode
{
    public string Name { get; }
    public List<ProcessorNode> Children { get; } = new();
    public List<string> StateStoreNames { get; } = new();

    protected ProcessorContext? Context;
    private ProcessorNodeMetrics? _nodeMetrics;
    private bool _lifecycleInitCalled;

    protected ProcessorNode(string name)
    {
        Name = name;
    }

    public abstract void Init(ProcessorContext context);
    public abstract void Process(byte[] key, byte[] value, long timestamp);
    public abstract void Close();

    /// <summary>
    /// Calls <see cref="IAsyncProcessorLifecycle.OnInitAsync"/> or <see cref="IProcessorLifecycle.OnInit"/>
    /// if this node implements either lifecycle interface. Should be called after state stores are initialized.
    /// Async lifecycle takes precedence over sync lifecycle.
    /// </summary>
    internal void InvokeLifecycleInit(ProcessorContext context)
    {
        if (_lifecycleInitCalled)
            return;

        var lifecycleContext = new ProcessorLifecycleContext
        {
            NodeName = Name,
            ProcessorContext = context,
            ShutdownToken = CancellationToken.None,
            ShutdownTimeout = TimeSpan.Zero
        };

        if (this is IAsyncProcessorLifecycle asyncLifecycle)
        {
            // Block synchronously — init is called during startup before processing begins
            asyncLifecycle.OnInitAsync(lifecycleContext).GetAwaiter().GetResult();
        }
        else if (this is IProcessorLifecycle syncLifecycle)
        {
            syncLifecycle.OnInit(lifecycleContext);
        }

        _lifecycleInitCalled = true;
    }

    /// <summary>
    /// Calls <see cref="IProcessorLifecycle.OnClose"/> if this node implements the lifecycle interface.
    /// </summary>
    internal void InvokeLifecycleClose(ProcessorContext context, TimeSpan shutdownTimeout, CancellationToken shutdownToken)
    {
        if (this is IProcessorLifecycle lifecycle)
        {
            var lifecycleContext = new ProcessorLifecycleContext
            {
                NodeName = Name,
                ProcessorContext = context,
                ShutdownToken = shutdownToken,
                ShutdownTimeout = shutdownTimeout
            };
            lifecycle.OnClose(lifecycleContext);
        }
    }

    public void AddChild(ProcessorNode child) => Children.Add(child);
    public void AddStateStore(string storeName) => StateStoreNames.Add(storeName);

    /// <summary>
    /// Processes a record with per-node metrics instrumentation.
    /// Uses allocation-free Stopwatch.GetTimestamp() instead of Stopwatch.StartNew().
    /// </summary>
    public void ProcessInstrumented(byte[] key, byte[] value, long timestamp)
    {
        _nodeMetrics ??= Context?.Metrics.GetOrCreateNodeMetrics(Name);
        _nodeMetrics?.RecordIn();

        var start = Stopwatch.GetTimestamp();
        try
        {
            Process(key, value, timestamp);
        }
        catch
        {
            _nodeMetrics?.RecordError();
            throw;
        }
        finally
        {
            var elapsed = Stopwatch.GetElapsedTime(start);
            _nodeMetrics?.RecordLatency(elapsed.TotalMilliseconds);
        }
    }

    protected void ForwardToChildren(byte[] key, byte[] value, long timestamp)
    {
        var children = Children;
        for (var i = 0; i < children.Count; i++)
        {
            children[i].ProcessInstrumented(key, value, timestamp);
        }

        _nodeMetrics ??= Context?.Metrics.GetOrCreateNodeMetrics(Name);
        _nodeMetrics?.RecordOut(children.Count);
    }
}
