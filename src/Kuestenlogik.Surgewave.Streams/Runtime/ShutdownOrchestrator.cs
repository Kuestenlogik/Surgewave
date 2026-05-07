using Kuestenlogik.Surgewave.Streams.Processors;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.Runtime;

/// <summary>
/// Orchestrates graceful shutdown of processor nodes in reverse-topological order
/// (sinks first, then processors, then sources).
/// </summary>
public sealed class ShutdownOrchestrator
{
    private readonly ILogger _logger;
    private readonly TimeSpan _shutdownTimeout;

    /// <summary>
    /// Raised whenever the shutdown transitions to a new <see cref="ShutdownPhase"/>.
    /// </summary>
    public event Action<ShutdownPhase>? PhaseChanged;

    public ShutdownOrchestrator(ILogger logger, TimeSpan shutdownTimeout)
    {
        _logger = logger;
        _shutdownTimeout = shutdownTimeout;
    }

    private void RaisePhase(ShutdownPhase phase)
    {
        _logger.LogDebug("Shutdown phase: {Phase}", phase);
        PhaseChanged?.Invoke(phase);
    }

    /// <summary>
    /// Shuts down all nodes in reverse-topological order with timeout (synchronous).
    /// </summary>
    public void Shutdown(IEnumerable<ProcessorNode> sourceNodes, ProcessorContext context, CancellationToken cancellationToken = default)
    {
        RaisePhase(ShutdownPhase.Starting);

        var topologicalOrder = CollectNodes(sourceNodes);

        RaisePhase(ShutdownPhase.TasksSuspending);
        RaisePhase(ShutdownPhase.StoresFlushing);
        RaisePhase(ShutdownPhase.NodesStopping);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_shutdownTimeout);

        foreach (var node in topologicalOrder)
        {
            if (timeoutCts.Token.IsCancellationRequested)
            {
                _logger.LogWarning("Shutdown timeout reached, skipping remaining nodes");
                break;
            }

            CloseNodeWithLifecycle(node, context, timeoutCts.Token);
        }

        RaisePhase(ShutdownPhase.Completed);
    }

    /// <summary>
    /// Shuts down all nodes in reverse-topological order with timeout (async).
    /// Checks for <see cref="IAsyncProcessorLifecycle"/> first; falls back to sync <see cref="IProcessorLifecycle"/>.
    /// </summary>
    public async Task ShutdownAsync(IEnumerable<ProcessorNode> sourceNodes, ProcessorContext context, CancellationToken ct = default)
    {
        RaisePhase(ShutdownPhase.Starting);

        var topologicalOrder = CollectNodes(sourceNodes);

        RaisePhase(ShutdownPhase.TasksSuspending);
        RaisePhase(ShutdownPhase.StoresFlushing);
        RaisePhase(ShutdownPhase.NodesStopping);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(_shutdownTimeout);

        foreach (var node in topologicalOrder)
        {
            if (timeoutCts.Token.IsCancellationRequested)
            {
                _logger.LogWarning("Shutdown timeout reached, skipping remaining nodes");
                break;
            }

            await CloseNodeWithLifecycleAsync(node, context, timeoutCts.Token);
        }

        RaisePhase(ShutdownPhase.Completed);
    }

    private void CloseNodeWithLifecycle(ProcessorNode node, ProcessorContext context, CancellationToken token)
    {
        try
        {
            using var nodeCts = CancellationTokenSource.CreateLinkedTokenSource(token);
            nodeCts.CancelAfter(TimeSpan.FromSeconds(10)); // Per-node timeout

            try
            {
                node.InvokeLifecycleClose(context, _shutdownTimeout, nodeCts.Token);
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("Node {NodeName} OnClose timed out", node.Name);
            }

            node.Close();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error closing node {NodeName}", node.Name);
        }
    }

    private async Task CloseNodeWithLifecycleAsync(ProcessorNode node, ProcessorContext context, CancellationToken token)
    {
        try
        {
            using var nodeCts = CancellationTokenSource.CreateLinkedTokenSource(token);
            nodeCts.CancelAfter(TimeSpan.FromSeconds(10)); // Per-node timeout

            try
            {
                var lifecycleContext = new ProcessorLifecycleContext
                {
                    NodeName = node.Name,
                    ProcessorContext = context,
                    ShutdownToken = nodeCts.Token,
                    ShutdownTimeout = _shutdownTimeout
                };

                if (node is IAsyncProcessorLifecycle asyncLifecycle)
                {
                    await asyncLifecycle.OnCloseAsync(lifecycleContext);
                }
                else if (node is IProcessorLifecycle syncLifecycle)
                {
                    syncLifecycle.OnClose(lifecycleContext);
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogWarning("Node {NodeName} OnCloseAsync timed out", node.Name);
            }

            node.Close();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error closing node {NodeName}", node.Name);
        }
    }

    /// <summary>
    /// Initializes lifecycle hooks on all nodes after state stores are ready.
    /// </summary>
    public void InitializeLifecycles(IEnumerable<ProcessorNode> sourceNodes, ProcessorContext context)
    {
        var visited = new HashSet<ProcessorNode>();

        foreach (var source in sourceNodes)
        {
            InitLifecycleRecursive(source, context, visited);
        }
    }

    private void InitLifecycleRecursive(ProcessorNode node, ProcessorContext context, HashSet<ProcessorNode> visited)
    {
        if (!visited.Add(node))
            return;

        try
        {
            node.InvokeLifecycleInit(context);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error in OnInit for node {NodeName}", node.Name);
        }

        foreach (var child in node.Children)
        {
            InitLifecycleRecursive(child, context, visited);
        }
    }

    private static List<ProcessorNode> CollectNodes(IEnumerable<ProcessorNode> sourceNodes)
    {
        var topologicalOrder = new List<ProcessorNode>();
        var visited = new HashSet<ProcessorNode>();

        foreach (var source in sourceNodes)
        {
            CollectTopological(source, topologicalOrder, visited);
        }

        // Reverse: close sinks first, then processors, then sources
        topologicalOrder.Reverse();
        return topologicalOrder;
    }

    private static void CollectTopological(ProcessorNode node, List<ProcessorNode> result, HashSet<ProcessorNode> visited)
    {
        if (!visited.Add(node))
            return;

        result.Add(node);

        foreach (var child in node.Children)
        {
            CollectTopological(child, result, visited);
        }
    }
}
