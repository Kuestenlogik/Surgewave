namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Async lifecycle interface for processor nodes.
/// Implement this to receive async init/close callbacks.
/// Takes precedence over <see cref="IProcessorLifecycle"/> when both are present.
/// </summary>
public interface IAsyncProcessorLifecycle
{
    /// <summary>Called after state stores are ready. Use for async resource initialization.</summary>
    Task OnInitAsync(ProcessorLifecycleContext context);

    /// <summary>Called during graceful shutdown. Use for async resource cleanup.</summary>
    Task OnCloseAsync(ProcessorLifecycleContext context);
}
