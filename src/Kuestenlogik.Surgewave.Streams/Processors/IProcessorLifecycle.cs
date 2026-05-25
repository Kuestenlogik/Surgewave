namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Optional lifecycle interface for processor nodes.
/// Implement this on custom processors to receive init/close callbacks with context.
/// </summary>
public interface IProcessorLifecycle
{
    void OnInit(ProcessorLifecycleContext context);
    void OnClose(ProcessorLifecycleContext context);
}
