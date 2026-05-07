namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Processor supplier interface.
/// </summary>
public interface IProcessorSupplier<TKeyIn, TValueIn, TKeyOut, TValueOut>
{
    IProcessor<TKeyIn, TValueIn, TKeyOut, TValueOut> Get();
}
