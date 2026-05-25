namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Lambda-based processor supplier.
/// </summary>
public sealed class LambdaProcessorSupplier<TKeyIn, TValueIn, TKeyOut, TValueOut>
    : IProcessorSupplier<TKeyIn, TValueIn, TKeyOut, TValueOut>
{
    private readonly Func<IProcessor<TKeyIn, TValueIn, TKeyOut, TValueOut>> _supplier;

    public LambdaProcessorSupplier(Func<IProcessor<TKeyIn, TValueIn, TKeyOut, TValueOut>> supplier)
    {
        _supplier = supplier;
    }

    public IProcessor<TKeyIn, TValueIn, TKeyOut, TValueOut> Get() => _supplier();
}
