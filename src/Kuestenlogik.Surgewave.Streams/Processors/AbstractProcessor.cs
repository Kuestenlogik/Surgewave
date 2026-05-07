namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Base processor implementation.
/// </summary>
public abstract class AbstractProcessor<TKeyIn, TValueIn, TKeyOut, TValueOut> : IProcessor<TKeyIn, TValueIn, TKeyOut, TValueOut>
{
    protected ProcessorContext Context { get; private set; } = null!;

    public virtual void Init(ProcessorContext context)
    {
        Context = context;
    }

    public abstract void Process(StreamRecord<TKeyIn, TValueIn> record);

    public virtual void Close() { }

    protected void Forward(TKeyOut key, TValueOut value, ISerde<TKeyOut> keySerde, ISerde<TValueOut> valueSerde)
    {
        Context.Forward(key, value, keySerde, valueSerde);
    }
}
