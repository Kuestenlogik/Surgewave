namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Transformer interface for stateful transformations.
/// </summary>
public interface ITransformer<TKeyIn, TValueIn, TKeyOut, TValueOut>
{
    void Init(ProcessorContext context);
    IEnumerable<KeyValue<TKeyOut, TValueOut>> Transform(TKeyIn key, TValueIn value);
    void Close();
}
