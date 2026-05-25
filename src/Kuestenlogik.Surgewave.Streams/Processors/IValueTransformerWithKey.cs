namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Value transformer with key access.
/// </summary>
public interface IValueTransformerWithKey<TKey, TValueIn, TValueOut>
{
    void Init(ProcessorContext context);
    TValueOut Transform(TKey key, TValueIn value);
    void Close();
}
