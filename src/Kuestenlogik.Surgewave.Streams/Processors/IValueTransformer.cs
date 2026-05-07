namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Value transformer interface.
/// </summary>
public interface IValueTransformer<TValueIn, TValueOut>
{
    void Init(ProcessorContext context);
    TValueOut Transform(TValueIn value);
    void Close();
}
