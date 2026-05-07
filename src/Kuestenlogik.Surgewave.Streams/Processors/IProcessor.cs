namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Base processor interface.
/// </summary>
public interface IProcessor
{
    void Init(ProcessorContext context);
    void Process(byte[] key, byte[] value, long timestamp);
    void Close();
}

/// <summary>
/// Typed processor interface.
/// </summary>
public interface IProcessor<TKeyIn, TValueIn, TKeyOut, TValueOut>
{
    void Init(ProcessorContext context);
    void Process(StreamRecord<TKeyIn, TValueIn> record);
    void Close();
}
