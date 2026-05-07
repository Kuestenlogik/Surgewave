using Kuestenlogik.Surgewave.Streams.EventTime;

namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Processor node that extracts timestamps and generates watermarks from stream records.
/// Injects watermark updates into the ProcessorContext as records flow through.
/// </summary>
internal sealed class WatermarkProcessorNode<TKey, TValue> : ProcessorNode
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly ITimestampAssigner<TValue> _timestampAssigner;
    private readonly IWatermarkGenerator<TValue> _watermarkGenerator;

    public WatermarkProcessorNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        ITimestampAssigner<TValue> timestampAssigner,
        IWatermarkGenerator<TValue> watermarkGenerator)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _timestampAssigner = timestampAssigner;
        _watermarkGenerator = watermarkGenerator;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        var v = _valueSerde.Deserialize(value);

        // Extract event timestamp
        var eventTimestamp = _timestampAssigner.ExtractTimestamp(v, timestamp);

        // Update watermark generator
        _watermarkGenerator.OnEvent(v, eventTimestamp);

        // Propagate watermark to context
        var watermark = _watermarkGenerator.GetCurrentWatermark();
        Context?.UpdateWatermark(watermark);

        // Forward with the extracted event timestamp
        ForwardToChildren(key, value, eventTimestamp);
    }

    public override void Close() { }
}
