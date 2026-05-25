using Kuestenlogik.Surgewave.Streams.Resilience;

namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Processor node that wraps downstream processing with retry logic.
/// </summary>
public sealed class RetryNode<TKey, TValue> : ProcessorNode
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly StreamsRetryPolicy _retryPolicy;

    public RetryNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        StreamsRetryConfig retryConfig)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _retryPolicy = new StreamsRetryPolicy(retryConfig);
    }

    public override void Init(ProcessorContext context) => Context = context;

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        _retryPolicy.Execute(
            () => ForwardToChildren(key, value, timestamp),
            Context?.Metrics);
    }

    public override void Close() { }
}
