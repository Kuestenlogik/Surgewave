namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Processor node that applies per-stream rate limiting using a token bucket.
/// </summary>
internal sealed class RateLimitNode<TKey, TValue> : ProcessorNode
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly RateLimiter _limiter;
    private readonly int _maxWaitMs;

    public RateLimitNode(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        int recordsPerSecond,
        int maxWaitMs = 5000)
        : base(name)
    {
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _limiter = new RateLimiter(recordsPerSecond);
        _maxWaitMs = maxWaitMs;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        if (!_limiter.TryConsume(1))
        {
            var waitMs = _limiter.CalculateWaitTimeMs(1);
            if (waitMs > 0 && waitMs <= _maxWaitMs)
            {
                Thread.Sleep((int)waitMs);
                _limiter.Refill();
                _limiter.TryConsume(1);
                Context?.Metrics.RecordRateLimitThrottle((int)waitMs);
            }
        }

        ForwardToChildren(key, value, timestamp);
    }

    public override void Close() { }
}
