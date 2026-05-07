using System.Threading.Channels;

namespace Kuestenlogik.Surgewave.Streams.Processors;

/// <summary>
/// Channel-based parallel processor that enables within-partition parallelism for stateless operations.
/// This is a competitive advantage over Kafka Streams which is limited to partition-level parallelism.
/// Uses System.Threading.Channels for lock-free, high-throughput work distribution.
/// </summary>
internal sealed class ParallelProcessorNode<TKeyIn, TValueIn, TKeyOut, TValueOut> : ProcessorNode
{
    private readonly ISerde<TKeyIn> _keyInSerde;
    private readonly ISerde<TValueIn> _valueInSerde;
    private readonly ISerde<TKeyOut> _keyOutSerde;
    private readonly ISerde<TValueOut> _valueOutSerde;
    private readonly Func<TKeyIn, TValueIn, IEnumerable<KeyValue<TKeyOut, TValueOut>>> _processor;
    private readonly int _degreeOfParallelism;
    private Channel<WorkItem>? _channel;
    private Task[]? _workers;
    private CancellationTokenSource? _cts;

    public ParallelProcessorNode(
        string name,
        ISerde<TKeyIn> keyInSerde,
        ISerde<TValueIn> valueInSerde,
        ISerde<TKeyOut> keyOutSerde,
        ISerde<TValueOut> valueOutSerde,
        Func<TKeyIn, TValueIn, IEnumerable<KeyValue<TKeyOut, TValueOut>>> processor,
        int degreeOfParallelism)
        : base(name)
    {
        _keyInSerde = keyInSerde;
        _valueInSerde = valueInSerde;
        _keyOutSerde = keyOutSerde;
        _valueOutSerde = valueOutSerde;
        _processor = processor;
        _degreeOfParallelism = degreeOfParallelism;
    }

    public override void Init(ProcessorContext context)
    {
        Context = context;
        _cts = new CancellationTokenSource();
        _channel = Channel.CreateBounded<WorkItem>(new BoundedChannelOptions(_degreeOfParallelism * 4)
        {
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        _workers = new Task[_degreeOfParallelism];
        for (int i = 0; i < _degreeOfParallelism; i++)
        {
            _workers[i] = Task.Run(() => ProcessWorkerAsync(_cts.Token));
        }
    }

    public override void Process(byte[] key, byte[] value, long timestamp)
    {
        if (_channel == null)
            return;

        // Synchronously write to channel (blocks if full)
        var item = new WorkItem(key, value, timestamp);
        _channel.Writer.TryWrite(item);
    }

    private async Task ProcessWorkerAsync(CancellationToken token)
    {
        if (_channel == null) return;

        await foreach (var item in _channel.Reader.ReadAllAsync(token))
        {
            try
            {
                var keyIn = _keyInSerde.Deserialize(item.Key);
                var valueIn = _valueInSerde.Deserialize(item.Value);

                foreach (var result in _processor(keyIn, valueIn))
                {
                    var keyOut = _keyOutSerde.Serialize(result.Key);
                    var valueOut = _valueOutSerde.Serialize(result.Value);

                    lock (Children)
                    {
                        ForwardToChildren(keyOut, valueOut, item.Timestamp);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    public override void Close()
    {
        _channel?.Writer.Complete();
        _cts?.Cancel();

        if (_workers != null)
        {
            try
            {
                Task.WaitAll(_workers, TimeSpan.FromSeconds(5));
            }
            catch (AggregateException)
            {
                // Workers may have been cancelled
            }
        }

        _cts?.Dispose();
    }

    private readonly record struct WorkItem(byte[] Key, byte[] Value, long Timestamp);
}
