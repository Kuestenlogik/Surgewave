using System.Collections.Concurrent;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Extensions;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

public sealed class ChannelExtensionsTests
{
    [Fact]
    public async Task AsChannelReader_Yields_All_Records_Until_Cancellation()
    {
        var records = Enumerable.Range(0, 50)
            .Select(i => Result("orders", 0, i, $"value-{i}"))
            .ToArray();
        await using var consumer = new FakeConsumer<string, string>(records);
        using var cts = new CancellationTokenSource();

        var reader = consumer.AsChannelReader(cancellationToken: cts.Token);

        var collected = new List<ConsumeResult<string, string>>();
        for (var i = 0; i < records.Length; i++)
        {
            collected.Add(await reader.ReadAsync(cts.Token));
        }

        Assert.Equal(records.Length, collected.Count);
        Assert.Equal("value-0", collected[0].Value);
        Assert.Equal("value-49", collected[^1].Value);
    }

    [Fact]
    public async Task AsChannelReader_Surfaces_Consumer_Exception_To_Reader()
    {
        await using var consumer = new FakeConsumer<string, string>(
            results: [Result("topic", 0, 0, "first")],
            throwAfter: 1,
            exception: new InvalidOperationException("broker died"));

        var reader = consumer.AsChannelReader();

        var first = await reader.ReadAsync();
        Assert.Equal("first", first.Value);

        var ex = await Assert.ThrowsAnyAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in reader.ReadAllAsync())
            {
            }
        });
        Assert.Equal("broker died", ex.Message);
    }

    [Fact]
    public async Task AsChannelReader_Skips_Null_Polls()
    {
        // Mix in nulls — consumer returns "no message available" between real records.
        await using var consumer = new FakeConsumer<string, string>(
            results: [Result("t", 0, 0, "a"), null, null, Result("t", 0, 1, "b")]);

        var reader = consumer.AsChannelReader();

        Assert.Equal("a", (await reader.ReadAsync()).Value);
        Assert.Equal("b", (await reader.ReadAsync()).Value);
    }

    [Fact]
    public async Task ToAsyncEnumerable_Yields_Records_And_Completes_On_Cancellation()
    {
        var records = Enumerable.Range(0, 5)
            .Select(i => Result("t", 0, i, $"v{i}"))
            .ToArray();
        await using var consumer = new FakeConsumer<string, string>(records);
        using var cts = new CancellationTokenSource();

        var collected = new List<string>();
        await foreach (var r in consumer.ToAsyncEnumerable(cts.Token))
        {
            collected.Add(r.Value);
            if (collected.Count == records.Length)
            {
                cts.Cancel();
            }
        }

        Assert.Equal(["v0", "v1", "v2", "v3", "v4"], collected);
    }

    [Fact]
    public async Task AsProducerChannel_Forwards_Records_In_Order()
    {
        await using var producer = new FakeProducer<string, string>();
        await using var channel = producer.AsProducerChannel();

        for (var i = 0; i < 20; i++)
        {
            await channel.Writer.WriteAsync(new ProducerRecord<string, string>
            {
                Topic = "orders",
                Key = $"k{i}",
                Value = $"v{i}",
            });
        }

        channel.Writer.TryComplete();
        await channel.DrainTask;

        var produced = producer.Produced.ToArray();
        Assert.Equal(20, produced.Length);
        Assert.Equal("v0", produced[0].Value);
        Assert.Equal("v19", produced[^1].Value);
        Assert.All(produced, p => Assert.Null(p.Partition));
    }

    [Fact]
    public async Task AsProducerChannel_Honors_Explicit_Partition()
    {
        await using var producer = new FakeProducer<string, string>();
        await using var channel = producer.AsProducerChannel();

        await channel.Writer.WriteAsync(new ProducerRecord<string, string>
        {
            Topic = "orders",
            Partition = 3,
            Key = "k",
            Value = "v",
        });

        channel.Writer.TryComplete();
        await channel.DrainTask;

        var only = Assert.Single(producer.Produced);
        Assert.Equal(3, only.Partition);
    }

    [Fact]
    public async Task AsProducerChannel_Dispose_Flushes_Buffered_Records()
    {
        await using var producer = new FakeProducer<string, string>();
        var channel = producer.AsProducerChannel();

        for (var i = 0; i < 10; i++)
        {
            await channel.Writer.WriteAsync(new ProducerRecord<string, string>
            {
                Topic = "t",
                Value = $"v{i}",
            });
        }

        // No explicit TryComplete; rely on DisposeAsync to flush + wait.
        await channel.DisposeAsync();

        Assert.Equal(10, producer.Produced.Count);
    }

    [Fact]
    public async Task AsProducerChannel_Forwards_Headers()
    {
        await using var producer = new FakeProducer<string, string>();
        await using var channel = producer.AsProducerChannel();

        await channel.Writer.WriteAsync(new ProducerRecord<string, string>
        {
            Topic = "t",
            Value = "v",
            Headers = new Dictionary<string, byte[]> { ["trace-id"] = [0xDE, 0xAD] },
        });

        channel.Writer.TryComplete();
        await channel.DrainTask;

        var only = Assert.Single(producer.Produced);
        Assert.NotNull(only.Headers);
        Assert.Equal([0xDE, 0xAD], only.Headers!["trace-id"]);
    }

    [Fact]
    public async Task AsProducerChannel_Drain_Task_Faults_On_Producer_Exception()
    {
        await using var producer = new FakeProducer<string, string>
        {
            ProduceThrows = new InvalidOperationException("topic missing"),
        };
        var channel = producer.AsProducerChannel();

        await channel.Writer.WriteAsync(new ProducerRecord<string, string>
        {
            Topic = "t",
            Value = "v",
        });

        var ex = await Assert.ThrowsAnyAsync<InvalidOperationException>(async () => await channel.DisposeAsync());
        Assert.Equal("topic missing", ex.Message);
    }

    private static ConsumeResult<string, string> Result(string topic, int partition, long offset, string value) => new()
    {
        Topic = topic,
        Partition = partition,
        Offset = offset,
        Value = value,
        Timestamp = DateTimeOffset.UtcNow,
    };

    private sealed class FakeConsumer<TKey, TValue> : IConsumer<TKey, TValue>
    {
        private readonly Queue<ConsumeResult<TKey, TValue>?> _results;
        private readonly int _throwAfter;
        private readonly Exception? _exception;
        private int _delivered;

        public FakeConsumer(
            IEnumerable<ConsumeResult<TKey, TValue>?> results,
            int throwAfter = -1,
            Exception? exception = null)
        {
            _results = new Queue<ConsumeResult<TKey, TValue>?>(results);
            _throwAfter = throwAfter;
            _exception = exception;
        }

        public ProtocolType Protocol => ProtocolType.SurgewaveNative;
        public bool IsConnected => true;
        public IReadOnlyList<(string topic, int partition)> Assignment => [];

        public Task<ConsumeResult<TKey, TValue>?> ConsumeAsync(CancellationToken cancellationToken = default)
        {
            if (_exception is not null && _delivered >= _throwAfter)
            {
                return Task.FromException<ConsumeResult<TKey, TValue>?>(_exception);
            }
            if (_results.TryDequeue(out var next))
            {
                if (next is not null)
                {
                    _delivered++;
                }
                return Task.FromResult(next);
            }
            // Empty + no exception: park forever until cancellation.
            var tcs = new TaskCompletionSource<ConsumeResult<TKey, TValue>?>(TaskCreationOptions.RunContinuationsAsynchronously);
            cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));
            return tcs.Task;
        }

        public Task<ConsumeResult<TKey, TValue>?> ConsumeAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
            => ConsumeAsync(cancellationToken);

        public void Subscribe(params string[] topics) { }
        public Task SubscribeAsync(CancellationToken cancellationToken = default, params string[] topics) => Task.CompletedTask;
        public void Assign(string topic, int partition, long offset = 0) { }
        public void Seek(string topic, int partition, long offset) { }
        public Task CommitAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task CommitAsync(TopicPartitionOffset offset, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task CommitAsync(ConsumeResult<TKey, TValue> result, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public Task CommitAsync(IEnumerable<TopicPartitionOffset> offsets, CancellationToken cancellationToken = default) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FakeProducer<TKey, TValue> : IProducer<TKey, TValue>
    {
        public ConcurrentQueue<ProducedRecord> Produced { get; } = new();
        public Exception? ProduceThrows { get; init; }

        public ProtocolType Protocol => ProtocolType.SurgewaveNative;

        public Task<ProduceResult> ProduceAsync(string topic, TKey? key, TValue value, CancellationToken cancellationToken = default)
            => ProduceCore(topic, partition: null, key, value, headers: null);

        public Task<ProduceResult> ProduceAsync(string topic, TKey? key, TValue value, IReadOnlyDictionary<string, byte[]>? headers, CancellationToken cancellationToken = default)
            => ProduceCore(topic, partition: null, key, value, headers);

        public Task<ProduceResult> ProduceAsync(string topic, int partition, TKey? key, TValue value, CancellationToken cancellationToken = default)
            => ProduceCore(topic, partition, key, value, headers: null);

        public Task<ProduceResult> ProduceAsync(string topic, int partition, TKey? key, TValue value, IReadOnlyDictionary<string, byte[]>? headers, CancellationToken cancellationToken = default)
            => ProduceCore(topic, partition, key, value, headers);

        public Task FlushAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private Task<ProduceResult> ProduceCore(string topic, int? partition, TKey? key, TValue value, IReadOnlyDictionary<string, byte[]>? headers)
        {
            if (ProduceThrows is not null)
            {
                return Task.FromException<ProduceResult>(ProduceThrows);
            }
            var offset = Produced.Count;
            Produced.Enqueue(new ProducedRecord(topic, partition, key, value, headers));
            return Task.FromResult(new ProduceResult
            {
                Topic = topic,
                Partition = partition ?? 0,
                Offset = offset,
                Timestamp = DateTimeOffset.UtcNow,
            });
        }

        public sealed record ProducedRecord(string Topic, int? Partition, TKey? Key, TValue Value, IReadOnlyDictionary<string, byte[]>? Headers);
    }
}
