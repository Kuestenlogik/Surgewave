using System.Threading.Channels;
using Kuestenlogik.Surgewave.Client.Abstractions;

namespace Kuestenlogik.Surgewave.Client.Extensions;

/// <summary>
/// Channel-flavoured wrappers around <see cref="IProducer{TKey,TValue}"/>.
/// </summary>
public static class ProducerChannelExtensions
{
    /// <summary>
    /// Build a <see cref="ChannelWriter{T}"/> that a caller can push
    /// <see cref="ProducerRecord{TKey,TValue}"/> values into; a single
    /// background task drains the channel and forwards every record through
    /// <see cref="IProducer{TKey,TValue}.ProduceAsync(string,TKey,TValue,IReadOnlyDictionary{string,byte[]}?,CancellationToken)"/>.
    /// Dispose the returned <see cref="ProducerChannel{TKey,TValue}"/> (or
    /// <c>await using</c> it) to complete the writer and wait for the drain
    /// task to flush any buffered records.
    /// </summary>
    /// <remarks>
    /// Errors from <c>ProduceAsync</c> surface on the writer via
    /// <see cref="ChannelWriter{T}.TryComplete(Exception)"/> — the next
    /// <c>WriteAsync</c> will throw a <see cref="ChannelClosedException"/>
    /// whose <c>InnerException</c> carries the original failure.
    /// </remarks>
    public static ProducerChannel<TKey, TValue> AsProducerChannel<TKey, TValue>(
        this IProducer<TKey, TValue> producer,
        SurgewaveChannelOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(producer);

        var channel = (options ?? SurgewaveChannelOptions.Default)
            .CreateChannel<ProducerRecord<TKey, TValue>>();

        var drainTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var record in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (record.Partition is { } partition)
                    {
                        await producer.ProduceAsync(record.Topic, partition, record.Key, record.Value, record.Headers, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        await producer.ProduceAsync(record.Topic, record.Key, record.Value, record.Headers, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // graceful — caller cancelled
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                throw;
            }
        }, CancellationToken.None);

        return new ProducerChannel<TKey, TValue>(channel.Writer, drainTask);
    }
}

/// <summary>
/// Combined <see cref="ChannelWriter{T}"/> + drain-task handle returned by
/// <see cref="ProducerChannelExtensions.AsProducerChannel{TKey,TValue}"/>.
/// Implements <see cref="IAsyncDisposable"/> for the usual <c>await using</c>
/// pattern — disposing completes the writer and waits for the drain task to
/// flush any pending records.
/// </summary>
public sealed class ProducerChannel<TKey, TValue> : IAsyncDisposable
{
    internal ProducerChannel(ChannelWriter<ProducerRecord<TKey, TValue>> writer, Task drainTask)
    {
        Writer = writer;
        DrainTask = drainTask;
    }

    /// <summary>The <see cref="ChannelWriter{T}"/> callers push records into.</summary>
    public ChannelWriter<ProducerRecord<TKey, TValue>> Writer { get; }

    /// <summary>
    /// The background task that drains the channel through the producer. Await
    /// it (after completing the writer) to observe any failure from
    /// <c>ProduceAsync</c> or to know when every buffered record has been
    /// flushed.
    /// </summary>
    public Task DrainTask { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        Writer.TryComplete();
        try
        {
            await DrainTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // expected when caller cancels the drain pump
        }
    }
}
