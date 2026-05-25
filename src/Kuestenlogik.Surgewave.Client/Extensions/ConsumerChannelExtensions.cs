using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;

namespace Kuestenlogik.Surgewave.Client.Extensions;

/// <summary>
/// Channel- and async-iterator-flavoured wrappers around <see cref="IConsumer{TKey,TValue}"/>.
/// Provide the idiomatic .NET surface (<c>await foreach</c>, <c>ChannelReader</c>)
/// without forcing every consumer implementation to adopt it directly.
/// </summary>
public static class ConsumerChannelExtensions
{
    /// <summary>
    /// Pump the consumer's <see cref="IConsumer{TKey,TValue}.ConsumeAsync(CancellationToken)"/>
    /// loop into a <see cref="ChannelReader{T}"/> and return it. A single background
    /// task drives the loop; the channel is completed cleanly when
    /// <paramref name="cancellationToken"/> fires, and completed with an exception
    /// if the consumer throws.
    /// </summary>
    /// <remarks>
    /// The background pump runs until the cancellation token is signalled. It
    /// is the caller's responsibility to keep the consumer alive for the lifetime
    /// of the returned channel — disposing the consumer concurrently will surface
    /// as a faulted channel.
    /// </remarks>
    public static ChannelReader<ConsumeResult<TKey, TValue>> AsChannelReader<TKey, TValue>(
        this IConsumer<TKey, TValue> consumer,
        SurgewaveChannelOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(consumer);

        var channel = (options ?? SurgewaveChannelOptions.Default)
            .CreateChannel<ConsumeResult<TKey, TValue>>();

        _ = Task.Run(async () =>
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = await consumer.ConsumeAsync(cancellationToken).ConfigureAwait(false);
                    if (result is null)
                    {
                        continue;
                    }

                    await channel.Writer.WriteAsync(result, cancellationToken).ConfigureAwait(false);
                }

                channel.Writer.TryComplete();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
            }
        }, CancellationToken.None);

        return channel.Reader;
    }

    /// <summary>
    /// Yield consumed records as an <see cref="IAsyncEnumerable{T}"/>. Allows
    /// idiomatic <c>await foreach (var record in consumer.ToAsyncEnumerable())</c>
    /// without manually building a channel pump.
    /// </summary>
    public static async IAsyncEnumerable<ConsumeResult<TKey, TValue>> ToAsyncEnumerable<TKey, TValue>(
        this IConsumer<TKey, TValue> consumer,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(consumer);

        while (!cancellationToken.IsCancellationRequested)
        {
            ConsumeResult<TKey, TValue>? result;
            try
            {
                result = await consumer.ConsumeAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                yield break;
            }

            if (result is null)
            {
                continue;
            }

            yield return result;
        }
    }
}
