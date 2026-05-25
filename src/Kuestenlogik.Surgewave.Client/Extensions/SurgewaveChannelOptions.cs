using System.Threading.Channels;

namespace Kuestenlogik.Surgewave.Client.Extensions;

/// <summary>
/// Tuning knobs for the channel adapters in
/// <see cref="ConsumerChannelExtensions"/> and <see cref="ProducerChannelExtensions"/>.
/// </summary>
public sealed record SurgewaveChannelOptions
{
    /// <summary>
    /// Maximum number of items the underlying channel may buffer. Setting
    /// <see cref="int.MaxValue"/> turns the channel into an unbounded one
    /// (use with care — a slow reader will then make the in-memory queue
    /// grow without limit).
    /// Default: <c>1024</c>.
    /// </summary>
    public int Capacity { get; init; } = 1024;

    /// <summary>
    /// Behaviour when a bounded channel is full and the next item arrives.
    /// Default: <see cref="BoundedChannelFullMode.Wait"/> (back-pressure —
    /// the producer waits for the reader to catch up).
    /// </summary>
    public BoundedChannelFullMode FullMode { get; init; } = BoundedChannelFullMode.Wait;

    /// <summary>
    /// When <c>true</c>, the channel optimises for a single writer (less
    /// synchronisation overhead). Default <c>true</c> — both the consumer-side
    /// pump and the producer-side drain are single tasks.
    /// </summary>
    public bool SingleWriter { get; init; } = true;

    /// <summary>
    /// When <c>true</c>, the channel optimises for a single reader. Default
    /// <c>true</c>. Set to <c>false</c> when fan-out across worker tasks is
    /// the intended consumption pattern.
    /// </summary>
    public bool SingleReader { get; init; } = true;

    /// <summary>
    /// Default options — bounded capacity 1024, back-pressure on overflow.
    /// </summary>
    public static SurgewaveChannelOptions Default { get; } = new();

    internal Channel<T> CreateChannel<T>()
    {
        if (Capacity == int.MaxValue)
        {
            return Channel.CreateUnbounded<T>(new UnboundedChannelOptions
            {
                SingleReader = SingleReader,
                SingleWriter = SingleWriter,
            });
        }

        return Channel.CreateBounded<T>(new BoundedChannelOptions(Capacity)
        {
            FullMode = FullMode,
            SingleReader = SingleReader,
            SingleWriter = SingleWriter,
        });
    }
}
