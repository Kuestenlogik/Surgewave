using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Core.Observability;

/// <summary>
/// Default <see cref="ISurgewaveBrokerObservability"/> implementation.
/// Multiplexes broker events to every active subscriber via
/// per-subscriber bounded channels with a drop-oldest policy —
/// a slow consumer loses events but never back-pressures the
/// broker's hot path.
/// </summary>
/// <remarks>
/// <para>
/// The broker registers one instance of this class as both
/// <see cref="ISurgewaveBrokerObservability"/> (consumer-facing) and
/// <see cref="SurgewaveBrokerObservability"/> (publisher-facing) in DI.
/// Pipeline code calls <see cref="Publish"/> directly on the
/// concrete type; <c>Bowire.Protocol.Surgewave</c> and any other
/// observer resolves the interface and iterates
/// <see cref="ObserveAsync"/>.
/// </para>
/// <para>
/// Subscriber channels use <see cref="BoundedChannelFullMode.DropOldest"/>
/// so the broker keeps emitting. Each subscriber's drop count is
/// logged once per <see cref="DropWarningThreshold"/> events so a
/// stuck observer is obvious in operator logs without drowning
/// them.
/// </para>
/// </remarks>
public sealed class SurgewaveBrokerObservability : ISurgewaveBrokerObservability
{
    /// <summary>Per-subscriber buffer size. 1024 matches the
    /// broker's default produce-batch window so a short GC pause in
    /// a consumer doesn't drop events under normal load.</summary>
    public const int DefaultSubscriberCapacity = 1024;

    /// <summary>Emit a "dropped N events" warning at most once per
    /// this many drops on a given subscriber.</summary>
    public const int DropWarningThreshold = 100;

    private readonly ILogger<SurgewaveBrokerObservability> _logger;
    private readonly int _subscriberCapacity;
    private readonly List<Subscription> _subscribers = new();
    private readonly object _subscribersLock = new();

    /// <summary>
    /// Lock-free subscriber count surfaced to the broker's hot path via
    /// <see cref="HasSubscribers"/>. Maintained alongside the
    /// <see cref="_subscribers"/> list under <see cref="_subscribersLock"/>;
    /// callers on the hot path read it via <see cref="Volatile.Read(ref int)"/>
    /// so they never contend with the subscribers lock.
    /// </summary>
    private int _subscriberCount;

    /// <inheritdoc />
    public bool HasSubscribers => Volatile.Read(ref _subscriberCount) > 0;

    /// <summary>Construct with the supplied logger. Buffer size
    /// defaults to <see cref="DefaultSubscriberCapacity"/>.</summary>
    public SurgewaveBrokerObservability(ILogger<SurgewaveBrokerObservability>? logger = null,
        int subscriberCapacity = DefaultSubscriberCapacity)
    {
        _logger = logger ?? NullLogger<SurgewaveBrokerObservability>.Instance;
        _subscriberCapacity = subscriberCapacity;
    }

    /// <summary>
    /// Push an event to every active subscriber. Safe to call from
    /// any thread; broker pipeline code invokes this from produce,
    /// consume, and reject paths. Never throws — channel writes are
    /// non-blocking (<c>DropOldest</c> eats the event on overflow
    /// with a deferred warning).
    /// </summary>
    public void Publish(SurgewaveBrokerEvent evt)
    {
        ArgumentNullException.ThrowIfNull(evt);
        lock (_subscribersLock)
        {
            foreach (var subscription in _subscribers)
            {
                // TryWrite returns false on DropOldest when the
                // channel is at capacity; the oldest entry is
                // replaced by this one. Track the drop count so
                // the subscriber sees periodic warnings.
                if (!subscription.Writer.TryWrite(evt))
                {
                    subscription.DropCount++;
                    if (subscription.DropCount % DropWarningThreshold == 1)
                    {
                        _logger.LogWarning(
                            "SurgewaveBrokerObservability subscriber '{Name}' has dropped {Count} event(s); " +
                            "consumer is slower than the broker hot path.",
                            subscription.Name, subscription.DropCount);
                    }
                }
            }
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<SurgewaveBrokerEvent> ObserveAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var channel = Channel.CreateBounded<SurgewaveBrokerEvent>(new BoundedChannelOptions(_subscriberCapacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest,
            SingleReader = true,
            SingleWriter = false,
        });

        var subscription = new Subscription(channel.Writer, name: "observer-" + Guid.NewGuid().ToString("N")[..8]);
        lock (_subscribersLock)
        {
            _subscribers.Add(subscription);
            Volatile.Write(ref _subscriberCount, _subscribers.Count);
        }

        try
        {
            await foreach (var evt in channel.Reader.ReadAllAsync(ct))
            {
                yield return evt;
            }
        }
        finally
        {
            lock (_subscribersLock)
            {
                _subscribers.Remove(subscription);
                Volatile.Write(ref _subscriberCount, _subscribers.Count);
            }
            channel.Writer.TryComplete();
        }
    }

    private sealed class Subscription(ChannelWriter<SurgewaveBrokerEvent> writer, string name)
    {
        public ChannelWriter<SurgewaveBrokerEvent> Writer { get; } = writer;
        public string Name { get; } = name;
        public long DropCount { get; set; }
    }
}
