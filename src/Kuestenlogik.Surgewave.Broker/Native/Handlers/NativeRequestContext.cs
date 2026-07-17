using Kuestenlogik.Surgewave.Broker.Native.Streaming;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Context passed to native request handlers. ONE instance is created per connection and reused
/// across requests, which is what makes dispatch allocation-free (#83) — previously every request
/// allocated a context, a closure display class and two delegates.
/// Only <see cref="Header"/> changes between requests, and the processor loop is strictly
/// sequential (single-reader channel, each handler awaited to completion before the next request).
/// Handlers must therefore not read <see cref="Header"/> from anything that outlives HandleAsync;
/// copy what you need first.
/// </summary>
public sealed class NativeRequestContext
{
    private readonly NativeConnectionResponder _responder;

    internal NativeRequestContext(
        NativeConnectionResponder responder,
        BrokerConfig config,
        StreamSubscriptionManager? subscriptionManager)
    {
        _responder = responder;
        Config = config;
        SubscriptionManager = subscriptionManager;
    }

    /// <summary>Header of the request currently being processed. Changes per request.</summary>
    public SurgewaveRequestHeader Header { get; private set; }

    public BrokerConfig Config { get; }

    public bool ClientSupportsCompression => _responder.ClientSupportsCompression;

    /// <summary>
    /// Per-connection subscription manager for push streaming.
    /// Null if streaming is not enabled or the connection hasn't been upgraded.
    /// </summary>
    public StreamSubscriptionManager? SubscriptionManager { get; }

    internal void SetHeader(in SurgewaveRequestHeader header) => Header = header;

    public Task SendResponseAsync(uint requestId, SurgewaveOpCode opCode, SurgewaveErrorCode errorCode,
        ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
        => _responder.SendResponseAsync(requestId, opCode, errorCode, payload, cancellationToken);

    public Task SendErrorAsync(uint requestId, SurgewaveOpCode opCode, SurgewaveErrorCode errorCode,
        string message, CancellationToken cancellationToken)
        => _responder.SendErrorAsync(requestId, opCode, errorCode, message, cancellationToken);
}
