using System.Threading.Channels;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Extensions;

/// <summary>
/// Reactive extensions for Surgewave client.
/// Provides IObservable and ISubject patterns for reactive programming.
/// </summary>
public static class ReactiveExtensions
{
    // ═══════════════════════════════════════════════════════════════
    // IObservable - Consuming messages reactively
    // ═══════════════════════════════════════════════════════════════

    /// <summary>
    /// Convert a ReceiveBuilder to an IObservable for reactive consumption.
    /// Compatible with System.Reactive operators when that package is referenced.
    /// </summary>
    /// <example>
    /// client.Messaging.Receive("events")
    ///     .FromBeginning()
    ///     .AsObservable()
    ///     .Subscribe(msg => Console.WriteLine(msg.ValueString));
    /// </example>
    public static IObservable<ReceivedMessage> AsObservable(this ReceiveBuilder builder)
        => new ReceiveObservable(builder);

    // ═══════════════════════════════════════════════════════════════
    // ISubject - Bidirectional reactive stream
    // ═══════════════════════════════════════════════════════════════

    /// <summary>
    /// Create a reactive subject for a topic that supports both publishing and subscribing.
    /// </summary>
    /// <example>
    /// var subject = client.Messaging.CreateSubject&lt;string&gt;("events");
    ///
    /// // Subscribe to incoming messages
    /// subject.Subscribe(msg => Console.WriteLine($"Received: {msg}"));
    ///
    /// // Publish messages
    /// subject.OnNext("Hello, World!");
    /// </example>
    public static SurgewaveSubject<T> CreateSubject<T>(this SurgewaveMessagingOperations messaging, string topic, int partition = 0)
        => new(messaging, topic, partition);

    /// <summary>
    /// Create a reactive subject for byte[] messages.
    /// </summary>
    public static SurgewaveSubject<byte[]> CreateSubject(this SurgewaveMessagingOperations messaging, string topic, int partition = 0)
        => new(messaging, topic, partition);

    // ═══════════════════════════════════════════════════════════════
    // Channel-based streaming
    // ═══════════════════════════════════════════════════════════════

    /// <summary>
    /// Get messages as a Channel for producer-consumer patterns.
    /// </summary>
    public static ChannelReader<ReceivedMessage> AsChannelReader(
        this ReceiveBuilder builder,
        int capacity = 1000,
        CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateBounded<ReceivedMessage>(capacity);
        _ = PumpToChannelAsync(builder, channel.Writer, cancellationToken);
        return channel.Reader;
    }

    private static async Task PumpToChannelAsync(
        ReceiveBuilder builder,
        ChannelWriter<ReceivedMessage> writer,
        CancellationToken ct)
    {
        try
        {
            await foreach (var msg in builder.Stream(ct))
                await writer.WriteAsync(msg, ct);
        }
        catch (OperationCanceledException) { }
        finally
        {
            writer.Complete();
        }
    }
}

/// <summary>
/// IObservable implementation for ReceiveBuilder.
/// </summary>
public sealed class ReceiveObservable : IObservable<ReceivedMessage>, IDisposable
{
    private readonly ReceiveBuilder _builder;
    private readonly List<IObserver<ReceivedMessage>> _observers = [];
    private readonly object _lock = new();
    private CancellationTokenSource? _cts;
    private Task? _receiveTask;
    private bool _disposed;

    internal ReceiveObservable(ReceiveBuilder builder)
    {
        _builder = builder;
    }

    public IDisposable Subscribe(IObserver<ReceivedMessage> observer)
    {
        lock (_lock)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);

            _observers.Add(observer);
            if (_receiveTask == null)
            {
                _cts = new CancellationTokenSource();
                _receiveTask = StartReceivingAsync(_cts.Token);
            }
        }
        return new Subscription(this, observer);
    }

    public void Dispose()
    {
        lock (_lock)
        {
            if (_disposed) return;
            _disposed = true;
            _cts?.Cancel();
            _cts?.Dispose();
            _cts = null;
        }
    }

    private async Task StartReceivingAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var msg in _builder.Stream(ct))
            {
                List<IObserver<ReceivedMessage>> observers;
                lock (_lock)
                    observers = [.. _observers];

                foreach (var observer in observers)
                {
                    try { observer.OnNext(msg); }
                    catch { /* Observer threw - continue */ }
                }
            }

            // Completed
            List<IObserver<ReceivedMessage>> finalObservers;
            lock (_lock)
                finalObservers = [.. _observers];

            foreach (var observer in finalObservers)
            {
                try { observer.OnCompleted(); }
                catch { }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal cancellation
        }
        catch (Exception ex)
        {
            List<IObserver<ReceivedMessage>> observers;
            lock (_lock)
                observers = [.. _observers];

            foreach (var observer in observers)
            {
                try { observer.OnError(ex); }
                catch { }
            }
        }
    }

    private void Unsubscribe(IObserver<ReceivedMessage> observer)
    {
        lock (_lock)
        {
            _observers.Remove(observer);
            if (_observers.Count == 0)
            {
                _cts?.Cancel();
                _cts?.Dispose();
                _cts = null;
                _receiveTask = null;
            }
        }
    }

    private sealed class Subscription(ReceiveObservable observable, IObserver<ReceivedMessage> observer) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            observable.Unsubscribe(observer);
        }
    }
}

/// <summary>
/// Reactive subject for Surgewave topics.
/// Implements IObservable for subscribing to messages and IObserver for publishing.
/// </summary>
/// <typeparam name="T">Message type (typically string or byte[]).</typeparam>
public sealed class SurgewaveSubject<T> : IObservable<T>, IObserver<T>, IDisposable
{
    private readonly SurgewaveMessagingOperations _messaging;
    private readonly string _topic;
    private readonly int _partition;
    private readonly List<IObserver<T>> _observers = [];
    private readonly object _lock = new();
    private readonly Channel<T> _outboundChannel;
    private CancellationTokenSource? _cts;
    private Task? _receiveTask;
    private Task? _sendTask;
    private bool _disposed;
    private bool _completed;
    private Exception? _error;

    internal SurgewaveSubject(SurgewaveMessagingOperations messaging, string topic, int partition)
    {
        _messaging = messaging;
        _topic = topic;
        _partition = partition;
        _outboundChannel = Channel.CreateUnbounded<T>();
    }

    /// <summary>
    /// Subscribe to receive messages from this topic.
    /// </summary>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        lock (_lock)
        {
            if (_error != null)
            {
                observer.OnError(_error);
                return new EmptyDisposable();
            }
            if (_completed)
            {
                observer.OnCompleted();
                return new EmptyDisposable();
            }

            _observers.Add(observer);
            if (_receiveTask == null && !_disposed)
            {
                _cts = new CancellationTokenSource();
                _receiveTask = StartReceivingAsync(_cts.Token);
                _sendTask = StartSendingAsync(_cts.Token);
            }
        }
        return new Subscription(this, observer);
    }

    /// <summary>
    /// Publish a message to this topic.
    /// </summary>
    public void OnNext(T value)
    {
        if (_disposed || _completed) return;
        _outboundChannel.Writer.TryWrite(value);
    }

    /// <summary>
    /// Signal that no more messages will be published.
    /// </summary>
    public void OnCompleted()
    {
        lock (_lock)
        {
            if (_completed) return;
            _completed = true;
            _outboundChannel.Writer.Complete();

            foreach (var observer in _observers)
            {
                try { observer.OnCompleted(); }
                catch { }
            }
        }
    }

    /// <summary>
    /// Signal an error.
    /// </summary>
    public void OnError(Exception error)
    {
        lock (_lock)
        {
            if (_completed) return;
            _completed = true;
            _error = error;
            _outboundChannel.Writer.Complete(error);

            foreach (var observer in _observers)
            {
                try { observer.OnError(error); }
                catch { }
            }
        }
    }

    private async Task StartReceivingAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var msg in _messaging.Receive(_topic).FromPartition(_partition).FromEnd().Stream(ct))
            {
                var value = DeserializeMessage(msg);

                List<IObserver<T>> observers;
                lock (_lock)
                    observers = [.. _observers];

                foreach (var observer in observers)
                {
                    try { observer.OnNext(value); }
                    catch { }
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            OnError(ex);
        }
    }

    private async Task StartSendingAsync(CancellationToken ct)
    {
        try
        {
            await foreach (var value in _outboundChannel.Reader.ReadAllAsync(ct))
            {
                var bytes = SerializeMessage(value);
                await _messaging.SendAsync(_topic, _partition, null, bytes, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            OnError(ex);
        }
    }

    private static T DeserializeMessage(ReceivedMessage msg)
    {
        var type = typeof(T);
        if (type == typeof(byte[]))
            return (T)(object)msg.Value;
        if (type == typeof(string))
            return (T)(object)System.Text.Encoding.UTF8.GetString(msg.Value);

        // JSON fallback
        return System.Text.Json.JsonSerializer.Deserialize<T>(msg.Value)!;
    }

    private static byte[] SerializeMessage(T value)
    {
        if (value is byte[] bytes)
            return bytes;
        if (value is string str)
            return System.Text.Encoding.UTF8.GetBytes(str);

        // JSON fallback
        return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(value);
    }

    private void Unsubscribe(IObserver<T> observer)
    {
        lock (_lock)
        {
            _observers.Remove(observer);
            if (_observers.Count == 0 && _receiveTask != null)
            {
                _cts?.Cancel();
            }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _cts?.Cancel();
        _cts?.Dispose();
        _outboundChannel.Writer.TryComplete();

        lock (_lock)
        {
            foreach (var observer in _observers)
            {
                try { observer.OnCompleted(); }
                catch { }
            }
            _observers.Clear();
        }
    }

    private sealed class Subscription(SurgewaveSubject<T> subject, IObserver<T> observer) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            subject.Unsubscribe(observer);
        }
    }

    private sealed class EmptyDisposable : IDisposable
    {
        public void Dispose() { }
    }
}

/// <summary>
/// Simple observer implementation for use with IObservable.
/// </summary>
public sealed class ActionObserver<T> : IObserver<T>
{
    private readonly Action<T>? _onNext;
    private readonly Action<Exception>? _onError;
    private readonly Action? _onCompleted;

    public ActionObserver(
        Action<T>? onNext = null,
        Action<Exception>? onError = null,
        Action? onCompleted = null)
    {
        _onNext = onNext;
        _onError = onError;
        _onCompleted = onCompleted;
    }

    public void OnNext(T value) => _onNext?.Invoke(value);
    public void OnError(Exception error) => _onError?.Invoke(error);
    public void OnCompleted() => _onCompleted?.Invoke();
}

/// <summary>
/// Extension methods for subscribing to observables with action delegates.
/// </summary>
public static class ObserverExtensions
{
    /// <summary>
    /// Subscribe to an observable with action delegates.
    /// </summary>
    public static IDisposable Subscribe<T>(
        this IObservable<T> observable,
        Action<T> onNext,
        Action<Exception>? onError = null,
        Action? onCompleted = null)
    {
        return observable.Subscribe(new ActionObserver<T>(onNext, onError, onCompleted));
    }
}
