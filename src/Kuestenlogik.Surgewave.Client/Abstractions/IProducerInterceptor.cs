namespace Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Interface for producer interceptors.
/// Interceptors allow custom logic before sending messages and after receiving acknowledgments.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public interface IProducerInterceptor<TKey, TValue>
{
    /// <summary>
    /// Called before a message is sent.
    /// Can modify the record or perform side effects.
    /// </summary>
    /// <param name="record">The record about to be sent.</param>
    /// <returns>The record to actually send (can be the same or modified).</returns>
    ProducerRecord<TKey, TValue> OnSend(ProducerRecord<TKey, TValue> record);

    /// <summary>
    /// Called after a message has been acknowledged (or failed).
    /// </summary>
    /// <param name="result">The produce result (null if failed).</param>
    /// <param name="exception">The exception if the produce failed.</param>
    void OnAcknowledgement(ProduceResult? result, Exception? exception);

    /// <summary>
    /// Called when the producer is being closed.
    /// Use this to clean up any resources held by the interceptor.
    /// </summary>
    void Close();
}
