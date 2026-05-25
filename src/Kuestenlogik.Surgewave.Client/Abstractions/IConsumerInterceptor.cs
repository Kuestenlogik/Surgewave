using Kuestenlogik.Surgewave.Client.Consumer;

namespace Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Interface for consumer interceptors.
/// Interceptors allow custom logic after consuming messages and after committing offsets.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public interface IConsumerInterceptor<TKey, TValue>
{
    /// <summary>
    /// Called after a message has been consumed.
    /// Can modify the result or perform side effects.
    /// </summary>
    /// <param name="result">The consume result.</param>
    /// <returns>The result to return to the consumer (can be the same or modified).</returns>
    ConsumeResult<TKey, TValue> OnConsume(ConsumeResult<TKey, TValue> result);

    /// <summary>
    /// Called after offsets have been committed.
    /// </summary>
    /// <param name="offsets">The committed offsets.</param>
    void OnCommit(IEnumerable<TopicPartitionOffset> offsets);

    /// <summary>
    /// Called when the consumer is being closed.
    /// Use this to clean up any resources held by the interceptor.
    /// </summary>
    void Close();
}
