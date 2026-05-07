using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Core.Queue;

/// <summary>
/// Factory and registry for <see cref="IQueueView"/> instances.
/// Returns an existing view for a given topic or creates one on demand.
/// </summary>
/// <remarks>
/// Protocol adapters (AMQP, etc.) inject this interface, while the concrete
/// implementation lives in the Broker project. This avoids circular project references.
/// </remarks>
public interface IQueueViewManager
{
    /// <summary>
    /// Gets or creates an <see cref="IQueueView"/> for the specified topic and partition log.
    /// Multiple callers requesting the same topic receive the same shared view.
    /// </summary>
    /// <param name="topic">The Surgewave topic name.</param>
    /// <param name="log">The partition log backing this view.</param>
    IQueueView GetOrCreate(string topic, IPartitionLog log);

    /// <summary>
    /// Returns the <see cref="IQueueView"/> for <paramref name="topic"/>, or <c>null</c> if none exists.
    /// </summary>
    IQueueView? Get(string topic);
}
