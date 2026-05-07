namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Plugin hook for topic-level lifecycle events (KIP-1010 Topic Hooks). Implementations
/// can react to creates, updates, and deletes — typically for governance, audit, or
/// catalog-sync purposes. The broker invokes every registered hook in registration
/// order, awaiting each. A hook MUST NOT throw; doing so logs the failure but does
/// not abort the topic operation, since the underlying log/metadata change has already
/// been applied.
/// </summary>
public interface ITopicLifecycleHook
{
    /// <summary>
    /// Called immediately after a topic and its partition logs have been created.
    /// </summary>
    Task OnTopicCreatedAsync(TopicLifecycleContext context, CancellationToken cancellationToken);

    /// <summary>
    /// Called immediately after a topic's configuration has been updated. The
    /// <see cref="TopicLifecycleContext.PreviousConfig"/> snapshot reflects what the
    /// config looked like before the change.
    /// </summary>
    Task OnTopicConfigChangedAsync(TopicLifecycleContext context, CancellationToken cancellationToken);

    /// <summary>
    /// Called immediately after a topic and its partition logs have been deleted.
    /// </summary>
    Task OnTopicDeletedAsync(TopicLifecycleContext context, CancellationToken cancellationToken);
}

/// <summary>
/// Snapshot passed to every hook invocation. Captures the topic identity plus the
/// pre/post state so hooks can reason about the delta without re-reading state from
/// the broker.
/// </summary>
public sealed record TopicLifecycleContext(
    string TopicName,
    Guid TopicId,
    int PartitionCount,
    short ReplicationFactor,
    IReadOnlyDictionary<string, string> Config,
    IReadOnlyDictionary<string, string>? PreviousConfig);
