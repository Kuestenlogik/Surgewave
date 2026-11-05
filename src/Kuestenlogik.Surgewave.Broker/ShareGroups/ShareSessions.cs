using System.Collections.Immutable;
using Kuestenlogik.Surgewave.Coordination.ShareGroups;

namespace Kuestenlogik.Surgewave.Broker.ShareGroups;

/// <summary>
/// The share sessions of the members fetching from this broker (KIP-932, #228): a member opens one with epoch 0 and the
/// partitions it fetches; each later ShareFetch or ShareAcknowledge carries the next epoch and only the partitions that
/// join or leave, and epoch -1 closes it. The partitions of a session are the ones this broker serves the member.
/// </summary>
/// <remarks>
/// A session nobody used for <see cref="IdleTimeout"/> is dropped — a member that went away does not say so to every
/// leader; one that comes back gets SHARE_SESSION_NOT_FOUND and opens a new session.
/// </remarks>
internal sealed class ShareSessions(TimeSpan idleTimeout, TimeProvider? timeProvider = null)
{
    private readonly TimeProvider _time = timeProvider ?? TimeProvider.System;
    private readonly Dictionary<(string GroupId, string MemberId), Session> _sessions = [];
    private readonly Lock _lock = new();

    /// <summary>The epoch that opens a session.</summary>
    public const int InitialEpoch = 0;

    /// <summary>The epoch that closes a session.</summary>
    public const int FinalEpoch = -1;

    /// <summary>How long a session is kept without being used.</summary>
    public TimeSpan IdleTimeout => idleTimeout;

    /// <summary>
    /// A ShareFetch of the session: the partitions to serve, or why the epoch does not fit the session. The final epoch
    /// closes the session and serves nothing but the acknowledgements the request carries.
    /// </summary>
    public (ShareGroupErrorStatus Status, IReadOnlyCollection<(Guid TopicId, int Partition)> Partitions) Fetch(
        string groupId, string memberId, int epoch,
        IEnumerable<(Guid TopicId, int Partition)> added, IEnumerable<(Guid TopicId, int Partition)> forgotten)
    {
        var key = (groupId, memberId);
        lock (_lock)
        {
            switch (epoch)
            {
                case InitialEpoch:
                {
                    var partitions = added.ToImmutableHashSet();
                    _sessions[key] = new Session(NextOf(InitialEpoch), partitions, _time.GetUtcNow());
                    return (ShareGroupErrorStatus.None, partitions);
                }
                case FinalEpoch:
                    return (_sessions.Remove(key) ? ShareGroupErrorStatus.None : ShareGroupErrorStatus.ShareSessionNotFound, []);
                default:
                {
                    if (Expected(key, epoch) is { } refused)
                        return (refused, []);
                    var session = _sessions[key];
                    var partitions = session.Partitions.Union(added).Except(forgotten);
                    _sessions[key] = new Session(NextOf(epoch), partitions, _time.GetUtcNow());
                    return (ShareGroupErrorStatus.None, partitions);
                }
            }
        }
    }

    /// <summary>
    /// A ShareAcknowledge of the session: it moves the session to its next epoch, or closes it with the final epoch. A
    /// session is opened by a ShareFetch only.
    /// </summary>
    public ShareGroupErrorStatus Acknowledge(string groupId, string memberId, int epoch)
    {
        var key = (groupId, memberId);
        lock (_lock)
        {
            switch (epoch)
            {
                case InitialEpoch:
                    return ShareGroupErrorStatus.InvalidShareSessionEpoch;
                case FinalEpoch:
                    return _sessions.Remove(key) ? ShareGroupErrorStatus.None : ShareGroupErrorStatus.ShareSessionNotFound;
                default:
                {
                    if (Expected(key, epoch) is { } refused)
                        return refused;
                    _sessions[key] = _sessions[key] with { NextEpoch = NextOf(epoch), LastUsed = _time.GetUtcNow() };
                    return ShareGroupErrorStatus.None;
                }
            }
        }
    }

    /// <summary>Drops the sessions nobody used for the idle timeout.</summary>
    public void DropIdle()
    {
        var cutoff = _time.GetUtcNow() - idleTimeout;
        lock (_lock)
        {
            foreach (var key in _sessions.Where(s => s.Value.LastUsed < cutoff).Select(s => s.Key).ToList())
                _sessions.Remove(key);
        }
    }

    // Under the lock: why the epoch does not continue the session, null when it does.
    private ShareGroupErrorStatus? Expected((string, string) key, int epoch) =>
        !_sessions.TryGetValue(key, out var session) ? ShareGroupErrorStatus.ShareSessionNotFound
        : session.NextEpoch != epoch ? ShareGroupErrorStatus.InvalidShareSessionEpoch
        : null;

    // Kafka's ShareRequestMetadata.nextEpoch: after the largest epoch comes 1 again, never the initial or final one.
    private static int NextOf(int epoch) => epoch == int.MaxValue ? 1 : epoch + 1;

    private sealed record Session(int NextEpoch, ImmutableHashSet<(Guid TopicId, int Partition)> Partitions, DateTimeOffset LastUsed);
}
