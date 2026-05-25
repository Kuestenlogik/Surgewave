using Kuestenlogik.Surgewave.Core.Monitoring;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Adapter that wraps OffsetStore and ConsumerGroupCoordinator to provide offset data for lag calculation.
/// </summary>
public sealed class OffsetStoreProvider : IOffsetProvider
{
    private readonly OffsetStore _offsetStore;
    private readonly Func<IEnumerable<(string GroupId, string State, int MemberCount)>> _getGroupInfo;

    /// <summary>
    /// Creates an OffsetStoreProvider.
    /// </summary>
    /// <param name="offsetStore">The offset store for committed offsets.</param>
    /// <param name="getGroupInfo">Function to get consumer group info (groupId, state, memberCount).</param>
    public OffsetStoreProvider(
        OffsetStore offsetStore,
        Func<IEnumerable<(string GroupId, string State, int MemberCount)>> getGroupInfo)
    {
        _offsetStore = offsetStore;
        _getGroupInfo = getGroupInfo;
    }

    public Dictionary<string, long> GetCommittedOffsets(string groupId)
    {
        return _offsetStore.GetAllOffsets(groupId);
    }

    public IEnumerable<string> GetGroupIds()
    {
        return _getGroupInfo().Select(g => g.GroupId);
    }

    public string GetGroupState(string groupId)
    {
        var group = _getGroupInfo().FirstOrDefault(g => g.GroupId == groupId);
        return group.State ?? "Unknown";
    }

    public int GetMemberCount(string groupId)
    {
        var group = _getGroupInfo().FirstOrDefault(g => g.GroupId == groupId);
        return group.MemberCount;
    }
}
