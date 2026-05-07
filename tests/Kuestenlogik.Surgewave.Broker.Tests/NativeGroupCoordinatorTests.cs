using Kuestenlogik.Surgewave.Broker.Native;
using Kuestenlogik.Surgewave.Broker.Native.Coordination;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Unit tests for NativeGroupCoordinator - isolated tests without broker startup.
/// Tests consumer group lifecycle, membership management, and offset tracking.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class NativeGroupCoordinatorTests
{
    private readonly NativeGroupCoordinator _coordinator;

    public NativeGroupCoordinatorTests()
    {
        _coordinator = new NativeGroupCoordinator(NullLogger<NativeGroupCoordinator>.Instance);
    }

    #region JoinGroup Tests

    [Fact]
    public void JoinGroup_NewGroup_CreatesGroupAndAssignsMember()
    {
        var groupId = "test-group";
        var protocols = new List<GroupProtocol> { new("range", Array.Empty<byte>()) };

        var result = _coordinator.JoinGroup(
            groupId,
            memberId: null,
            groupInstanceId: null,
            clientId: "test-client",
            protocolType: "consumer",
            sessionTimeoutMs: 10000,
            rebalanceTimeoutMs: 30000,
            protocols);

        Assert.Equal(0, result.ErrorCode);
        Assert.Equal(1, result.GenerationId);
        Assert.Equal("range", result.ProtocolName);
        Assert.NotEmpty(result.MemberId);
        Assert.StartsWith("test-client-", result.MemberId);
        Assert.Equal(result.MemberId, result.LeaderId); // First member is leader
        Assert.Single(result.Members); // Leader gets member list
    }

    [Fact]
    public void JoinGroup_ExistingGroup_AddsMemberAndTriggersRebalance()
    {
        var groupId = "test-group-2";
        var protocols = new List<GroupProtocol> { new("range", Array.Empty<byte>()) };

        // First member joins
        var result1 = _coordinator.JoinGroup(
            groupId, null, null, "client-1", "consumer", 10000, 30000, protocols);

        // Second member joins
        var result2 = _coordinator.JoinGroup(
            groupId, null, null, "client-2", "consumer", 10000, 30000, protocols);

        Assert.Equal(0, result1.ErrorCode);
        Assert.Equal(0, result2.ErrorCode);
        Assert.NotEqual(result1.MemberId, result2.MemberId);
        Assert.Equal(result1.MemberId, result1.LeaderId); // First member is leader
        Assert.Equal(result1.MemberId, result2.LeaderId); // Same leader for second
        Assert.Single(result1.Members); // Only leader gets member list
        Assert.Empty(result2.Members); // Non-leader gets empty list
    }

    [Fact]
    public void JoinGroup_WithExistingMemberId_UpdatesMember()
    {
        var groupId = "test-group-3";
        var protocols = new List<GroupProtocol> { new("range", Array.Empty<byte>()) };

        // First join
        var result1 = _coordinator.JoinGroup(
            groupId, null, null, "client-1", "consumer", 10000, 30000, protocols);

        // Rejoin with same member ID
        var result2 = _coordinator.JoinGroup(
            groupId, result1.MemberId, null, "client-1", "consumer", 10000, 30000, protocols);

        Assert.Equal(0, result2.ErrorCode);
        Assert.Equal(result1.MemberId, result2.MemberId);
        Assert.Equal(result1.GenerationId, result2.GenerationId); // Same generation for rejoin
    }

    [Fact]
    public void JoinGroup_WithStaticMembership_UsesGroupInstanceId()
    {
        var groupId = "test-group-static";
        var instanceId = "static-instance-1";
        var protocols = new List<GroupProtocol> { new("range", Array.Empty<byte>()) };

        var result = _coordinator.JoinGroup(
            groupId, null, instanceId, "client-1", "consumer", 10000, 30000, protocols);

        Assert.Equal(0, result.ErrorCode);
        Assert.NotEmpty(result.MemberId);
    }

    #endregion

    #region SyncGroup Tests

    [Fact]
    public void SyncGroup_LeaderWithAssignments_StoresAssignments()
    {
        var groupId = "sync-group-1";
        var protocols = new List<GroupProtocol> { new("range", Array.Empty<byte>()) };

        // Join
        var joinResult = _coordinator.JoinGroup(
            groupId, null, null, "client-1", "consumer", 10000, 30000, protocols);

        // Sync with assignments
        var assignment = new byte[] { 1, 2, 3, 4 };
        var assignments = new List<MemberAssignment>
        {
            new(joinResult.MemberId, assignment)
        };

        var syncResult = _coordinator.SyncGroup(
            groupId, joinResult.MemberId, joinResult.GenerationId, assignments);

        Assert.Equal(0, syncResult.ErrorCode);
        Assert.Equal(assignment, syncResult.Assignment);
    }

    [Fact]
    public void SyncGroup_UnknownGroup_ReturnsError()
    {
        var result = _coordinator.SyncGroup(
            "unknown-group", "member-1", 1, new List<MemberAssignment>());

        Assert.Equal(10, result.ErrorCode); // GroupNotFound
    }

    [Fact]
    public void SyncGroup_UnknownMember_ReturnsError()
    {
        var groupId = "sync-group-2";
        var protocols = new List<GroupProtocol> { new("range", Array.Empty<byte>()) };

        _coordinator.JoinGroup(
            groupId, null, null, "client-1", "consumer", 10000, 30000, protocols);

        var result = _coordinator.SyncGroup(
            groupId, "unknown-member", 1, new List<MemberAssignment>());

        Assert.Equal(15, result.ErrorCode); // UnknownMemberId
    }

    #endregion

    #region Heartbeat Tests

    [Fact]
    public void Heartbeat_ValidMember_ReturnsSuccess()
    {
        var groupId = "heartbeat-group-1";
        var protocols = new List<GroupProtocol> { new("range", Array.Empty<byte>()) };

        var joinResult = _coordinator.JoinGroup(
            groupId, null, null, "client-1", "consumer", 10000, 30000, protocols);

        var result = _coordinator.Heartbeat(groupId, joinResult.MemberId, joinResult.GenerationId);

        Assert.Equal(0, result.ErrorCode);
    }

    [Fact]
    public void Heartbeat_UnknownGroup_ReturnsError()
    {
        var result = _coordinator.Heartbeat("unknown-group", "member-1", 1);

        Assert.Equal(10, result.ErrorCode); // GroupNotFound
    }

    [Fact]
    public void Heartbeat_UnknownMember_ReturnsError()
    {
        var groupId = "heartbeat-group-2";
        var protocols = new List<GroupProtocol> { new("range", Array.Empty<byte>()) };

        _coordinator.JoinGroup(
            groupId, null, null, "client-1", "consumer", 10000, 30000, protocols);

        var result = _coordinator.Heartbeat(groupId, "unknown-member", 1);

        Assert.Equal(15, result.ErrorCode); // UnknownMemberId
    }

    [Fact]
    public void Heartbeat_WrongGeneration_ReturnsError()
    {
        var groupId = "heartbeat-group-3";
        var protocols = new List<GroupProtocol> { new("range", Array.Empty<byte>()) };

        var joinResult = _coordinator.JoinGroup(
            groupId, null, null, "client-1", "consumer", 10000, 30000, protocols);

        var result = _coordinator.Heartbeat(groupId, joinResult.MemberId, joinResult.GenerationId + 1);

        Assert.Equal(16, result.ErrorCode); // IllegalGeneration
    }

    #endregion

    #region LeaveGroup Tests

    [Fact]
    public void LeaveGroup_ValidMember_RemovesMember()
    {
        var groupId = "leave-group-1";
        var protocols = new List<GroupProtocol> { new("range", Array.Empty<byte>()) };

        var joinResult = _coordinator.JoinGroup(
            groupId, null, null, "client-1", "consumer", 10000, 30000, protocols);

        var result = _coordinator.LeaveGroup(groupId, joinResult.MemberId);

        Assert.Equal(0, result.ErrorCode);

        // Verify member is removed by checking heartbeat fails
        var heartbeatResult = _coordinator.Heartbeat(groupId, joinResult.MemberId, joinResult.GenerationId);
        Assert.Equal(15, heartbeatResult.ErrorCode); // UnknownMemberId
    }

    [Fact]
    public void LeaveGroup_UnknownGroup_ReturnsError()
    {
        var result = _coordinator.LeaveGroup("unknown-group", "member-1");

        Assert.Equal(10, result.ErrorCode); // GroupNotFound
    }

    #endregion

    #region ListGroups / DescribeGroup Tests

    [Fact]
    public void ListGroups_ReturnsAllGroups()
    {
        var coordinator = new NativeGroupCoordinator(NullLogger<NativeGroupCoordinator>.Instance);
        var protocols = new List<GroupProtocol> { new("range", Array.Empty<byte>()) };

        coordinator.JoinGroup("group-a", null, null, "client-1", "consumer", 10000, 30000, protocols);
        coordinator.JoinGroup("group-b", null, null, "client-2", "consumer", 10000, 30000, protocols);

        var groups = coordinator.ListGroups();

        Assert.Equal(2, groups.Count);
        Assert.Contains(groups, g => g.GroupId == "group-a");
        Assert.Contains(groups, g => g.GroupId == "group-b");
    }

    [Fact]
    public void DescribeGroup_ReturnsGroupDetails()
    {
        var coordinator = new NativeGroupCoordinator(NullLogger<NativeGroupCoordinator>.Instance);
        var protocols = new List<GroupProtocol> { new("range", new byte[] { 1, 2, 3 }) };

        coordinator.JoinGroup("describe-group", null, null, "client-1", "consumer", 10000, 30000, protocols);

        var result = coordinator.DescribeGroup("describe-group");

        Assert.Equal(0, result.ErrorCode);
        Assert.Equal("describe-group", result.GroupId);
        Assert.Equal("consumer", result.ProtocolType);
        Assert.Equal("range", result.ProtocolName);
        Assert.Single(result.Members);
    }

    [Fact]
    public void DescribeGroup_UnknownGroup_ReturnsError()
    {
        var result = _coordinator.DescribeGroup("unknown-group");

        Assert.Equal(10, result.ErrorCode); // GroupNotFound
    }

    #endregion

    #region DeleteGroup Tests

    [Fact]
    public void DeleteGroup_EmptyGroup_DeletesSuccessfully()
    {
        var coordinator = new NativeGroupCoordinator(NullLogger<NativeGroupCoordinator>.Instance);
        var protocols = new List<GroupProtocol> { new("range", Array.Empty<byte>()) };

        var joinResult = coordinator.JoinGroup("delete-group", null, null, "client-1", "consumer", 10000, 30000, protocols);
        coordinator.LeaveGroup("delete-group", joinResult.MemberId);

        var result = coordinator.DeleteGroup("delete-group");

        Assert.Equal(0, result.ErrorCode);

        // Verify group is deleted
        var describeResult = coordinator.DescribeGroup("delete-group");
        Assert.Equal(10, describeResult.ErrorCode); // GroupNotFound
    }

    [Fact]
    public void DeleteGroup_NonEmptyGroup_ReturnsError()
    {
        var coordinator = new NativeGroupCoordinator(NullLogger<NativeGroupCoordinator>.Instance);
        var protocols = new List<GroupProtocol> { new("range", Array.Empty<byte>()) };

        coordinator.JoinGroup("delete-group-2", null, null, "client-1", "consumer", 10000, 30000, protocols);

        var result = coordinator.DeleteGroup("delete-group-2");

        Assert.Equal(18, result.ErrorCode); // GroupNotEmpty
    }

    [Fact]
    public void DeleteGroup_UnknownGroup_ReturnsError()
    {
        var result = _coordinator.DeleteGroup("unknown-group");

        Assert.Equal(10, result.ErrorCode); // GroupNotFound
    }

    #endregion

    #region Offset Management Tests

    [Fact]
    public void CommitOffset_StoresOffset()
    {
        var groupId = "offset-group-1";
        var topic = "test-topic";
        var partition = 0;
        var offset = 100L;

        var result = _coordinator.CommitOffset(
            groupId, "member-1", 1, topic, partition, offset, null);

        Assert.Equal(0, result.ErrorCode);
    }

    [Fact]
    public void FetchOffset_ReturnsCommittedOffset()
    {
        var groupId = "offset-group-2";
        var topic = "test-topic";
        var partition = 0;
        var offset = 200L;

        _coordinator.CommitOffset(groupId, "member-1", 1, topic, partition, offset, null);

        var result = _coordinator.FetchOffset(groupId, topic, partition);

        Assert.Equal(0, result.ErrorCode);
        Assert.Equal(offset, result.Offset);
    }

    [Fact]
    public void FetchOffset_UnknownOffset_ReturnsMinusOne()
    {
        var result = _coordinator.FetchOffset("unknown-group", "unknown-topic", 0);

        Assert.Equal(0, result.ErrorCode);
        Assert.Equal(-1, result.Offset);
    }

    [Fact]
    public void CommitOffset_AutoCreatesGroup()
    {
        var groupId = "auto-create-group";
        var topic = "test-topic";

        // Commit to non-existent group - should auto-create
        var result = _coordinator.CommitOffset(groupId, "member-1", 0, topic, 0, 50L, null);

        Assert.Equal(0, result.ErrorCode);

        // Verify offset is stored
        var fetchResult = _coordinator.FetchOffset(groupId, topic, 0);
        Assert.Equal(50L, fetchResult.Offset);
    }

    #endregion

    #region FindCoordinator Tests

    [Fact]
    public void FindCoordinator_ReturnsSelf()
    {
        var result = _coordinator.FindCoordinator("test-group", 0);

        Assert.Equal(0, result.ErrorCode);
        Assert.Equal(0, result.CoordinatorId);
        Assert.Equal("localhost", result.Host);
        Assert.Equal(9092, result.Port);
    }

    #endregion
}
