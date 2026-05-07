using Kuestenlogik.Surgewave.Storage.Tiering;
using Xunit;

namespace Kuestenlogik.Surgewave.Storage.Tiering.Tests;

/// <summary>
/// Tests for RemoteLogSegmentState lifecycle, transitions, and helper extensions.
/// </summary>
public class RemoteLogSegmentStateTests
{
    [Fact]
    public void CopySegmentStarted_CanTransitionTo_CopySegmentFinished()
    {
        Assert.True(RemoteLogSegmentState.CopySegmentStarted.CanTransitionTo(
            RemoteLogSegmentState.CopySegmentFinished));
    }

    [Fact]
    public void CopySegmentStarted_CanTransitionTo_DeleteSegmentStarted()
    {
        Assert.True(RemoteLogSegmentState.CopySegmentStarted.CanTransitionTo(
            RemoteLogSegmentState.DeleteSegmentStarted));
    }

    [Fact]
    public void CopySegmentStarted_CannotTransitionTo_DeleteSegmentFinished()
    {
        Assert.False(RemoteLogSegmentState.CopySegmentStarted.CanTransitionTo(
            RemoteLogSegmentState.DeleteSegmentFinished));
    }

    [Fact]
    public void CopySegmentFinished_CanTransitionTo_DeleteSegmentStarted()
    {
        Assert.True(RemoteLogSegmentState.CopySegmentFinished.CanTransitionTo(
            RemoteLogSegmentState.DeleteSegmentStarted));
    }

    [Fact]
    public void CopySegmentFinished_CannotTransitionTo_CopySegmentStarted()
    {
        Assert.False(RemoteLogSegmentState.CopySegmentFinished.CanTransitionTo(
            RemoteLogSegmentState.CopySegmentStarted));
    }

    [Fact]
    public void DeleteSegmentStarted_CanTransitionTo_DeleteSegmentFinished()
    {
        Assert.True(RemoteLogSegmentState.DeleteSegmentStarted.CanTransitionTo(
            RemoteLogSegmentState.DeleteSegmentFinished));
    }

    [Fact]
    public void DeleteSegmentFinished_IsTerminal_NoValidTransitions()
    {
        var transitions = RemoteLogSegmentState.DeleteSegmentFinished.ValidTransitions();
        Assert.Empty(transitions);
    }

    [Fact]
    public void SelfTransition_IsAlwaysAllowed()
    {
        foreach (var state in Enum.GetValues<RemoteLogSegmentState>())
        {
            Assert.True(state.CanTransitionTo(state),
                $"Self-transition should be allowed for {state}");
        }
    }

    [Fact]
    public void IsReadable_OnlyCopySegmentFinished_IsTrue()
    {
        Assert.False(RemoteLogSegmentState.CopySegmentStarted.IsReadable());
        Assert.True(RemoteLogSegmentState.CopySegmentFinished.IsReadable());
        Assert.False(RemoteLogSegmentState.DeleteSegmentStarted.IsReadable());
        Assert.False(RemoteLogSegmentState.DeleteSegmentFinished.IsReadable());
    }

    [Fact]
    public void IsVisibleForCleanup_AllExceptTerminalState()
    {
        Assert.True(RemoteLogSegmentState.CopySegmentStarted.IsVisibleForCleanup());
        Assert.True(RemoteLogSegmentState.CopySegmentFinished.IsVisibleForCleanup());
        Assert.True(RemoteLogSegmentState.DeleteSegmentStarted.IsVisibleForCleanup());
        Assert.False(RemoteLogSegmentState.DeleteSegmentFinished.IsVisibleForCleanup());
    }

    [Fact]
    public void ValidTransitions_CopySegmentStarted_HasTwoTargets()
    {
        var transitions = RemoteLogSegmentState.CopySegmentStarted.ValidTransitions();
        Assert.Equal(2, transitions.Length);
    }

    [Fact]
    public void ValidTransitions_CopySegmentFinished_HasOneTarget()
    {
        var transitions = RemoteLogSegmentState.CopySegmentFinished.ValidTransitions();
        Assert.Single(transitions);
    }

    [Fact]
    public void RemoteSegmentState_DefaultValues_AreCorrect()
    {
        var state = new RemoteSegmentState();

        Assert.NotEqual(Guid.Empty, state.SegmentId);
        Assert.Equal(0, state.BaseOffset);
        Assert.Equal(RemoteLogSegmentState.CopySegmentStarted, state.State);
        Assert.False(state.IsRemoteOnly);
        Assert.True(state.TransactionIndexEmpty);
        Assert.Null(state.CachePath);
        Assert.Null(state.CachedAt);
    }

    [Fact]
    public void RemoteSegmentState_IsReadable_ReflectsState()
    {
        var state = new RemoteSegmentState { State = RemoteLogSegmentState.CopySegmentFinished };
        Assert.True(state.IsReadable);

        state.State = RemoteLogSegmentState.CopySegmentStarted;
        Assert.False(state.IsReadable);
    }

    [Fact]
    public void RemoteIndexType_HasExpectedValues()
    {
        Assert.Equal(0, (int)RemoteIndexType.Offset);
        Assert.Equal(1, (int)RemoteIndexType.Timestamp);
        Assert.Equal(2, (int)RemoteIndexType.ProducerSnapshot);
        Assert.Equal(3, (int)RemoteIndexType.Transaction);
        Assert.Equal(4, (int)RemoteIndexType.LeaderEpoch);
    }

    [Fact]
    public void RemotePartitionDeleteState_HasExpectedValues()
    {
        Assert.Equal(0, (int)RemotePartitionDeleteState.DeletePartitionMarked);
        Assert.Equal(1, (int)RemotePartitionDeleteState.DeletePartitionStarted);
        Assert.Equal(2, (int)RemotePartitionDeleteState.DeletePartitionFinished);
    }
}
