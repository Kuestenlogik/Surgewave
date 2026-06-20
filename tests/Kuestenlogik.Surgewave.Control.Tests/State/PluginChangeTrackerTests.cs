using Kuestenlogik.Surgewave.Control.State;

namespace Kuestenlogik.Surgewave.Control.Tests.State;

/// <summary>
/// Covers <see cref="PluginChangeTracker"/> — the per-circuit state that
/// drives the restart-required banner. Logic is tiny but load-bearing for
/// the UX: a missed StateChanged event would leave the banner stale.
/// </summary>
public sealed class PluginChangeTrackerTests
{
    [Fact]
    public void Initial_PendingRestart_IsFalse_AndChangedIsEmpty()
    {
        var t = new PluginChangeTracker();
        Assert.False(t.PendingRestart);
        Assert.Empty(t.ChangedPackages);
    }

    [Fact]
    public void MarkChanged_FlipsPending_AndRaisesEventOnce()
    {
        var t = new PluginChangeTracker();
        var raises = 0;
        t.StateChanged += () => raises++;

        t.MarkChanged("acme.connector.s3");

        Assert.True(t.PendingRestart);
        Assert.Equal(["acme.connector.s3"], t.ChangedPackages);
        Assert.Equal(1, raises);
    }

    [Fact]
    public void MarkChanged_SamePackageTwice_DoesNotDoubleRaise()
    {
        var t = new PluginChangeTracker();
        var raises = 0;
        t.StateChanged += () => raises++;

        t.MarkChanged("acme.s3");
        t.MarkChanged("acme.s3");

        Assert.Single(t.ChangedPackages);
        Assert.Equal(1, raises);
    }

    [Fact]
    public void MarkChanged_DifferentCaseSameId_IsDeduped()
    {
        var t = new PluginChangeTracker();
        t.MarkChanged("Acme.S3");
        t.MarkChanged("acme.s3");
        Assert.Single(t.ChangedPackages);
    }

    [Fact]
    public void Acknowledge_ResetsState_AndRaisesEvent()
    {
        var t = new PluginChangeTracker();
        t.MarkChanged("acme.s3");
        t.MarkChanged("acme.kafka");

        var raises = 0;
        t.StateChanged += () => raises++;

        t.Acknowledge();

        Assert.False(t.PendingRestart);
        Assert.Empty(t.ChangedPackages);
        Assert.Equal(1, raises);
    }

    [Fact]
    public void Acknowledge_WhenAlreadyEmpty_IsNoop()
    {
        var t = new PluginChangeTracker();
        var raises = 0;
        t.StateChanged += () => raises++;

        t.Acknowledge();

        Assert.Equal(0, raises);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void MarkChanged_InvalidId_Throws(string? id)
    {
        var t = new PluginChangeTracker();
        Assert.ThrowsAny<ArgumentException>(() => t.MarkChanged(id!));
    }
}
