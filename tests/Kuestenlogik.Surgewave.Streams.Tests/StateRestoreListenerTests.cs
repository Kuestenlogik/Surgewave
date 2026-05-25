using Kuestenlogik.Surgewave.Streams;
using Xunit;

namespace Kuestenlogik.Surgewave.Streams.Tests;

public sealed class StateRestoreListenerTests
{
    private readonly ITestOutputHelper _output;

    public StateRestoreListenerTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void DelegateListener_OnRestoreStart_CallsDelegate()
    {
        var started = false;
        string? storeName = null;

        var listener = new DelegateStateRestoreListener(
            onStart: ctx =>
            {
                started = true;
                storeName = ctx.StoreName;
            });

        var context = new StateRestoreContext
        {
            StoreName = "my-store",
            Topic = "changelog-topic",
            Partition = 0,
            StartingOffset = 0,
            EndingOffset = 100
        };

        listener.OnRestoreStart(context);

        Assert.True(started);
        Assert.Equal("my-store", storeName);
    }

    [Fact]
    public void DelegateListener_OnBatchRestored_TracksProgress()
    {
        var batches = new List<int>();

        var listener = new DelegateStateRestoreListener(
            onBatch: (ctx, numRestored) =>
            {
                ctx.TotalRestored += numRestored;
                batches.Add(numRestored);
            });

        var context = new StateRestoreContext
        {
            StoreName = "counter-store",
            Topic = "changelog",
            Partition = 0,
            StartingOffset = 0,
            EndingOffset = 1000
        };

        listener.OnBatchRestored(context, 100);
        listener.OnBatchRestored(context, 200);
        listener.OnBatchRestored(context, 300);

        Assert.Equal(3, batches.Count);
        Assert.Equal(600, context.TotalRestored);
    }

    [Fact]
    public void DelegateListener_OnRestoreEnd_ReportsTotalRestored()
    {
        long totalRestored = 0;

        var listener = new DelegateStateRestoreListener(
            onEnd: (ctx, total) => totalRestored = total);

        var context = new StateRestoreContext
        {
            StoreName = "state-store",
            Topic = "changelog",
            Partition = 0,
            StartingOffset = 50,
            EndingOffset = 1050
        };

        listener.OnRestoreEnd(context, 1000);

        Assert.Equal(1000, totalRestored);
    }

    [Fact]
    public void NoOpListener_DoesNotThrow()
    {
        var listener = NoOpStateRestoreListener.Instance;
        var context = new StateRestoreContext
        {
            StoreName = "test",
            Topic = "topic",
            Partition = 0,
            StartingOffset = 0,
            EndingOffset = 0
        };

        listener.OnRestoreStart(context);
        listener.OnBatchRestored(context, 50);
        listener.OnRestoreEnd(context, 50);

        Assert.NotNull(listener);
    }

    [Fact]
    public void StateRestoreContext_Properties_SetCorrectly()
    {
        var context = new StateRestoreContext
        {
            StoreName = "kv-store",
            Topic = "app-changelog-kv-store",
            Partition = 3,
            StartingOffset = 42,
            EndingOffset = 1042
        };

        Assert.Equal("kv-store", context.StoreName);
        Assert.Equal("app-changelog-kv-store", context.Topic);
        Assert.Equal(3, context.Partition);
        Assert.Equal(42, context.StartingOffset);
        Assert.Equal(1042, context.EndingOffset);
        Assert.Equal(0, context.TotalRestored);
    }

    [Fact]
    public void StreamsConfig_StateRestoreListener_DefaultIsNoOp()
    {
        var config = new StreamsConfig
        {
            ApplicationId = "test",
            BootstrapServers = "localhost:9092"
        };

        Assert.IsType<NoOpStateRestoreListener>(config.StateRestoreListener);
    }
}
