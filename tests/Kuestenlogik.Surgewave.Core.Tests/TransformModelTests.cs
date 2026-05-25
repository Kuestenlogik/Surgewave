using Kuestenlogik.Surgewave.Core.Transforms;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Tests for Transform models: TransformContext, TransformResult, TransformPhase.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TransformModelTests
{
    #region TransformPhase Tests

    [Fact]
    public void TransformPhase_HasProduceAndFetch()
    {
        var values = Enum.GetValues<TransformPhase>();
        Assert.Equal(2, values.Length);
        Assert.Contains(TransformPhase.Produce, values);
        Assert.Contains(TransformPhase.Fetch, values);
    }

    #endregion

    #region TransformContext Tests

    [Fact]
    public void TransformContext_Properties_SetCorrectly()
    {
        var key = new byte[] { 1, 2 };
        var value = new byte[] { 3, 4, 5 };
        var headers = new Dictionary<string, byte[]>
        {
            ["h1"] = [10]
        };

        var ctx = new TransformContext
        {
            Topic = "orders",
            Partition = 2,
            Key = key,
            Value = value,
            Headers = headers,
            Timestamp = 1234567890,
            Phase = TransformPhase.Produce
        };

        Assert.Equal("orders", ctx.Topic);
        Assert.Equal(2, ctx.Partition);
        Assert.Same(key, ctx.Key);
        Assert.Same(value, ctx.Value);
        Assert.Same(headers, ctx.Headers);
        Assert.Equal(1234567890, ctx.Timestamp);
        Assert.Equal(TransformPhase.Produce, ctx.Phase);
    }

    [Fact]
    public void TransformContext_DefaultHeaders_IsEmpty()
    {
        var ctx = new TransformContext
        {
            Topic = "t",
            Partition = 0,
            Key = [],
            Value = []
        };

        Assert.Empty(ctx.Headers);
    }

    #endregion

    #region TransformResult Tests

    [Fact]
    public void TransformResult_Pass_CreatesPassThroughResult()
    {
        var key = new byte[] { 1 };
        var value = new byte[] { 2, 3 };

        var result = TransformResult.Pass(key, value);

        Assert.False(result.Dropped);
        Assert.Same(key, result.Key);
        Assert.Same(value, result.Value);
        Assert.Null(result.Headers);
        Assert.Null(result.RouteTopic);
    }

    [Fact]
    public void TransformResult_Pass_WithHeaders_IncludesHeaders()
    {
        var headers = new Dictionary<string, byte[]> { ["x"] = [1] };
        var result = TransformResult.Pass([], [], headers);

        Assert.NotNull(result.Headers);
        Assert.Single(result.Headers);
    }

    [Fact]
    public void TransformResult_Drop_SetsDroppedFlag()
    {
        var result = TransformResult.Drop();

        Assert.True(result.Dropped);
        Assert.Null(result.RouteTopic);
    }

    [Fact]
    public void TransformResult_Route_SetsRouteTopicAndData()
    {
        var key = new byte[] { 1 };
        var value = new byte[] { 2 };

        var result = TransformResult.Route("dlq-topic", key, value);

        Assert.False(result.Dropped);
        Assert.Equal("dlq-topic", result.RouteTopic);
        Assert.Same(key, result.Key);
        Assert.Same(value, result.Value);
    }

    [Fact]
    public void TransformResult_Route_WithHeaders_IncludesHeaders()
    {
        var headers = new Dictionary<string, byte[]> { ["h"] = [42] };
        var result = TransformResult.Route("reroute", [], [], headers);

        Assert.Equal("reroute", result.RouteTopic);
        Assert.NotNull(result.Headers);
    }

    #endregion
}
