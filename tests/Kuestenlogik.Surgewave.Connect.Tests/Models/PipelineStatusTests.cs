using Kuestenlogik.Surgewave.Connect.Pipelines;

namespace Kuestenlogik.Surgewave.Connect.Tests.Models;

/// <summary>
/// Tests for PipelineStatus enum and PipelineConnectionType enum.
/// </summary>
public sealed class PipelineStatusTests
{
    [Fact]
    public void PipelineStatus_AllValues_Exist()
    {
        var values = Enum.GetValues<PipelineStatus>();

        Assert.Contains(PipelineStatus.Draft, values);
        Assert.Contains(PipelineStatus.Running, values);
        Assert.Contains(PipelineStatus.Stopped, values);
        Assert.Contains(PipelineStatus.Failed, values);
        Assert.Equal(4, values.Length);
    }

    [Theory]
    [InlineData(PipelineStatus.Draft, "Draft")]
    [InlineData(PipelineStatus.Running, "Running")]
    [InlineData(PipelineStatus.Stopped, "Stopped")]
    [InlineData(PipelineStatus.Failed, "Failed")]
    public void PipelineStatus_ToString_ReturnsExpected(PipelineStatus status, string expected)
    {
        Assert.Equal(expected, status.ToString());
    }

    [Fact]
    public void PipelineConnectionType_AllValues_Exist()
    {
        var values = Enum.GetValues<PipelineConnectionType>();

        Assert.Contains(PipelineConnectionType.Normal, values);
        Assert.Contains(PipelineConnectionType.Error, values);
        Assert.Equal(2, values.Length);
    }

    [Theory]
    [InlineData(PipelineConnectionType.Normal, "Normal")]
    [InlineData(PipelineConnectionType.Error, "Error")]
    public void PipelineConnectionType_ToString_ReturnsExpected(PipelineConnectionType type, string expected)
    {
        Assert.Equal(expected, type.ToString());
    }

    [Fact]
    public void ConnectorStatus_AllValues_Exist()
    {
        var values = Enum.GetValues<ConnectorStatus>();

        Assert.Contains(ConnectorStatus.Unassigned, values);
        Assert.Contains(ConnectorStatus.Running, values);
        Assert.Contains(ConnectorStatus.Paused, values);
        Assert.Contains(ConnectorStatus.Failed, values);
        Assert.Equal(4, values.Length);
    }
}
