namespace Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Tests.Types;

public class ErrorTests
{
    [Fact]
    public void NoError_HasCorrectCode()
    {
        var error = Error.NoError;
        Assert.Equal(ErrorCode.NoError, error.Code);
        Assert.False(error.IsError);
        Assert.False(error.IsFatal);
        Assert.False(error.IsBrokerError);
        Assert.False(error.IsLocalError);
    }

    [Fact]
    public void Constructor_WithCode_SetsProperties()
    {
        var error = new Error(ErrorCode.UnknownTopicOrPartition);
        Assert.Equal(ErrorCode.UnknownTopicOrPartition, error.Code);
        Assert.True(error.IsError);
    }

    [Fact]
    public void Constructor_WithReason_SetsReason()
    {
        var error = new Error(ErrorCode.Local_TimedOut, "Operation timed out");
        Assert.Equal("Operation timed out", error.Reason);
    }

    [Fact]
    public void Constructor_WithoutReason_UsesCodeNameAsReason()
    {
        var error = new Error(ErrorCode.Local_TimedOut);
        Assert.Equal("Local_TimedOut", error.Reason);
    }

    [Fact]
    public void Constructor_WithFatal_SetsFatal()
    {
        var error = new Error(ErrorCode.Local_Fatal, "Fatal error", true);
        Assert.True(error.IsFatal);
    }

    [Fact]
    public void IsError_ForNoError_ReturnsFalse()
    {
        var error = new Error(ErrorCode.NoError);
        Assert.False(error.IsError);
    }

    [Fact]
    public void IsError_ForErrorCode_ReturnsTrue()
    {
        var error = new Error(ErrorCode.RequestTimedOut);
        Assert.True(error.IsError);
    }

    [Fact]
    public void IsBrokerError_ForBrokerCodes_ReturnsTrue()
    {
        var error = new Error(ErrorCode.UnknownTopicOrPartition);
        Assert.True(error.IsBrokerError);
    }

    [Fact]
    public void IsLocalError_ForLocalCodes_ReturnsTrue()
    {
        var error = new Error(ErrorCode.Local_TimedOut);
        Assert.True(error.IsLocalError);
    }

    [Fact]
    public void ToString_ReturnsReason()
    {
        var error = new Error(ErrorCode.UnknownTopicOrPartition, "Topic not found");
        var str = error.ToString();
        Assert.Equal("Topic not found", str);  // ToString returns just the Reason
    }
}
