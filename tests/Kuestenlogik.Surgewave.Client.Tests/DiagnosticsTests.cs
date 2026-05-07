using Kuestenlogik.Surgewave.Client.Diagnostics;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// Tests for RecoverySuggestion and ProtocolException diagnostics.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class DiagnosticsTests
{
    #region RecoverySuggestion.ForErrorCode Tests

    [Theory]
    [InlineData(ErrorCode.UnknownTopicOrPartition)]
    [InlineData(ErrorCode.LeaderNotAvailable)]
    [InlineData(ErrorCode.NotLeaderForPartition)]
    [InlineData(ErrorCode.RequestTimedOut)]
    [InlineData(ErrorCode.MessageTooLarge)]
    [InlineData(ErrorCode.OffsetOutOfRange)]
    [InlineData(ErrorCode.TopicAlreadyExists)]
    [InlineData(ErrorCode.InvalidPartitions)]
    [InlineData(ErrorCode.InvalidConfig)]
    public void ForErrorCode_KnownCodes_ReturnsSuggestion(ErrorCode code)
    {
        var suggestion = RecoverySuggestion.ForErrorCode(code);
        Assert.NotNull(suggestion);
        Assert.NotEmpty(suggestion);
    }

    [Fact]
    public void ForErrorCode_None_ReturnsNull()
    {
        var suggestion = RecoverySuggestion.ForErrorCode(ErrorCode.None);
        Assert.Null(suggestion);
    }

    #endregion

    #region RecoverySuggestion.ForConnectionError Tests

    [Fact]
    public void ForConnectionError_WithHostAndPort()
    {
        var suggestion = RecoverySuggestion.ForConnectionError("localhost", 9092);
        Assert.Contains("localhost:9092", suggestion);
        Assert.Contains("reachable", suggestion);
    }

    [Fact]
    public void ForConnectionError_HostOnly()
    {
        var suggestion = RecoverySuggestion.ForConnectionError("broker.example.com", null);
        Assert.Contains("broker.example.com", suggestion);
    }

    [Fact]
    public void ForConnectionError_NullHost()
    {
        var suggestion = RecoverySuggestion.ForConnectionError(null, null);
        Assert.Contains("broker", suggestion);
    }

    #endregion

    #region RecoverySuggestion.ForConfigurationError Tests

    [Theory]
    [InlineData("BootstrapServers")]
    [InlineData("GroupId")]
    [InlineData("TransactionalId")]
    [InlineData("ClientId")]
    public void ForConfigurationError_KnownProperties(string propertyName)
    {
        var suggestion = RecoverySuggestion.ForConfigurationError(propertyName);
        Assert.NotNull(suggestion);
        Assert.NotEmpty(suggestion);
    }

    [Fact]
    public void ForConfigurationError_UnknownProperty()
    {
        var suggestion = RecoverySuggestion.ForConfigurationError("CustomProp");
        Assert.Contains("CustomProp", suggestion);
    }

    #endregion

    #region RecoverySuggestion.ForSerializationError Tests

    [Fact]
    public void ForSerializationError_Serialize()
    {
        var suggestion = RecoverySuggestion.ForSerializationError(typeof(string), SerializationDirection.Serialize);
        Assert.Contains("serializable", suggestion, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("String", suggestion);
    }

    [Fact]
    public void ForSerializationError_Deserialize()
    {
        var suggestion = RecoverySuggestion.ForSerializationError(typeof(int), SerializationDirection.Deserialize);
        Assert.Contains("deserializer", suggestion, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ForSerializationError_NullType()
    {
        var suggestion = RecoverySuggestion.ForSerializationError(null, SerializationDirection.Serialize);
        Assert.NotNull(suggestion);
        Assert.Contains("message", suggestion, StringComparison.OrdinalIgnoreCase);
    }

    #endregion

    #region RecoverySuggestion.ForTopicPartitionError Tests

    [Fact]
    public void ForTopicPartitionError_WithPartition()
    {
        var suggestion = RecoverySuggestion.ForTopicPartitionError("orders", 5);
        Assert.Contains("partition 5", suggestion);
        Assert.Contains("orders", suggestion);
    }

    [Fact]
    public void ForTopicPartitionError_TopicOnly()
    {
        var suggestion = RecoverySuggestion.ForTopicPartitionError("orders", null);
        Assert.Contains("orders", suggestion);
        Assert.Contains("create", suggestion, StringComparison.OrdinalIgnoreCase);
    }

    #endregion

    #region ProtocolException Tests

    [Fact]
    public void ProtocolException_DefaultConstructor()
    {
        var ex = new ProtocolException();
        Assert.NotNull(ex);
    }

    [Fact]
    public void ProtocolException_WithMessage()
    {
        var ex = new ProtocolException("test message");
        Assert.Equal("test message", ex.Message);
    }

    [Fact]
    public void ProtocolException_WithInner()
    {
        var inner = new IOException("inner");
        var ex = new ProtocolException("outer", inner);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void ProtocolException_IsSurgewaveClientException()
    {
        var ex = new ProtocolException("test");
        Assert.IsAssignableFrom<SurgewaveClientException>(ex);
    }

    #endregion
}
