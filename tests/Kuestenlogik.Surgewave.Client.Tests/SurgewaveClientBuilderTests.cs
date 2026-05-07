using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Native.Operations.Transactions;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// Tests for SurgewaveClientBuilder, CrossTopicTxnModels, BatchResult, and AutoOffsetReset.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class SurgewaveClientBuilderTests
{
    #region SurgewaveClientBuilder Tests

    [Fact]
    public void SurgewaveClientBuilder_NullBootstrapServers_Throws()
    {
        Assert.Throws<ArgumentException>(() => new SurgewaveClientBuilder(null!));
    }

    [Fact]
    public void SurgewaveClientBuilder_EmptyBootstrapServers_Throws()
    {
        Assert.Throws<ArgumentException>(() => new SurgewaveClientBuilder(""));
    }

    [Fact]
    public void SurgewaveClientBuilder_WhitespaceBootstrapServers_Throws()
    {
        Assert.Throws<ArgumentException>(() => new SurgewaveClientBuilder("   "));
    }

    [Fact]
    public void SurgewaveClient_Create_ReturnsBuilder()
    {
        var builder = SurgewaveClient.Create("localhost:9092");
        Assert.NotNull(builder);
    }

    [Fact]
    public void SurgewaveClientBuilder_FluentChaining()
    {
        var builder = SurgewaveClient.Create("localhost:9092")
            .UseSurgewaveProtocol()
            .WithClientId("my-app");

        Assert.NotNull(builder);
    }

    [Fact]
    public void SurgewaveClientBuilder_UseKafkaProtocol_ReturnsSelf()
    {
        var builder = SurgewaveClient.Create("localhost:9092");
        var result = builder.UseKafkaProtocol();
        Assert.Same(builder, result);
    }

    [Fact]
    public void SurgewaveClientBuilder_UseSurgewaveProtocol_ReturnsSelf()
    {
        var builder = SurgewaveClient.Create("localhost:9092");
        var result = builder.UseSurgewaveProtocol();
        Assert.Same(builder, result);
    }

    [Fact]
    public void SurgewaveClientBuilder_UseAutoDetect_ReturnsSelf()
    {
        var builder = SurgewaveClient.Create("localhost:9092");
        var result = builder.UseAutoDetect();
        Assert.Same(builder, result);
    }

    [Fact]
    public void SurgewaveClientBuilder_WithClientId_ReturnsSelf()
    {
        var builder = SurgewaveClient.Create("localhost:9092");
        var result = builder.WithClientId("test");
        Assert.Same(builder, result);
    }

    #endregion

    #region CrossTopicTxnModels Tests

    [Fact]
    public void CrossTopicTxnBeginResponse_Properties()
    {
        var response = new CrossTopicTxnBeginResponse(Protocol.Native.SurgewaveErrorCode.None, "txn-123");
        Assert.Equal(Protocol.Native.SurgewaveErrorCode.None, response.ErrorCode);
        Assert.Equal("txn-123", response.TransactionId);
    }

    [Fact]
    public void CrossTopicTxnAddWriteResponse_Properties()
    {
        var response = new CrossTopicTxnAddWriteResponse(Protocol.Native.SurgewaveErrorCode.None, 5);
        Assert.Equal(Protocol.Native.SurgewaveErrorCode.None, response.ErrorCode);
        Assert.Equal(5, response.PendingWriteCount);
    }

    [Fact]
    public void CrossTopicTxnCommitResponse_Properties()
    {
        var response = new CrossTopicTxnCommitResponse(
            Protocol.Native.SurgewaveErrorCode.None, 3, 15, 120, null);

        Assert.Equal(Protocol.Native.SurgewaveErrorCode.None, response.ErrorCode);
        Assert.Equal(3, response.TopicsWritten);
        Assert.Equal(15, response.MessagesWritten);
        Assert.Equal(120, response.DurationMs);
        Assert.Null(response.Error);
    }

    [Fact]
    public void CrossTopicTxnCommitResponse_WithError()
    {
        var response = new CrossTopicTxnCommitResponse(
            Protocol.Native.SurgewaveErrorCode.TopicNotFound, 0, 0, 0, "Topic not found");

        Assert.Equal(Protocol.Native.SurgewaveErrorCode.TopicNotFound, response.ErrorCode);
        Assert.Equal("Topic not found", response.Error);
    }

    #endregion

    #region BatchResult Tests

    [Fact]
    public void BatchResult_Success()
    {
        var offsets = new Dictionary<string, long>
        {
            ["topic-0"] = 42,
            ["topic-1"] = 100
        };
        var result = new BatchResult(true, 10, offsets);

        Assert.True(result.Success);
        Assert.Equal(10, result.MessageCount);
        Assert.Equal(2, result.Offsets.Count);
        Assert.Equal(42, result.Offsets["topic-0"]);
    }

    [Fact]
    public void BatchResult_Empty()
    {
        var result = new BatchResult(true, 0, new Dictionary<string, long>());
        Assert.True(result.Success);
        Assert.Equal(0, result.MessageCount);
        Assert.Empty(result.Offsets);
    }

    #endregion

    #region AutoOffsetReset Tests

    [Fact]
    public void AutoOffsetReset_Values()
    {
        Assert.True(Enum.IsDefined(Consumer.AutoOffsetReset.Earliest));
        Assert.True(Enum.IsDefined(Consumer.AutoOffsetReset.Latest));
    }

    #endregion

    #region IsolationLevel Tests

    [Fact]
    public void IsolationLevel_Values()
    {
        Assert.True(Enum.IsDefined(Consumer.IsolationLevel.ReadUncommitted));
        Assert.True(Enum.IsDefined(Consumer.IsolationLevel.ReadCommitted));
    }

    #endregion

    #region ProtocolType Tests

    [Fact]
    public void ProtocolType_Values()
    {
        Assert.True(Enum.IsDefined(ProtocolType.Auto));
        Assert.True(Enum.IsDefined(ProtocolType.SurgewaveNative));
        Assert.True(Enum.IsDefined(ProtocolType.Kafka));
    }

    #endregion
}
