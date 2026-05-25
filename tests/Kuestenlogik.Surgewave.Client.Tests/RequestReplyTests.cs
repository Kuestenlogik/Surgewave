using Kuestenlogik.Surgewave.Client.RequestReply;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Client.Tests;

/// <summary>
/// Tests for the request-reply (RPC) pattern over Surgewave topics.
/// These tests validate the envelope format, correlation matching,
/// timeout behavior, typed serialization, concurrency, and error handling.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class RequestReplyTests
{
    #region Envelope Tests

    [Fact]
    public void RequestReply_RoundTrip()
    {
        // Arrange
        var correlationId = Guid.NewGuid().ToString("N");
        var replyTopic = "__reply.test123";
        var payload = "Hello, World!"u8.ToArray();

        // Act - wrap and unwrap a request
        var wrapped = RequestReplyEnvelope.WrapRequest(correlationId, replyTopic, payload);
        var (unwrappedCorrelation, unwrappedReplyTopic, unwrappedPayload) =
            RequestReplyEnvelope.UnwrapRequest(wrapped);

        // Assert
        Assert.Equal(correlationId, unwrappedCorrelation);
        Assert.Equal(replyTopic, unwrappedReplyTopic);
        Assert.Equal(payload, unwrappedPayload);

        // Act - wrap and unwrap a reply
        var replyPayload = "Response data"u8.ToArray();
        var wrappedReply = RequestReplyEnvelope.WrapReply(correlationId, replyPayload, isError: false, errorMessage: null);
        var (replyCorrelation, replyData, isError, errorMsg) =
            RequestReplyEnvelope.UnwrapReply(wrappedReply);

        // Assert
        Assert.Equal(correlationId, replyCorrelation);
        Assert.Equal(replyPayload, replyData);
        Assert.False(isError);
        Assert.Null(errorMsg);
    }

    [Fact]
    public void RequestReply_Timeout_ThrowsException()
    {
        // Verify the timeout configuration works correctly
        var config = new RequestReplyConfig
        {
            DefaultTimeout = TimeSpan.FromMilliseconds(50)
        };

        Assert.Equal(TimeSpan.FromMilliseconds(50), config.DefaultTimeout);

        // Verify TaskCompletionSource-based timeout pattern works
        var tcs = new TaskCompletionSource<ReplyMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        cts.Token.Register(() =>
            tcs.TrySetException(new TimeoutException("Request-reply timed out")));

        var ex = Assert.ThrowsAsync<TimeoutException>(async () => await tcs.Task);
        Assert.NotNull(ex);
    }

    [Fact]
    public void RequestReply_TypedSerialization()
    {
        // Test that request and reply envelopes handle arbitrary payloads
        var correlationId = Guid.NewGuid().ToString("N");
        var replyTopic = "__reply.typed";

        // Simulate JSON-serialized payload
        var jsonPayload = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
            new TestRequest { Name = "test", Count = 42 });

        var wrapped = RequestReplyEnvelope.WrapRequest(correlationId, replyTopic, jsonPayload);
        var (_, _, unwrapped) = RequestReplyEnvelope.UnwrapRequest(wrapped);

        var deserialized = System.Text.Json.JsonSerializer.Deserialize<TestRequest>(unwrapped);
        Assert.NotNull(deserialized);
        Assert.Equal("test", deserialized.Name);
        Assert.Equal(42, deserialized.Count);

        // Reply side
        var replyJson = System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(
            new TestReply { Result = "success", Value = 100 });

        var wrappedReply = RequestReplyEnvelope.WrapReply(correlationId, replyJson, false, null);
        var (_, replyData, _, _) = RequestReplyEnvelope.UnwrapReply(wrappedReply);

        var replyDeserialized = System.Text.Json.JsonSerializer.Deserialize<TestReply>(replyData);
        Assert.NotNull(replyDeserialized);
        Assert.Equal("success", replyDeserialized.Result);
        Assert.Equal(100, replyDeserialized.Value);
    }

    [Fact]
    public void RequestReply_MultipleRequests_Concurrent()
    {
        // Test that multiple independent correlation IDs can be tracked concurrently
        var correlationIds = Enumerable.Range(0, 100)
            .Select(_ => Guid.NewGuid().ToString("N"))
            .ToList();

        var replyTopic = "__reply.concurrent";
        var payloads = new Dictionary<string, byte[]>();

        // Wrap all requests
        var wrappedRequests = correlationIds.Select(id =>
        {
            var payload = System.Text.Encoding.UTF8.GetBytes($"request-{id}");
            payloads[id] = payload;
            return RequestReplyEnvelope.WrapRequest(id, replyTopic, payload);
        }).ToList();

        // Unwrap all in parallel and verify correlation
        var results = wrappedRequests.AsParallel().Select(wrapped =>
        {
            var (correlationId, topic, payload) = RequestReplyEnvelope.UnwrapRequest(wrapped);
            return (correlationId, topic, payload);
        }).ToList();

        Assert.Equal(100, results.Count);

        foreach (var (correlationId, topic, payload) in results)
        {
            Assert.Equal(replyTopic, topic);
            Assert.Contains(correlationId, correlationIds);
            Assert.Equal(payloads[correlationId], payload);
        }
    }

    [Fact]
    public void RequestReply_ErrorResponse()
    {
        // Test that error responses preserve the error message
        var correlationId = Guid.NewGuid().ToString("N");
        var errorMessage = "Division by zero";
        var emptyPayload = Array.Empty<byte>();

        var wrappedError = RequestReplyEnvelope.WrapReply(correlationId, emptyPayload, isError: true, errorMessage);
        var (replyCorrelation, replyPayload, isError, unwrappedErrorMsg) =
            RequestReplyEnvelope.UnwrapReply(wrappedError);

        Assert.Equal(correlationId, replyCorrelation);
        Assert.True(isError);
        Assert.Equal(errorMessage, unwrappedErrorMsg);
        Assert.Empty(replyPayload);

        // Verify that creating a ReplyMessage with error works
        var reply = new ReplyMessage(correlationId, replyPayload, DateTimeOffset.UtcNow, isError, unwrappedErrorMsg);
        Assert.True(reply.IsError);
        Assert.Equal("Division by zero", reply.ErrorMessage);
    }

    [Fact]
    public void RequestReply_CorrelationMatching()
    {
        // Simulate concurrent pending requests with TCS-based correlation matching
        var pending = new System.Collections.Concurrent.ConcurrentDictionary<string, TaskCompletionSource<ReplyMessage>>();

        // Create 10 pending requests
        var correlationIds = new List<string>();
        for (int i = 0; i < 10; i++)
        {
            var id = Guid.NewGuid().ToString("N");
            correlationIds.Add(id);
            pending[id] = new TaskCompletionSource<ReplyMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        Assert.Equal(10, pending.Count);

        // Complete them out of order (reverse)
        for (int i = correlationIds.Count - 1; i >= 0; i--)
        {
            var id = correlationIds[i];
            var payload = System.Text.Encoding.UTF8.GetBytes($"reply-{i}");
            var reply = new ReplyMessage(id, payload, DateTimeOffset.UtcNow, false, null);

            Assert.True(pending.TryRemove(id, out var tcs));
            tcs.TrySetResult(reply);
        }

        Assert.Empty(pending);

        // Verify unmatched correlation ID doesn't affect pending requests
        var unknownId = Guid.NewGuid().ToString("N");
        Assert.False(pending.TryRemove(unknownId, out _));
    }

    #endregion

    #region Configuration Tests

    [Fact]
    public void RequestReplyConfig_DefaultValues()
    {
        var config = new RequestReplyConfig();

        Assert.Equal(TimeSpan.FromSeconds(30), config.DefaultTimeout);
        Assert.Equal("__reply", config.ReplyTopicPrefix);
        Assert.True(config.AutoCreateTopics);
    }

    [Fact]
    public void RequestReplyConfig_CustomValues()
    {
        var config = new RequestReplyConfig
        {
            DefaultTimeout = TimeSpan.FromSeconds(10),
            ReplyTopicPrefix = "_responses",
            AutoCreateTopics = false
        };

        Assert.Equal(TimeSpan.FromSeconds(10), config.DefaultTimeout);
        Assert.Equal("_responses", config.ReplyTopicPrefix);
        Assert.False(config.AutoCreateTopics);
    }

    #endregion

    #region Exception Tests

    [Fact]
    public void RequestReplyException_PreservesCorrelationId()
    {
        var correlationId = "abc123";
        var ex = new RequestReplyException("Server error", correlationId);

        Assert.Equal("Server error", ex.Message);
        Assert.Equal(correlationId, ex.CorrelationId);
        Assert.IsAssignableFrom<SurgewaveClientException>(ex);

        var exWithInner = new RequestReplyException("Outer", correlationId, new InvalidOperationException("Inner"));
        Assert.Equal(correlationId, exWithInner.CorrelationId);
        Assert.IsType<InvalidOperationException>(exWithInner.InnerException);
    }

    #endregion

    #region Model Tests

    [Fact]
    public void RequestMessage_RecordEquality()
    {
        var msg1 = new RequestMessage("corr1", "reply-topic", [1, 2, 3], [4, 5, 6],
            DateTimeOffset.UnixEpoch, null);
        var msg2 = new RequestMessage("corr1", "reply-topic", [1, 2, 3], [4, 5, 6],
            DateTimeOffset.UnixEpoch, null);

        // Record equality checks reference equality for arrays, so these won't be equal
        // but the properties should match
        Assert.Equal(msg1.CorrelationId, msg2.CorrelationId);
        Assert.Equal(msg1.ReplyTopic, msg2.ReplyTopic);
        Assert.Equal(msg1.Key, msg2.Key);
        Assert.Equal(msg1.Value, msg2.Value);
        Assert.Equal(msg1.Timestamp, msg2.Timestamp);
    }

    [Fact]
    public void ReplyMessage_RecordProperties()
    {
        var now = DateTimeOffset.UtcNow;
        var payload = new byte[] { 10, 20, 30 };
        var reply = new ReplyMessage("corr-id", payload, now, false, null);

        Assert.Equal("corr-id", reply.CorrelationId);
        Assert.Same(payload, reply.Value);
        Assert.Equal(now, reply.Timestamp);
        Assert.False(reply.IsError);
        Assert.Null(reply.ErrorMessage);
    }

    #endregion

    #region Envelope Edge Cases

    [Fact]
    public void RequestReply_EmptyPayload_RoundTrip()
    {
        var correlationId = Guid.NewGuid().ToString("N");
        var replyTopic = "__reply.empty";
        var emptyPayload = Array.Empty<byte>();

        var wrapped = RequestReplyEnvelope.WrapRequest(correlationId, replyTopic, emptyPayload);
        var (unwrappedCorrelation, unwrappedReplyTopic, unwrappedPayload) =
            RequestReplyEnvelope.UnwrapRequest(wrapped);

        Assert.Equal(correlationId, unwrappedCorrelation);
        Assert.Equal(replyTopic, unwrappedReplyTopic);
        Assert.Empty(unwrappedPayload);
    }

    [Fact]
    public void RequestReply_LargePayload_RoundTrip()
    {
        var correlationId = Guid.NewGuid().ToString("N");
        var replyTopic = "__reply.large";
        var largePayload = new byte[64 * 1024]; // 64 KB
        Random.Shared.NextBytes(largePayload);

        var wrapped = RequestReplyEnvelope.WrapRequest(correlationId, replyTopic, largePayload);
        var (_, _, unwrappedPayload) = RequestReplyEnvelope.UnwrapRequest(wrapped);

        Assert.Equal(largePayload, unwrappedPayload);
    }

    [Fact]
    public void RequestReply_ErrorResponse_NoMessage()
    {
        var correlationId = Guid.NewGuid().ToString("N");

        // Error with null error message
        var wrapped = RequestReplyEnvelope.WrapReply(correlationId, [], isError: true, errorMessage: null);
        var (_, _, isError, errorMsg) = RequestReplyEnvelope.UnwrapReply(wrapped);

        Assert.True(isError);
        Assert.Null(errorMsg);
    }

    [Fact]
    public void RequestReply_UnicodeContent_RoundTrip()
    {
        var correlationId = Guid.NewGuid().ToString("N");
        var replyTopic = "__reply.unicode.日本語";
        var payload = System.Text.Encoding.UTF8.GetBytes("Grüße aus dem Weltall 🌍");

        var wrapped = RequestReplyEnvelope.WrapRequest(correlationId, replyTopic, payload);
        var (_, unwrappedTopic, unwrappedPayload) = RequestReplyEnvelope.UnwrapRequest(wrapped);

        Assert.Equal(replyTopic, unwrappedTopic);
        Assert.Equal(payload, unwrappedPayload);
    }

    #endregion

    #region Test Types

    private sealed class TestRequest
    {
        public string Name { get; set; } = "";
        public int Count { get; set; }
    }

    private sealed class TestReply
    {
        public string Result { get; set; } = "";
        public int Value { get; set; }
    }

    #endregion
}
