using System.Text.Json;
using Kuestenlogik.Surgewave.Connect.Pipelines.Chat;

namespace Kuestenlogik.Surgewave.Connect.Tests;

public class PipelineChatTests
{
    private static readonly JsonSerializerOptions s_camelCaseOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    [Fact]
    public void ChatSession_AddUserMessage_TracksInHistory()
    {
        var session = new ChatSession("session-1", "pipeline-1");
        var msgId = session.AddUserMessage("Hello");

        Assert.NotNull(msgId);
        Assert.Single(session.History);
        Assert.Equal("user", session.History[0].Role);
        Assert.Equal("Hello", session.History[0].Content);
        Assert.Equal(1, session.PendingCount);
    }

    [Fact]
    public void ChatSession_CompleteResponse_AddsAssistantMessage()
    {
        var session = new ChatSession("session-1", "pipeline-1");
        var msgId = session.AddUserMessage("Hello");

        session.CompleteResponse(msgId, "Hi there!");

        Assert.Equal(2, session.MessageCount);
        Assert.Equal(0, session.PendingCount);

        var history = session.History;
        Assert.Equal("user", history[0].Role);
        Assert.Equal("assistant", history[1].Role);
        Assert.Equal("Hi there!", history[1].Content);
    }

    [Fact]
    public async Task ChatSession_WaitForResponse_ReturnsWhenCompleted()
    {
        var session = new ChatSession("session-1", "pipeline-1");
        var msgId = session.AddUserMessage("Question?");

        // Complete after a short delay
        _ = Task.Run(async () =>
        {
            await Task.Delay(50);
            session.CompleteResponse(msgId, "Answer!");
        });

        var response = await session.WaitForResponseAsync(msgId, TimeSpan.FromSeconds(5), CancellationToken.None);

        Assert.Equal("assistant", response.Role);
        Assert.Equal("Answer!", response.Content);
    }

    [Fact]
    public async Task ChatSession_WaitForResponse_TimesOut()
    {
        var session = new ChatSession("session-1", "pipeline-1");
        var msgId = session.AddUserMessage("Question?");

        var response = await session.WaitForResponseAsync(msgId, TimeSpan.FromMilliseconds(50), CancellationToken.None);

        Assert.Equal("system", response.Role);
        Assert.Contains("timed out", response.Content, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ChatSession_FailResponse_AddsSystemMessage()
    {
        var session = new ChatSession("session-1", "pipeline-1");
        var msgId = session.AddUserMessage("Hello");

        session.FailResponse(msgId, "Pipeline error");

        Assert.Equal(2, session.MessageCount);
        Assert.Equal(0, session.PendingCount);

        var history = session.History;
        Assert.Equal("system", history[1].Role);
        Assert.Equal("Pipeline error", history[1].Content);
        Assert.Equal("true", history[1].Metadata!["error"]);
    }

    [Fact]
    public void ChatSession_MultipleMessages_MaintainsOrder()
    {
        var session = new ChatSession("session-1", "pipeline-1");

        var id1 = session.AddUserMessage("First");
        session.CompleteResponse(id1, "Response 1");

        var id2 = session.AddUserMessage("Second");
        session.CompleteResponse(id2, "Response 2");

        var history = session.History;
        Assert.Equal(4, history.Count);
        Assert.Equal("First", history[0].Content);
        Assert.Equal("Response 1", history[1].Content);
        Assert.Equal("Second", history[2].Content);
        Assert.Equal("Response 2", history[3].Content);
    }

    [Fact]
    public void TopicNames_AreCorrectlyFormatted()
    {
        Assert.Equal("_pipeline-chat-my-pipe-signal", PipelineChatManager.GetSignalTopicName("my-pipe"));
        Assert.Equal("_pipeline-chat-my-pipe-response", PipelineChatManager.GetResponseTopicName("my-pipe"));
    }

    [Fact]
    public void ChatMessage_HasRequiredProperties()
    {
        var msg = new ChatMessage
        {
            Id = "test-id",
            Role = "user",
            Content = "Hello",
            Metadata = new Dictionary<string, string> { ["key"] = "value" }
        };

        Assert.Equal("test-id", msg.Id);
        Assert.Equal("user", msg.Role);
        Assert.Equal("Hello", msg.Content);
        Assert.Equal("value", msg.Metadata["key"]);
        Assert.True(msg.Timestamp <= DateTimeOffset.UtcNow);
    }

    [Fact]
    public void ChatSession_Properties_AreInitialized()
    {
        var session = new ChatSession("session-abc", "pipeline-xyz");

        Assert.Equal("session-abc", session.SessionId);
        Assert.Equal("pipeline-xyz", session.PipelineId);
        Assert.Equal(0, session.MessageCount);
        Assert.Equal(0, session.PendingCount);
        Assert.True(session.CreatedAt <= DateTimeOffset.UtcNow);
    }

    [Fact]
    public void ChatSession_UserMessage_HasMetadata()
    {
        var session = new ChatSession("session-1", "pipeline-1");
        var meta = new Dictionary<string, string> { ["source"] = "api" };
        var msgId = session.AddUserMessage("Hello", meta);

        var history = session.History;
        Assert.Equal("api", history[0].Metadata!["source"]);
    }

    [Fact]
    public async Task ChatSession_CancellationToken_Cancels()
    {
        var session = new ChatSession("session-1", "pipeline-1");
        var msgId = session.AddUserMessage("Hello");

        using var cts = new CancellationTokenSource(50);

        // When the caller's token is cancelled, TaskCanceledException propagates
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => session.WaitForResponseAsync(msgId, TimeSpan.FromSeconds(30), cts.Token));
    }

    // --- ChatStreamEvent Tests ---

    [Fact]
    public void ChatStreamEvent_Token_HasCorrectProperties()
    {
        var evt = new ChatStreamEvent
        {
            Type = "token",
            Token = "Hello"
        };

        Assert.Equal("token", evt.Type);
        Assert.Equal("Hello", evt.Token);
        Assert.Null(evt.SessionId);
        Assert.Null(evt.MessageId);
        Assert.Null(evt.FullContent);
        Assert.Null(evt.Error);
    }

    [Fact]
    public void ChatStreamEvent_Done_HasCorrectProperties()
    {
        var evt = new ChatStreamEvent
        {
            Type = "done",
            SessionId = "session-1",
            MessageId = "msg-1",
            FullContent = "Hello world"
        };

        Assert.Equal("done", evt.Type);
        Assert.Equal("session-1", evt.SessionId);
        Assert.Equal("msg-1", evt.MessageId);
        Assert.Equal("Hello world", evt.FullContent);
        Assert.Null(evt.Token);
        Assert.Null(evt.Error);
    }

    [Fact]
    public void ChatStreamEvent_Error_HasCorrectProperties()
    {
        var evt = new ChatStreamEvent
        {
            Type = "error",
            Error = "Something went wrong"
        };

        Assert.Equal("error", evt.Type);
        Assert.Equal("Something went wrong", evt.Error);
        Assert.Null(evt.Token);
        Assert.Null(evt.SessionId);
    }

    [Fact]
    public void ChatStreamEvent_Serialization_RoundTrips()
    {
        var original = new ChatStreamEvent
        {
            Type = "done",
            SessionId = "s1",
            MessageId = "m1",
            FullContent = "Test content"
        };

        var json = JsonSerializer.Serialize(original, s_camelCaseOptions);

        Assert.Contains("\"type\":\"done\"", json);
        Assert.Contains("\"sessionId\":\"s1\"", json);
        Assert.Contains("\"fullContent\":\"Test content\"", json);
    }
}
