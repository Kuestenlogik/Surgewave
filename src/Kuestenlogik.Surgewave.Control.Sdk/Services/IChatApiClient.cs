using Kuestenlogik.Surgewave.Control.Models.Chat;

namespace Kuestenlogik.Surgewave.Control.Services;

public interface IChatApiClient
{
    Task<ChatResponse> SendMessageAsync(string pipelineId, string message, string? sessionId = null, CancellationToken ct = default);
    Task<AsyncChatResponse> SendMessageFireAndForgetAsync(string pipelineId, string message, string? sessionId = null, CancellationToken ct = default);
    IAsyncEnumerable<ChatStreamEvent> StreamMessageAsync(string pipelineId, string message, string? sessionId = null, CancellationToken ct = default);
    Task<ChatHistoryResponse> GetHistoryAsync(string pipelineId, string sessionId, CancellationToken ct = default);
    Task<ChatSessionListResponse> ListSessionsAsync(string pipelineId, CancellationToken ct = default);
    Task DeleteSessionAsync(string pipelineId, string sessionId, CancellationToken ct = default);
    Task<ChatTopicInfo> GetTopicsAsync(string pipelineId, CancellationToken ct = default);
}
