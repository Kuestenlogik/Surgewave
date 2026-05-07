using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Kuestenlogik.Surgewave.Control.Models.Chat;

namespace Kuestenlogik.Surgewave.Control.Services;

public sealed class ChatApiClient : IChatApiClient
{
    private static readonly JsonSerializerOptions s_sseJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly HttpClient _httpClient;

    public ChatApiClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public async Task<ChatResponse> SendMessageAsync(string pipelineId, string message, string? sessionId = null, CancellationToken ct = default)
    {
        var request = new ChatRequest { Message = message, SessionId = sessionId };
        var response = await _httpClient.PostAsJsonAsync($"/api/pipelines/{pipelineId}/chat", request, ct);
        response.EnsureSuccessStatusCode();

        return await response.Content.ReadFromJsonAsync<ChatResponse>(ct)
            ?? throw new InvalidOperationException("Failed to deserialize chat response");
    }

    public async Task<AsyncChatResponse> SendMessageFireAndForgetAsync(string pipelineId, string message, string? sessionId = null, CancellationToken ct = default)
    {
        var request = new ChatRequest { Message = message, SessionId = sessionId };
        var response = await _httpClient.PostAsJsonAsync($"/api/pipelines/{pipelineId}/chat/async", request, ct);
        response.EnsureSuccessStatusCode();

        return await response.Content.ReadFromJsonAsync<AsyncChatResponse>(ct)
            ?? throw new InvalidOperationException("Failed to deserialize async chat response");
    }

    public async IAsyncEnumerable<ChatStreamEvent> StreamMessageAsync(
        string pipelineId,
        string message,
        string? sessionId = null,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        var chatRequest = new ChatRequest { Message = message, SessionId = sessionId };
        var content = JsonContent.Create(chatRequest);

        using var request = new HttpRequestMessage(HttpMethod.Post, $"/api/pipelines/{pipelineId}/chat/stream")
        {
            Content = content
        };

        using var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, ct);
        response.EnsureSuccessStatusCode();

        using var stream = await response.Content.ReadAsStreamAsync(ct);
        using var reader = new StreamReader(stream);

        string? currentEventType = null;

        while (!reader.EndOfStream)
        {
            ct.ThrowIfCancellationRequested();

            var line = await reader.ReadLineAsync(ct);
            if (line is null)
                break;

            if (line.StartsWith("event: ", StringComparison.Ordinal))
            {
                currentEventType = line[7..];
            }
            else if (line.StartsWith("data: ", StringComparison.Ordinal))
            {
                var json = line[6..];
                var evt = JsonSerializer.Deserialize<ChatStreamEvent>(json, s_sseJsonOptions);
                if (evt is not null)
                {
                    // Ensure the Type is set from the event line if not present in the JSON
                    if (string.IsNullOrEmpty(evt.Type) && currentEventType is not null)
                    {
                        evt.Type = currentEventType;
                    }

                    yield return evt;
                }

                currentEventType = null;
            }
            // Empty line = event boundary, just continue
        }
    }

    public async Task<ChatHistoryResponse> GetHistoryAsync(string pipelineId, string sessionId, CancellationToken ct = default)
    {
        var response = await _httpClient.GetFromJsonAsync<ChatHistoryResponse>(
            $"/api/pipelines/{pipelineId}/chat/sessions/{sessionId}", ct);
        return response ?? throw new InvalidOperationException("Failed to deserialize chat history");
    }

    public async Task<ChatSessionListResponse> ListSessionsAsync(string pipelineId, CancellationToken ct = default)
    {
        var response = await _httpClient.GetFromJsonAsync<ChatSessionListResponse>(
            $"/api/pipelines/{pipelineId}/chat/sessions", ct);
        return response ?? new ChatSessionListResponse { PipelineId = pipelineId };
    }

    public async Task DeleteSessionAsync(string pipelineId, string sessionId, CancellationToken ct = default)
    {
        var response = await _httpClient.DeleteAsync($"/api/pipelines/{pipelineId}/chat/sessions/{sessionId}", ct);
        response.EnsureSuccessStatusCode();
    }

    public async Task<ChatTopicInfo> GetTopicsAsync(string pipelineId, CancellationToken ct = default)
    {
        var response = await _httpClient.GetFromJsonAsync<ChatTopicInfo>(
            $"/api/pipelines/{pipelineId}/chat/topics", ct);
        return response ?? throw new InvalidOperationException("Failed to deserialize chat topic info");
    }
}
