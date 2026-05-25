using System.Net.Http.Json;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Kuestenlogik.Surgewave.Control.Models.Assistant;

namespace Kuestenlogik.Surgewave.Control.Services.Assistant;

/// <summary>
/// Minimal OpenAI-compatible HTTP client for chat completions.
/// Works with OpenAI, Ollama, Azure OpenAI, and LM Studio.
/// Gracefully degrades when no endpoint is configured.
/// </summary>
public sealed class LlmClient : ILlmClient
{
    private readonly HttpClient _httpClient;
    private readonly AssistantSettings _settings;
    private readonly ILogger<LlmClient> _logger;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public LlmClient(HttpClient httpClient, AssistantSettings settings, ILogger<LlmClient> logger)
    {
        _httpClient = httpClient;
        _settings = settings;
        _logger = logger;
    }

    /// <inheritdoc />
    public async Task<string> CompleteAsync(string systemPrompt, string userMessage, CancellationToken ct = default)
    {
        if (!IsConfigured())
            return "LLM not configured. Set an endpoint and API key in Assistant settings to enable AI-powered responses.";

        try
        {
            var request = BuildRequest(systemPrompt, userMessage, stream: false);
            var response = await SendRequestAsync(request, ct);

            var json = await response.Content.ReadFromJsonAsync<ChatCompletionResponse>(JsonOptions, ct);
            return json?.Choices?.FirstOrDefault()?.Message?.Content
                ?? "No response from LLM.";
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "LLM completion request failed");
            return $"LLM request failed: {ex.Message}";
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> StreamCompleteAsync(
        string systemPrompt,
        string userMessage,
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (!IsConfigured())
        {
            yield return "LLM not configured. Set an endpoint and API key in Assistant settings to enable AI-powered responses.";
            yield break;
        }

        HttpResponseMessage? response = null;
        string? errorMessage = null;
        try
        {
            var request = BuildRequest(systemPrompt, userMessage, stream: true);
            response = await SendRequestAsync(request, ct);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "LLM streaming request failed");
            errorMessage = $"LLM request failed: {ex.Message}";
        }

        if (errorMessage is not null)
        {
            yield return errorMessage;
            yield break;
        }

        using var stream = await response!.Content.ReadAsStreamAsync(ct);
        using var reader = new StreamReader(stream, Encoding.UTF8);

        while (!reader.EndOfStream && !ct.IsCancellationRequested)
        {
            var line = await reader.ReadLineAsync(ct);
            if (line is null) break;

            if (!line.StartsWith("data: ", StringComparison.Ordinal)) continue;
            var data = line[6..];

            if (data == "[DONE]") break;

            ChatCompletionChunk? chunk;
            try
            {
                chunk = JsonSerializer.Deserialize<ChatCompletionChunk>(data, JsonOptions);
            }
            catch (JsonException)
            {
                continue;
            }

            var content = chunk?.Choices?.FirstOrDefault()?.Delta?.Content;
            if (content is not null)
            {
                yield return content;
            }
        }
    }

    private bool IsConfigured()
    {
        return _settings.LlmEnabled
            && !string.IsNullOrWhiteSpace(_settings.LlmEndpoint);
    }

    private ChatCompletionRequest BuildRequest(string systemPrompt, string userMessage, bool stream)
    {
        return new ChatCompletionRequest
        {
            Model = _settings.LlmModel ?? "gpt-4",
            Stream = stream,
            Messages =
            [
                new ChatMessage { Role = "system", Content = systemPrompt },
                new ChatMessage { Role = "user", Content = userMessage }
            ]
        };
    }

    private async Task<HttpResponseMessage> SendRequestAsync(ChatCompletionRequest request, CancellationToken ct)
    {
        var endpoint = _settings.LlmEndpoint!.TrimEnd('/');
        var url = endpoint.EndsWith("/chat/completions", StringComparison.OrdinalIgnoreCase)
            ? endpoint
            : $"{endpoint}/v1/chat/completions";

        using var httpRequest = new HttpRequestMessage(HttpMethod.Post, url);
        httpRequest.Content = JsonContent.Create(request, options: JsonOptions);

        if (!string.IsNullOrWhiteSpace(_settings.LlmApiKey))
        {
            httpRequest.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", _settings.LlmApiKey);
        }

        var completionOption = request.Stream
            ? HttpCompletionOption.ResponseHeadersRead
            : HttpCompletionOption.ResponseContentRead;

        var response = await _httpClient.SendAsync(httpRequest, completionOption, ct);
        response.EnsureSuccessStatusCode();
        return response;
    }

    // DTOs for OpenAI-compatible API

    private sealed class ChatCompletionRequest
    {
        public string Model { get; init; } = "gpt-4";
        public bool Stream { get; init; }
        public List<ChatMessage> Messages { get; init; } = [];
    }

    private sealed class ChatMessage
    {
        public string Role { get; init; } = "";
        public string Content { get; init; } = "";
    }

    private sealed class ChatCompletionResponse
    {
        public List<ChatChoice>? Choices { get; init; }
    }

    private sealed class ChatChoice
    {
        public ChatMessage? Message { get; init; }
    }

    private sealed class ChatCompletionChunk
    {
        public List<ChatChunkChoice>? Choices { get; init; }
    }

    private sealed class ChatChunkChoice
    {
        public ChatDelta? Delta { get; init; }
    }

    private sealed class ChatDelta
    {
        public string? Content { get; init; }
    }
}
