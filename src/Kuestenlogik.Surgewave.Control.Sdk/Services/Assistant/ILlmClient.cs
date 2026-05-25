namespace Kuestenlogik.Surgewave.Control.Services.Assistant;

/// <summary>
/// Minimal client for OpenAI-compatible chat completion endpoints.
/// Works with OpenAI, Ollama, Azure OpenAI, and LM Studio.
/// </summary>
public interface ILlmClient
{
    /// <summary>
    /// Sends a single completion request and returns the full response.
    /// </summary>
    /// <param name="systemPrompt">System-level instructions.</param>
    /// <param name="userMessage">The user's message.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The assistant's response text, or a fallback message if LLM is not configured.</returns>
    Task<string> CompleteAsync(string systemPrompt, string userMessage, CancellationToken ct = default);

    /// <summary>
    /// Streams a completion response token-by-token using server-sent events.
    /// </summary>
    /// <param name="systemPrompt">System-level instructions.</param>
    /// <param name="userMessage">The user's message.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An async stream of response text chunks.</returns>
    IAsyncEnumerable<string> StreamCompleteAsync(string systemPrompt, string userMessage, CancellationToken ct = default);
}
