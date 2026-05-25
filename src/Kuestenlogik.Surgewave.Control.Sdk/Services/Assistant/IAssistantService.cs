using Kuestenlogik.Surgewave.Control.Models.Assistant;

namespace Kuestenlogik.Surgewave.Control.Services.Assistant;

/// <summary>
/// Orchestrates assistant operations by routing user questions to the appropriate backend service.
/// </summary>
public interface IAssistantService
{
    /// <summary>
    /// Processes a natural-language question and returns a rich assistant response.
    /// </summary>
    /// <param name="question">The user's question or command.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>An assistant message with content, anomalies, recommendations, or generated SQL.</returns>
    Task<AssistantMessage> AskAsync(string question, CancellationToken ct = default);
}
