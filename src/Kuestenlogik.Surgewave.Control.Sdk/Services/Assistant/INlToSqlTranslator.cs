using Kuestenlogik.Surgewave.Control.Models.Assistant;

namespace Kuestenlogik.Surgewave.Control.Services.Assistant;

/// <summary>
/// Translates natural-language questions about message data into Surgewave SQL queries.
/// </summary>
public interface INlToSqlTranslator
{
    /// <summary>
    /// Attempts to translate a natural-language question into a SQL query.
    /// </summary>
    /// <param name="question">The user's natural-language question.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>The translation result including generated SQL and confidence score.</returns>
    Task<NlSqlResult> TranslateAsync(string question, CancellationToken ct = default);
}
