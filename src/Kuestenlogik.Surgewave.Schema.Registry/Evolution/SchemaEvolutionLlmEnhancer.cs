using System.Text;

namespace Kuestenlogik.Surgewave.Schema.Registry.Evolution;

/// <summary>
/// Enhances schema evolution analysis with LLM-generated natural language explanations.
/// Uses an optional LLM client — degrades gracefully to rule-based text when no LLM is configured.
/// </summary>
public sealed class SchemaEvolutionLlmEnhancer
{
    private readonly Func<string, string, CancellationToken, Task<string>>? _llmComplete;

    /// <summary>
    /// Creates an enhancer with an LLM completion function.
    /// </summary>
    /// <param name="llmComplete">A delegate that takes (systemPrompt, userMessage, ct) and returns LLM text.
    /// Pass null if no LLM is available.</param>
    public SchemaEvolutionLlmEnhancer(Func<string, string, CancellationToken, Task<string>>? llmComplete)
    {
        _llmComplete = llmComplete;
    }

    /// <summary>
    /// Generate a natural language explanation of a schema change.
    /// Falls back to rule-based summary when LLM is unavailable.
    /// </summary>
    public async Task<string> ExplainChangeAsync(SchemaChange change, CancellationToken ct = default)
    {
        if (_llmComplete is null)
        {
            return GenerateRuleBasedExplanation(change);
        }

        var systemPrompt = """
            You are a schema evolution expert for Apache Kafka / Surgewave message broker.
            Explain schema changes in clear, concise language that a developer can understand.
            Focus on what changed, why it matters, and what consumers need to do.
            Keep the response under 200 words.
            """;

        var userMessage = BuildChangeDescription(change);

        try
        {
            return await _llmComplete(systemPrompt, userMessage, ct);
        }
        catch
        {
            // Fall back to rule-based explanation on any LLM failure
            return GenerateRuleBasedExplanation(change);
        }
    }

    /// <summary>
    /// Suggest the best migration strategy for a schema change.
    /// Falls back to rule-based suggestion when LLM is unavailable.
    /// </summary>
    public async Task<string> SuggestMigrationAsync(SchemaChange change, CancellationToken ct = default)
    {
        if (_llmComplete is null)
        {
            return GenerateRuleBasedMigrationSuggestion(change);
        }

        var systemPrompt = """
            You are a .NET developer expert. Given a schema change, suggest the best migration strategy.
            Provide specific C# code patterns and best practices.
            Keep the response concise and actionable.
            """;

        var userMessage = BuildChangeDescription(change);

        try
        {
            return await _llmComplete(systemPrompt, userMessage, ct);
        }
        catch
        {
            return GenerateRuleBasedMigrationSuggestion(change);
        }
    }

    private static string BuildChangeDescription(SchemaChange change)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"Schema subject: {change.SubjectName}");
        sb.AppendLine($"Version change: v{change.OldVersion} -> v{change.NewVersion}");
        sb.AppendLine($"Breaking level: {change.Breaking}");
        sb.AppendLine("Field changes:");

        foreach (var fc in change.FieldChanges)
        {
            sb.Append($"  - {fc.Type}: {fc.FieldName}");
            if (fc.OldType is not null) sb.Append($" (was: {fc.OldType})");
            if (fc.NewType is not null) sb.Append($" (now: {fc.NewType})");
            if (fc.OldFieldName is not null) sb.Append($" (renamed from: {fc.OldFieldName})");
            sb.AppendLine();
        }

        return sb.ToString();
    }

    private static string GenerateRuleBasedExplanation(SchemaChange change)
    {
        var sb = new StringBuilder();

        sb.AppendLine($"Schema '{change.SubjectName}' has been updated from version {change.OldVersion} to {change.NewVersion}.");
        sb.AppendLine();

        if (change.Breaking == BreakingLevel.Major)
        {
            sb.AppendLine("WARNING: This is a BREAKING change. Existing consumers will need code updates before processing new messages.");
        }
        else if (change.Breaking == BreakingLevel.Minor)
        {
            sb.AppendLine("This is a minor change. New consumers should be aware, but existing consumers will continue to work.");
        }
        else
        {
            sb.AppendLine("This is a non-breaking change. Existing consumers will continue to work without modification.");
        }

        sb.AppendLine();
        sb.AppendLine("Changes:");

        foreach (var fc in change.FieldChanges)
        {
            switch (fc.Type)
            {
                case FieldChangeType.Added:
                    sb.AppendLine($"  - New field '{fc.FieldName}' ({fc.NewType}) was added.{(fc.HasDefault ? " It is optional, so old messages are still valid." : " It is required for new messages.")}");
                    break;
                case FieldChangeType.Removed:
                    sb.AppendLine($"  - Field '{fc.FieldName}' ({fc.OldType}) was removed. Consumers referencing this field must be updated.");
                    break;
                case FieldChangeType.TypeChanged:
                    sb.AppendLine($"  - Field '{fc.FieldName}' changed type from {fc.OldType} to {fc.NewType}. Deserialization logic must be updated.");
                    break;
                case FieldChangeType.Renamed:
                    sb.AppendLine($"  - Field '{fc.OldFieldName}' was renamed to '{fc.FieldName}'. Update property names and JSON mappings.");
                    break;
                case FieldChangeType.MadeNullable:
                    sb.AppendLine($"  - Field '{fc.FieldName}' is now nullable. Add null-safety checks.");
                    break;
                case FieldChangeType.MadeRequired:
                    sb.AppendLine($"  - Field '{fc.FieldName}' is now required. Ensure all producers provide this field.");
                    break;
            }
        }

        return sb.ToString();
    }

    private static string GenerateRuleBasedMigrationSuggestion(SchemaChange change)
    {
        var sb = new StringBuilder();

        sb.AppendLine("Recommended migration strategy:");
        sb.AppendLine();

        if (change.Breaking == BreakingLevel.Major)
        {
            sb.AppendLine("1. Deploy updated consumers BEFORE producers start sending v" + change.NewVersion + " messages.");
            sb.AppendLine("2. Use a dual-read approach: try deserializing as v" + change.NewVersion + " first, fall back to v" + change.OldVersion + ".");
            sb.AppendLine("3. Consider using the Schema Registry's compatibility mode to prevent future breaking changes.");
        }
        else
        {
            sb.AppendLine("1. Update consumer model classes to include new fields (they will default to null/zero for old messages).");
            sb.AppendLine("2. Deploy updated consumers — they are backward compatible with v" + change.OldVersion + " messages.");
            sb.AppendLine("3. Update producers to start populating new fields.");
        }

        return sb.ToString();
    }
}
