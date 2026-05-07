namespace Kuestenlogik.Surgewave.Control.Models;

/// <summary>
/// Predefined reusable instruction blocks for agent behavior configuration.
/// </summary>
public static class InstructionTemplates
{
    public static readonly Dictionary<string, string> Templates = new()
    {
        ["concise"] = "Be concise and direct. Avoid unnecessary filler words.",
        ["german"] = "Always respond in German (Deutsch).",
        ["english"] = "Always respond in English.",
        ["sources"] = "Include source references when citing facts or data.",
        ["bullets"] = "Format your response as bullet points when listing multiple items.",
        ["step-by-step"] = "Break down complex tasks into numbered steps.",
        ["no-assumptions"] = "Ask clarifying questions instead of making assumptions.",
        ["code-examples"] = "Include code examples when explaining technical concepts.",
        ["json-output"] = "Return your response as valid JSON.",
        ["safety"] = "If asked about harmful activities, politely decline and redirect.",
        ["context-only"] = "Only answer based on the provided context. Say 'I don't know' if the answer is not in the context.",
        ["summarize"] = "Summarize long texts to key points (max 3-5 sentences).",
    };

    /// <summary>
    /// Human-readable label for each template key.
    /// </summary>
    public static readonly Dictionary<string, string> Labels = new()
    {
        ["concise"] = "Be Concise",
        ["german"] = "Respond in German",
        ["english"] = "Respond in English",
        ["sources"] = "Include Sources",
        ["bullets"] = "Bullet Points",
        ["step-by-step"] = "Step-by-Step",
        ["no-assumptions"] = "No Assumptions",
        ["code-examples"] = "Code Examples",
        ["json-output"] = "JSON Output",
        ["safety"] = "Safety First",
        ["context-only"] = "Context Only",
        ["summarize"] = "Summarize",
    };
}
