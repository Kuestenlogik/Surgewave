using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Control.Models;

/// <summary>
/// Saved agent configuration for the Agent Design Studio UI.
/// </summary>
public sealed class AgentConfig : IValidatableConfig
{
    [Required]
    [MinLength(1)]
    public string Id { get; init; } = Guid.NewGuid().ToString("N");

    [Required]
    [MinLength(1)]
    public required string Name { get; set; }

    public string? Description { get; set; }
    public DateTimeOffset CreatedAt { get; init; } = DateTimeOffset.UtcNow;
    public bool IsDeployed { get; set; }
    public string? PipelineId { get; set; }

    // Persona
    public AgentPersona Persona { get; set; } = new();

    // Core behavior
    public string SystemPrompt { get; set; } = "";
    public List<string> Instructions { get; set; } = []; // reusable instruction blocks
    public List<FewShotExample> Examples { get; set; } = []; // few-shot examples

    // Model
    [Required]
    [MinLength(1)]
    public string Model { get; set; } = "gpt-4";

    [Required]
    [MinLength(1)]
    public string Provider { get; set; } = "openai";

    public string? ApiKey { get; set; }

    [Range(0.0, 2.0)]
    public double Temperature { get; set; } = 0.7;

    [Range(1, 100)]
    public int MaxTurns { get; set; } = 5;

    // Tools (new structured list)
    public List<AgentToolConfig> ToolConfigs { get; set; } = [];

    // Legacy tools (kept for backward compatibility with saved configs)
    public List<string> Tools { get; set; } = [];

    // Knowledge Base
    public List<KnowledgeSource> KnowledgeSources { get; set; } = [];

    // Output
    public AgentOutputConfig Output { get; set; } = new();

    // Guardrails
    public AgentGuardrailConfig Guardrails { get; set; } = new();

    // Workflow Rules
    public List<WorkflowRule> WorkflowRules { get; set; } = [];

    // Memory
    public bool MemoryEnabled { get; set; }

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}

/// <summary>
/// Agent persona configuration for display and behavioral tone.
/// </summary>
public sealed class AgentPersona
{
    public string? DisplayName { get; set; }
    public string? Avatar { get; set; } // MudBlazor icon name or emoji
    public string Tonality { get; set; } = "Professional"; // Professional, Casual, Technical, Friendly
    public string Language { get; set; } = "auto"; // auto, en, de, etc.
}

/// <summary>
/// A few-shot example demonstrating expected agent behavior.
/// </summary>
public sealed class FewShotExample
{
    public required string UserMessage { get; set; }
    public required string AssistantResponse { get; set; }
}

/// <summary>
/// Structured tool configuration for an agent.
/// </summary>
public sealed class AgentToolConfig
{
    public required string Name { get; set; }
    public required string Type { get; set; } // "mcp", "surgewave-topic", "http"
    public string? Uri { get; set; } // MCP server URI or HTTP endpoint
    public string? TopicName { get; set; } // for surgewave-topic tools
    public string? Description { get; set; }
}

/// <summary>
/// Knowledge source for agent RAG/context augmentation.
/// </summary>
public sealed class KnowledgeSource
{
    public required string Name { get; set; }
    public required string Type { get; set; } // "topic", "document", "url"
    public string? TopicName { get; set; }
    public string? Content { get; set; } // inline text content
    public string? Url { get; set; }
}

/// <summary>
/// Output format and constraints for the agent.
/// </summary>
public sealed class AgentOutputConfig
{
    public string Format { get; set; } = "text"; // "text", "json", "markdown"
    public string? JsonSchema { get; set; } // JSON Schema for structured output
    public int? MaxLength { get; set; }
}

/// <summary>
/// Guardrail configuration for agent safety.
/// </summary>
public sealed class AgentGuardrailConfig
{
    public bool PiiDetection { get; set; }
    public bool ToxicityFilter { get; set; }
    public bool PromptInjectionDetection { get; set; }
    public int? MaxInputLength { get; set; }
    public int? MaxOutputLength { get; set; }
}

/// <summary>
/// Conditional workflow rule for agent behavior.
/// </summary>
public sealed class WorkflowRule
{
    public required string Name { get; set; }
    public required string Trigger { get; set; } // "contains:keyword", "intent:greeting", "regex:pattern"
    public required string Action { get; set; } // "use-tool:toolName", "respond:text", "escalate", "reject"
    public string? ActionParameter { get; set; }
}
