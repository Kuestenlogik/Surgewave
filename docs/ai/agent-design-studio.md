# Agent Design Studio

Visual agent builder with 6-tab configuration, test chat, and one-click deployment to AI pipelines.

## Overview

The Agent Design Studio is a visual builder in the Surgewave Control UI for designing, configuring, testing, and deploying AI agents. It provides a 6-tab interface that covers every aspect of agent configuration -- from persona and behavior to tools, knowledge bases, output formats, and safety guardrails. Agents are saved as `AgentConfig` JSON and can be deployed as pipeline nodes with one click.

Key characteristics:

- **6-tab visual builder**: Persona, Behavior, Model & Tools, Knowledge Base, Output & Guardrails, Test Chat
- **12 instruction templates**: Pre-built system prompt building blocks for common agent behaviors
- **Few-shot examples**: Define user/assistant example pairs to guide agent behavior
- **Three tool types**: MCP servers, Surgewave topics, and HTTP endpoints
- **Knowledge sources**: Topics, documents, and URLs for RAG-style context augmentation
- **Built-in guardrails**: PII detection, toxicity filtering, prompt injection detection
- **Test chat**: Interactive chat panel for validating agent behavior before deployment
- **Export/Import**: Save and share agent configurations as JSON

## Tab Overview

### Tab 1: Persona

Define how the agent presents itself to users:

| Field | Description |
|-------|-------------|
| **Name** | Agent name (required) |
| **Display Name** | Public-facing name shown in chat |
| **Avatar** | MudBlazor icon name or emoji for visual identity |
| **Tonality** | Communication style: Professional, Casual, Technical, or Friendly |
| **Language** | Response language: auto-detect, English, German, etc. |
| **Description** | Internal description of the agent's purpose |

### Tab 2: Behavior

Configure the agent's core behavior:

- **System Prompt**: Free-form system message that defines the agent's role and constraints
- **Instruction Templates**: 12 reusable instruction blocks that can be toggled on/off:
  - Answer questions concisely
  - Always cite sources
  - Respond in the user's language
  - Refuse off-topic questions
  - Ask clarifying questions
  - Provide step-by-step explanations
  - Use markdown formatting
  - Include code examples
  - Summarize long content
  - Be empathetic and supportive
  - Maintain conversation context
  - Escalate when uncertain
- **Few-Shot Examples**: User/assistant message pairs that demonstrate expected behavior

### Tab 3: Model & Tools

Select the LLM provider and configure tool access:

| Setting | Options |
|---------|---------|
| **Provider** | OpenAI, Ollama, Azure OpenAI, Anthropic |
| **Model** | Provider-specific model selection (e.g., gpt-4, llama3) |
| **Temperature** | 0.0 - 2.0 slider for response creativity |
| **Max Turns** | Maximum conversation turns before completion |
| **API Key** | Provider API key (stored securely) |

**Tool Types**:

| Type | Configuration | Description |
|------|---------------|-------------|
| **MCP** | Server URI | Model Context Protocol server for structured tool calls |
| **Surgewave Topic** | Topic name | Read/write Surgewave topics as tool actions |
| **HTTP** | Endpoint URL | Call external HTTP APIs as tools |

### Tab 4: Knowledge Base

Add context sources for RAG-style augmentation:

| Source Type | Configuration | Description |
|-------------|---------------|-------------|
| **Topic** | Topic name | Consume messages from a Surgewave topic as context |
| **Document** | Inline text | Paste document content directly |
| **URL** | Web URL | Fetch and index web content |

### Tab 5: Output & Guardrails

Control output format and safety:

**Output Configuration**:

| Setting | Options | Description |
|---------|---------|-------------|
| **Format** | text, json, markdown | Response format |
| **JSON Schema** | JSON Schema string | Structured output validation |
| **Max Length** | integer | Maximum response length |

**Guardrails**:

| Guardrail | Description |
|-----------|-------------|
| **PII Detection** | Detect and redact personally identifiable information |
| **Toxicity Filter** | Block toxic, harmful, or offensive content |
| **Prompt Injection Detection** | Detect and block prompt injection attempts |
| **Max Input Length** | Limit user input length |
| **Max Output Length** | Limit agent response length |

### Tab 6: Test Chat

Interactive chat panel for testing the agent configuration before deployment:

- Send test messages and see agent responses in real-time
- Conversation history is maintained during the test session
- Reset conversation to start fresh
- Tool calls are displayed inline for debugging
- Guardrail activations are highlighted with warnings

## AgentConfig Model

Agent configurations are persisted as `AgentConfig` objects:

```csharp
var config = new AgentConfig
{
    Name = "Support Agent",
    Persona = new AgentPersona
    {
        DisplayName = "Surgewave Support",
        Avatar = "HeadsetMic",
        Tonality = "Professional",
        Language = "en"
    },
    SystemPrompt = "You are a helpful support agent for Surgewave messaging platform.",
    Instructions = ["Answer questions concisely", "Always cite sources"],
    Examples =
    [
        new FewShotExample
        {
            UserMessage = "How do I create a topic?",
            AssistantResponse = "Use `surgewave topics create my-topic` or the REST API..."
        }
    ],
    Provider = "openai",
    Model = "gpt-4",
    Temperature = 0.3,
    MaxTurns = 10,
    ToolConfigs =
    [
        new AgentToolConfig
        {
            Name = "docs-search",
            Type = "mcp",
            Uri = "http://localhost:8080/mcp",
            Description = "Search Surgewave documentation"
        },
        new AgentToolConfig
        {
            Name = "topic-reader",
            Type = "surgewave-topic",
            TopicName = "support-tickets",
            Description = "Read recent support tickets"
        }
    ],
    KnowledgeSources =
    [
        new KnowledgeSource
        {
            Name = "FAQ",
            Type = "document",
            Content = "Q: What ports does Surgewave use? A: 9092 (broker), 5050 (control)..."
        }
    ],
    Output = new AgentOutputConfig
    {
        Format = "markdown",
        MaxLength = 2000
    },
    Guardrails = new AgentGuardrailConfig
    {
        PiiDetection = true,
        ToxicityFilter = true,
        PromptInjectionDetection = true,
        MaxInputLength = 4000,
        MaxOutputLength = 2000
    }
};
```

## Deployment

### Deploy as Pipeline Node

From the Agent Design Studio, click **Deploy** to create an AI pipeline with the agent as a processing node. The studio generates:

1. A pipeline definition with the agent's configuration
2. Source/sink topic bindings from the knowledge base and tool configs
3. Guardrail nodes wired before and after the agent node

### Export JSON

Export the agent configuration as a JSON file for version control or sharing:

```bash
# Download via API
curl http://localhost:5050/api/agents/{id}/export > my-agent.json

# Import
curl -X POST http://localhost:5050/api/agents/import \
  -H "Content-Type: application/json" \
  -d @my-agent.json
```

## Workflow Rules

Agents support conditional workflow rules for automated behavior:

```csharp
WorkflowRules =
[
    new WorkflowRule
    {
        Name = "Greeting",
        Trigger = "intent:greeting",
        Action = "respond:text",
        ActionParameter = "Hello! How can I help you today?"
    },
    new WorkflowRule
    {
        Name = "Escalation",
        Trigger = "contains:speak to human",
        Action = "escalate"
    }
]
```

| Trigger Format | Description |
|----------------|-------------|
| `contains:keyword` | Message contains the keyword |
| `intent:name` | Detected intent matches |
| `regex:pattern` | Message matches regex pattern |

| Action | Description |
|--------|-------------|
| `use-tool:toolName` | Invoke a specific tool |
| `respond:text` | Respond with fixed text |
| `escalate` | Escalate to a human operator |
| `reject` | Reject the message |

## Use Cases

- **Customer support agents**: Build support bots with FAQ knowledge and ticket tools
- **Data pipeline assistants**: Create agents that monitor and explain pipeline behavior
- **Internal knowledge bots**: Connect agents to internal documentation and APIs
- **Compliance agents**: Deploy agents with strict guardrails for regulated industries

## Next Steps

- [Pipeline Chat](pipeline-chat.md) - Interactive chat with deployed AI pipelines
- [Guardrails](guardrails.md) - Content safety (PII, toxicity, prompt injection)
- [Agent Integration](agent-integration.md) - Multi-agent architectures with Surgewave
- [Agent Memory](agent-memory.md) - Persistent memory across conversations
