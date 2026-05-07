namespace Kuestenlogik.Surgewave.Connect.Pipelines;

/// <summary>
/// Seeds example AI pipelines so users can immediately explore AI capabilities.
/// Pipelines are created in Draft status - users must configure API keys before starting.
/// </summary>
public sealed class AiPipelineSeeder
{
    private const string IdPrefix = "example-ai-";

    private const string ChatEndpointType = "Kuestenlogik.Surgewave.AI.Nodes.ChatEndpointNode";
    private const string PromptBuilderType = "Kuestenlogik.Surgewave.AI.Nodes.PromptBuilderNode";
    private const string LlmNodeType = "Kuestenlogik.Surgewave.AI.Nodes.LlmNode";
    private const string ChatResponseType = "Kuestenlogik.Surgewave.AI.Nodes.ChatResponseNode";
    private const string DocumentParserType = "Kuestenlogik.Surgewave.AI.Nodes.DocumentParserNode";
    private const string EmbedderType = "Kuestenlogik.Surgewave.AI.Nodes.EmbedderNode";
    private const string RetrieverType = "Kuestenlogik.Surgewave.AI.Nodes.RetrieverNode";
    private const string AgentType = "Kuestenlogik.Surgewave.AI.Nodes.AgentNode";
    private const string MultiOutputType = "Kuestenlogik.Surgewave.Connect.Nodes.Workflow.MultiOutputNode";

    /// <summary>
    /// Seed example AI pipelines into the store if they do not already exist.
    /// </summary>
    public static async Task SeedAsync(PipelineStore store, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(store);

        var existing = store.GetAll();
        if (existing.Any(p => p.Id.StartsWith(IdPrefix, StringComparison.Ordinal)))
            return;

        await store.SaveAsync(CreateSimpleChatbot(), ct);
        await store.SaveAsync(CreateDocumentQaPipeline(), ct);
        await store.SaveAsync(CreateAgentWorkflow(), ct);
    }

    /// <summary>
    /// Returns the list of example pipeline definitions without persisting them.
    /// Useful for testing and previewing.
    /// </summary>
    public static IReadOnlyList<PipelineDefinition> CreateAll()
    {
        return [CreateSimpleChatbot(), CreateDocumentQaPipeline(), CreateAgentWorkflow()];
    }

    // ── Pipeline 1: Simple Chatbot ──────────────────────────────────────────

    internal static PipelineDefinition CreateSimpleChatbot()
    {
        var nodes = new List<PipelineNode>
        {
            new()
            {
                Id = "chat-input",
                ConnectorType = ChatEndpointType,
                Config = new Dictionary<string, string>
                {
                    ["mode"] = "websocket",
                    ["path"] = "/chat"
                },
                X = 100,
                Y = 200,
                Label = "Chat Input"
            },
            new()
            {
                Id = "prompt-builder",
                ConnectorType = PromptBuilderType,
                Config = new Dictionary<string, string>
                {
                    ["system.prompt"] = "You are a helpful assistant powered by Surgewave AI pipelines.",
                    ["template"] = "{{system}}\n\nUser: {{input}}\nAssistant:"
                },
                X = 350,
                Y = 200,
                Label = "Prompt Builder"
            },
            new()
            {
                Id = "llm",
                ConnectorType = LlmNodeType,
                Config = new Dictionary<string, string>
                {
                    ["provider"] = "openai",
                    ["model"] = "gpt-4o-mini",
                    ["api.key"] = "<YOUR_API_KEY>",
                    ["max.tokens"] = "1024",
                    ["temperature"] = "0.7"
                },
                X = 600,
                Y = 200,
                Label = "LLM"
            },
            new()
            {
                Id = "chat-response",
                ConnectorType = ChatResponseType,
                Config = new Dictionary<string, string>
                {
                    ["stream"] = "true"
                },
                X = 850,
                Y = 200,
                Label = "Chat Response"
            }
        };

        var connections = new List<PipelineConnection>
        {
            new() { Id = "c1", SourceNodeId = "chat-input", TargetNodeId = "prompt-builder" },
            new() { Id = "c2", SourceNodeId = "prompt-builder", TargetNodeId = "llm" },
            new() { Id = "c3", SourceNodeId = "llm", TargetNodeId = "chat-response" }
        };

        return new PipelineDefinition
        {
            Id = "example-ai-chatbot",
            Name = "Example: Simple AI Chatbot",
            Description = "A basic chatbot pipeline. Send messages via the Chat API and get AI-generated responses. Configure your API key in the LLM node to activate.",
            Nodes = nodes,
            Connections = connections,
            Status = PipelineStatus.Draft,
            CreatedAt = DateTimeOffset.UtcNow
        };
    }

    // ── Pipeline 2: Document Q&A with RAG ───────────────────────────────────

    internal static PipelineDefinition CreateDocumentQaPipeline()
    {
        var nodes = new List<PipelineNode>
        {
            new()
            {
                Id = "chat-input",
                ConnectorType = ChatEndpointType,
                Config = new Dictionary<string, string>
                {
                    ["mode"] = "websocket",
                    ["path"] = "/qa"
                },
                X = 100,
                Y = 200,
                Label = "Chat Input"
            },
            new()
            {
                Id = "router",
                ConnectorType = MultiOutputType,
                Config = new Dictionary<string, string>
                {
                    ["route.field"] = "type",
                    ["route.1.value"] = "document",
                    ["route.1.topic"] = "_example-ai-doc-qa-docs",
                    ["route.2.value"] = "query",
                    ["route.2.topic"] = "_example-ai-doc-qa-queries",
                    ["default.topic"] = "_example-ai-doc-qa-queries"
                },
                X = 300,
                Y = 200,
                Label = "Router"
            },
            new()
            {
                Id = "doc-parser",
                ConnectorType = DocumentParserType,
                Config = new Dictionary<string, string>
                {
                    ["chunk.size"] = "512",
                    ["chunk.overlap"] = "64",
                    ["formats"] = "pdf,txt,md,html"
                },
                X = 500,
                Y = 100,
                Label = "Document Parser"
            },
            new()
            {
                Id = "embedder",
                ConnectorType = EmbedderType,
                Config = new Dictionary<string, string>
                {
                    ["provider"] = "openai",
                    ["model"] = "text-embedding-3-small",
                    ["api.key"] = "<YOUR_API_KEY>",
                    ["dimensions"] = "1536"
                },
                X = 700,
                Y = 100,
                Label = "Embedder"
            },
            new()
            {
                Id = "retriever",
                ConnectorType = RetrieverType,
                Config = new Dictionary<string, string>
                {
                    ["top.k"] = "5",
                    ["similarity.threshold"] = "0.7",
                    ["store"] = "in-memory"
                },
                X = 500,
                Y = 300,
                Label = "Retriever"
            },
            new()
            {
                Id = "prompt-builder",
                ConnectorType = PromptBuilderType,
                Config = new Dictionary<string, string>
                {
                    ["system.prompt"] = "Answer the user's question based on the provided context. If the context does not contain the answer, say so.",
                    ["template"] = "{{system}}\n\nContext:\n{{context}}\n\nQuestion: {{input}}\nAnswer:"
                },
                X = 700,
                Y = 300,
                Label = "Prompt Builder"
            },
            new()
            {
                Id = "llm",
                ConnectorType = LlmNodeType,
                Config = new Dictionary<string, string>
                {
                    ["provider"] = "openai",
                    ["model"] = "gpt-4o-mini",
                    ["api.key"] = "<YOUR_API_KEY>",
                    ["max.tokens"] = "2048",
                    ["temperature"] = "0.3"
                },
                X = 900,
                Y = 300,
                Label = "LLM"
            },
            new()
            {
                Id = "chat-response",
                ConnectorType = ChatResponseType,
                Config = new Dictionary<string, string>
                {
                    ["stream"] = "true"
                },
                X = 1100,
                Y = 300,
                Label = "Chat Response"
            }
        };

        var connections = new List<PipelineConnection>
        {
            new() { Id = "c1", SourceNodeId = "chat-input", TargetNodeId = "router" },
            new() { Id = "c2", SourceNodeId = "router", TargetNodeId = "doc-parser" },
            new() { Id = "c3", SourceNodeId = "doc-parser", TargetNodeId = "embedder" },
            new() { Id = "c4", SourceNodeId = "router", TargetNodeId = "retriever" },
            new() { Id = "c5", SourceNodeId = "retriever", TargetNodeId = "prompt-builder" },
            new() { Id = "c6", SourceNodeId = "prompt-builder", TargetNodeId = "llm" },
            new() { Id = "c7", SourceNodeId = "llm", TargetNodeId = "chat-response" }
        };

        return new PipelineDefinition
        {
            Id = "example-ai-document-qa",
            Name = "Example: Document Q&A with RAG",
            Description = "Ask questions about documents. Upload documents via the signal topic, they get parsed, embedded and indexed. Then ask questions that are answered using retrieved context.",
            Nodes = nodes,
            Connections = connections,
            Status = PipelineStatus.Draft,
            CreatedAt = DateTimeOffset.UtcNow
        };
    }

    // ── Pipeline 3: Agent with Tools ────────────────────────────────────────

    internal static PipelineDefinition CreateAgentWorkflow()
    {
        var nodes = new List<PipelineNode>
        {
            new()
            {
                Id = "chat-input",
                ConnectorType = ChatEndpointType,
                Config = new Dictionary<string, string>
                {
                    ["mode"] = "websocket",
                    ["path"] = "/agent"
                },
                X = 100,
                Y = 200,
                Label = "Chat Input"
            },
            new()
            {
                Id = "agent",
                ConnectorType = AgentType,
                Config = new Dictionary<string, string>
                {
                    ["provider"] = "openai",
                    ["model"] = "gpt-4o",
                    ["api.key"] = "<YOUR_API_KEY>",
                    ["max.iterations"] = "10",
                    ["mcp.servers"] = "",
                    ["system.prompt"] = "You are a helpful AI agent that can use tools to accomplish tasks. Available MCP servers provide external tool access.",
                    ["tools.enabled"] = "true"
                },
                X = 400,
                Y = 200,
                Label = "AI Agent"
            },
            new()
            {
                Id = "chat-response",
                ConnectorType = ChatResponseType,
                Config = new Dictionary<string, string>
                {
                    ["stream"] = "true",
                    ["include.tool.calls"] = "true"
                },
                X = 700,
                Y = 200,
                Label = "Chat Response"
            }
        };

        var connections = new List<PipelineConnection>
        {
            new() { Id = "c1", SourceNodeId = "chat-input", TargetNodeId = "agent" },
            new() { Id = "c2", SourceNodeId = "agent", TargetNodeId = "chat-response" }
        };

        return new PipelineDefinition
        {
            Id = "example-ai-agent",
            Name = "Example: AI Agent with Tool Access",
            Description = "An autonomous AI agent that can use tools via MCP. Configure MCP server URLs and API keys to activate.",
            Nodes = nodes,
            Connections = connections,
            Status = PipelineStatus.Draft,
            CreatedAt = DateTimeOffset.UtcNow
        };
    }
}
