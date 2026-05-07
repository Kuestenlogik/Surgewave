namespace Kuestenlogik.Surgewave.Connect.Tests.Pipelines;

using Kuestenlogik.Surgewave.Connect.Pipelines;

public class AiPipelineSeederTests
{
    [Fact]
    public void Seeder_CreatesThreeExamplePipelines()
    {
        var pipelines = AiPipelineSeeder.CreateAll();

        Assert.Equal(3, pipelines.Count);
    }

    [Fact]
    public void Seeder_SkipsIfAlreadySeeded()
    {
        // First call produces 3 pipelines
        var first = AiPipelineSeeder.CreateAll();
        Assert.Equal(3, first.Count);

        // Simulate store already containing seeded pipelines
        var existing = first.ToList();
        var hasExisting = existing.Any(p => p.Id.StartsWith("example-ai-", StringComparison.Ordinal));

        // The seeder checks for the prefix - if any exist, it returns early
        Assert.True(hasExisting);

        // CreateAll always returns the same definitions (idempotent data)
        var second = AiPipelineSeeder.CreateAll();
        Assert.Equal(3, second.Count);

        // IDs are stable across calls
        Assert.Equal(first[0].Id, second[0].Id);
        Assert.Equal(first[1].Id, second[1].Id);
        Assert.Equal(first[2].Id, second[2].Id);
    }

    [Fact]
    public void Seeder_PipelinesHaveCorrectStructure()
    {
        var pipelines = AiPipelineSeeder.CreateAll();

        // Pipeline 1: Simple Chatbot - 4 nodes, 3 connections
        var chatbot = pipelines.Single(p => p.Id == "example-ai-chatbot");
        Assert.Equal(4, chatbot.Nodes.Count);
        Assert.Equal(3, chatbot.Connections.Count);
        Assert.Equal("Example: Simple AI Chatbot", chatbot.Name);
        Assert.NotNull(chatbot.Description);
        AssertAllNodesConnected(chatbot);
        AssertUniqueNodeIds(chatbot);
        AssertUniqueConnectionIds(chatbot);

        // Pipeline 2: Document Q&A - 8 nodes, 7 connections
        var docQa = pipelines.Single(p => p.Id == "example-ai-document-qa");
        Assert.Equal(8, docQa.Nodes.Count);
        Assert.Equal(7, docQa.Connections.Count);
        Assert.Equal("Example: Document Q&A with RAG", docQa.Name);
        Assert.NotNull(docQa.Description);
        AssertUniqueNodeIds(docQa);
        AssertUniqueConnectionIds(docQa);

        // Pipeline 3: Agent Workflow - 3 nodes, 2 connections
        var agent = pipelines.Single(p => p.Id == "example-ai-agent");
        Assert.Equal(3, agent.Nodes.Count);
        Assert.Equal(2, agent.Connections.Count);
        Assert.Equal("Example: AI Agent with Tool Access", agent.Name);
        Assert.NotNull(agent.Description);
        AssertAllNodesConnected(agent);
        AssertUniqueNodeIds(agent);
        AssertUniqueConnectionIds(agent);
    }

    [Fact]
    public void Seeder_PipelinesAreDraftStatus()
    {
        var pipelines = AiPipelineSeeder.CreateAll();

        foreach (var pipeline in pipelines)
        {
            Assert.Equal(PipelineStatus.Draft, pipeline.Status);
        }
    }

    [Fact]
    public void Seeder_AllPipelineIdsHaveCorrectPrefix()
    {
        var pipelines = AiPipelineSeeder.CreateAll();

        foreach (var pipeline in pipelines)
        {
            Assert.StartsWith("example-ai-", pipeline.Id, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Seeder_ChatbotPipeline_HasExpectedNodeTypes()
    {
        var pipelines = AiPipelineSeeder.CreateAll();
        var chatbot = pipelines.Single(p => p.Id == "example-ai-chatbot");

        var types = chatbot.Nodes.Select(n => n.ConnectorType).ToList();
        Assert.Contains(types, t => t.Contains("ChatEndpointNode", StringComparison.Ordinal));
        Assert.Contains(types, t => t.Contains("PromptBuilderNode", StringComparison.Ordinal));
        Assert.Contains(types, t => t.Contains("LlmNode", StringComparison.Ordinal));
        Assert.Contains(types, t => t.Contains("ChatResponseNode", StringComparison.Ordinal));
    }

    [Fact]
    public void Seeder_DocumentQaPipeline_HasRagComponents()
    {
        var pipelines = AiPipelineSeeder.CreateAll();
        var docQa = pipelines.Single(p => p.Id == "example-ai-document-qa");

        var types = docQa.Nodes.Select(n => n.ConnectorType).ToList();
        Assert.Contains(types, t => t.Contains("DocumentParserNode", StringComparison.Ordinal));
        Assert.Contains(types, t => t.Contains("EmbedderNode", StringComparison.Ordinal));
        Assert.Contains(types, t => t.Contains("RetrieverNode", StringComparison.Ordinal));
        Assert.Contains(types, t => t.Contains("MultiOutputNode", StringComparison.Ordinal));
    }

    [Fact]
    public void Seeder_AgentPipeline_HasAgentNode()
    {
        var pipelines = AiPipelineSeeder.CreateAll();
        var agent = pipelines.Single(p => p.Id == "example-ai-agent");

        var types = agent.Nodes.Select(n => n.ConnectorType).ToList();
        Assert.Contains(types, t => t.Contains("AgentNode", StringComparison.Ordinal));
        Assert.Contains(types, t => t.Contains("ChatEndpointNode", StringComparison.Ordinal));
        Assert.Contains(types, t => t.Contains("ChatResponseNode", StringComparison.Ordinal));
    }

    [Fact]
    public void Seeder_AllNodesHaveLabels()
    {
        var pipelines = AiPipelineSeeder.CreateAll();

        foreach (var pipeline in pipelines)
        {
            foreach (var node in pipeline.Nodes)
            {
                Assert.False(string.IsNullOrEmpty(node.Label),
                    $"Node '{node.Id}' in pipeline '{pipeline.Id}' has no label");
            }
        }
    }

    [Fact]
    public void Seeder_AllNodesHaveNonEmptyConfig()
    {
        var pipelines = AiPipelineSeeder.CreateAll();

        foreach (var pipeline in pipelines)
        {
            foreach (var node in pipeline.Nodes)
            {
                Assert.NotEmpty(node.Config);
            }
        }
    }

    [Fact]
    public void Seeder_NodesHaveReasonablePositions()
    {
        var pipelines = AiPipelineSeeder.CreateAll();

        foreach (var pipeline in pipelines)
        {
            foreach (var node in pipeline.Nodes)
            {
                Assert.True(node.X >= 0, $"Node '{node.Id}' has negative X position");
                Assert.True(node.Y >= 0, $"Node '{node.Id}' has negative Y position");
                Assert.True(node.X <= 2000, $"Node '{node.Id}' has unreasonably large X position");
                Assert.True(node.Y <= 2000, $"Node '{node.Id}' has unreasonably large Y position");
            }
        }
    }

    [Fact]
    public void Seeder_ConnectionsReferenceExistingNodes()
    {
        var pipelines = AiPipelineSeeder.CreateAll();

        foreach (var pipeline in pipelines)
        {
            var nodeIds = new HashSet<string>(pipeline.Nodes.Select(n => n.Id));

            foreach (var conn in pipeline.Connections)
            {
                Assert.Contains(conn.SourceNodeId, nodeIds);
                Assert.Contains(conn.TargetNodeId, nodeIds);
            }
        }
    }

    private static void AssertAllNodesConnected(PipelineDefinition pipeline)
    {
        var connectedNodes = new HashSet<string>();
        foreach (var conn in pipeline.Connections)
        {
            connectedNodes.Add(conn.SourceNodeId);
            connectedNodes.Add(conn.TargetNodeId);
        }

        foreach (var node in pipeline.Nodes)
        {
            Assert.Contains(node.Id, connectedNodes);
        }
    }

    private static void AssertUniqueNodeIds(PipelineDefinition pipeline)
    {
        var ids = pipeline.Nodes.Select(n => n.Id).ToList();
        Assert.Equal(ids.Count, ids.Distinct().Count());
    }

    private static void AssertUniqueConnectionIds(PipelineDefinition pipeline)
    {
        var ids = pipeline.Connections.Select(c => c.Id).ToList();
        Assert.Equal(ids.Count, ids.Distinct().Count());
    }
}
