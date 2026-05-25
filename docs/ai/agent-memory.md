# Agent Memory & Tool Caching

Surgewave provides persistent memory and tool result caching for AI agents, enabling agents to learn from interactions, recall facts, and avoid redundant tool calls.

## Agent Memory

### IAgentMemoryStore

The core interface for persistent agent memory:

```csharp
public interface IAgentMemoryStore
{
    Task SaveMemoryAsync(string agentId, MemoryEntry entry, CancellationToken ct = default);
    Task<MemoryEntry?> GetMemoryAsync(string agentId, string memoryId, CancellationToken ct = default);
    Task DeleteMemoryAsync(string agentId, string memoryId, CancellationToken ct = default);
    IAsyncEnumerable<MemoryEntry> SearchMemoriesAsync(string agentId, MemoryQuery query, CancellationToken ct = default);
    IAsyncEnumerable<MemoryEntry> ListMemoriesAsync(string agentId, MemoryType? type = null, CancellationToken ct = default);
    Task<MemorySummary> GetSummaryAsync(string agentId, CancellationToken ct = default);
}
```

### Backends

| Backend | Class | Description |
|---------|-------|-------------|
| In-Memory | `InMemoryAgentMemoryStore` | Thread-safe `ConcurrentDictionary` storage with TTL expiration and access tracking |
| File | `FileAgentMemoryStore` | JSON file-backed storage for persistence across restarts |

### Memory Types

Each memory entry is classified by type:

| Type | Description |
|------|-------------|
| `ConversationSummary` | Summary of a previous conversation |
| `LearnedFact` | A fact discovered during agent interactions |
| `UserPreference` | A user preference detected during interactions |
| `ToolResult` | The result of a tool invocation |
| `Episodic` | A memory tied to a specific session or event |
| `Procedural` | Instructions describing how to perform a task |

### AgentMemoryContext

A convenience wrapper that scopes memory operations to a specific agent:

```csharp
var store = new InMemoryAgentMemoryStore();
var options = new MemoryOptions { DefaultMemoryTtl = TimeSpan.FromDays(30) };
var context = new AgentMemoryContext(store, "agent-1", options);

// Save different types of memories
await context.SaveFactAsync("User prefers dark mode", importance: 0.8f);
await context.SavePreferenceAsync("Prefers concise responses");
await context.SaveEpisodeAsync("Discussed deployment strategy", sessionId: "session-42");

// Recall memories by text query
var results = await context.RecallAsync("deployment", maxResults: 5);

// Recall by type
var facts = await context.RecallByTypeAsync(MemoryType.LearnedFact, maxResults: 10);

// Forget a specific memory
await context.ForgetAsync(memoryId);

// Get summary statistics
var summary = await context.GetSummaryAsync();
// summary.TotalMemories, summary.ByType, summary.OldestMemory, summary.NewestMemory
```

### Memory Search

Memories can be searched with multiple criteria:

```csharp
var query = new MemoryQuery(
    TextQuery: "deployment",
    Type: MemoryType.LearnedFact,
    MaxResults: 10,
    MinImportance: 0.5f,
    SortBy: MemorySortOrder.Relevance,
    CreatedAfter: DateTimeOffset.UtcNow.AddDays(-7)
);

await foreach (var entry in store.SearchMemoriesAsync("agent-1", query))
{
    Console.WriteLine($"[{entry.Type}] {entry.Content} (importance: {entry.Importance})");
}
```

Sort orders:
- `Relevance` - Text match strength blended with importance score
- `Recency` - Most recent first
- `Importance` - Highest importance first
- `Frequency` - Most accessed first

## Conversation Summarizer

`ConversationSummarizer` produces extractive summaries of conversation history without requiring an LLM call:

```csharp
var history = new List<AgentMessage>
{
    new("user", "How do I deploy Surgewave to Kubernetes?"),
    new("assistant", "You can use Helm charts or plain manifests..."),
    new("user", "What about monitoring?"),
    new("assistant", "Surgewave exposes Prometheus metrics on port 9093...")
};

var summary = ConversationSummarizer.Summarize(history);
// "Conversation with 4 messages (2 user, 2 assistant). User asked 2 question(s).
//  Topics: deploy, kubernetes, monitoring. Actions: provided explanations."
```

For short conversations (3 or fewer messages), it produces a truncated transcript. For longer conversations, it extracts:
- Message counts by role
- Number of user questions
- Top 5 topics via keyword extraction
- Top 3 assistant action categories

## Tool Result Caching

### CachedAgentTool

A decorator that transparently caches tool results:

```csharp
var cache = new InMemoryToolResultCache(new ToolCacheOptions
{
    DefaultTtl = TimeSpan.FromMinutes(5),
    MaxCachedEntries = 1000
});

var cachedTool = new CachedAgentTool(originalTool, cache, TimeSpan.FromMinutes(5));
var result = await cachedTool.InvokeAsync(arguments);
// Subsequent calls with the same arguments return cached results
```

Cache keys are computed by sorting argument keys, serializing to JSON, and hashing with SHA-256. Error results are never cached.

### InMemoryToolResultCache

In-memory cache with TTL expiration and LRU-style eviction:

```csharp
var cache = new InMemoryToolResultCache(new ToolCacheOptions
{
    DefaultTtl = TimeSpan.FromMinutes(5),
    MaxCachedEntries = 1000
});

// Get cache statistics
var stats = await cache.GetStatsAsync();
// stats.TotalEntries, stats.Hits, stats.Misses

// Invalidate specific entries
await cache.InvalidateAsync("tool-name", cacheKey);

// Invalidate all entries for a tool
await cache.InvalidateAsync("tool-name");
```

When the cache exceeds `MaxCachedEntries`, the entry with the earliest expiration time is evicted.

### CachingToolProvider

Wraps an entire `IAgentToolProvider` to automatically cache all tool results:

```csharp
var cachingProvider = new CachingToolProvider(
    innerProvider,
    cache,
    new ToolCacheOptions
    {
        DefaultTtl = TimeSpan.FromMinutes(5),
        MaxCachedEntries = 1000,
        ExcludedTools = { "current-time" },  // Skip caching for specific tools
        ToolOverrides =
        {
            ["weather"] = new ToolCachePolicy
            {
                Ttl = TimeSpan.FromMinutes(30)
            },
            ["stock-price"] = new ToolCachePolicy
            {
                Disabled = true  // Never cache
            }
        }
    });

var tools = await cachingProvider.GetToolsAsync();
// All tools are wrapped with CachedAgentTool (except excluded ones)
```

## Next Steps

- [Guardrails](guardrails.md) - Content safety for AI pipelines
- [Pipeline Chat](pipeline-chat.md) - Interactive chat with AI pipelines
- [Agent Integration](agent-integration.md) - Multi-agent architectures
