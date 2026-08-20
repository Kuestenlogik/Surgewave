# Pipeline as Code (C# DSL)

Connect pipelines don't have to be clicked together in the visual editor — the
`Kuestenlogik.Surgewave.Pipelines` package gives you a fluent, typed C# DSL that
builds the same pipeline definitions in code. Pipelines become code-reviewable,
git-versionable and refactorable; the visual editor stays for prototyping and
for everyone who prefers dragging nodes.

Building needs **no running broker**. You only need one to deploy.

## 1. Define a pipeline

```csharp
using Kuestenlogik.Surgewave.Pipelines;

public sealed record OrderEvent(string OrderId, string CustomerId, string Status, double Amount);

public sealed class HighValueOrders : ISurgewavePipeline
{
    public BuiltPipeline Define() => Pipeline
        .From<OrderEvent>("orders")
        .Named("high-value-orders")
        .Filter(o => o.Status == "active" && o.Amount > 1000)
        .Map(m => m
            .Field("order", o => o.OrderId)
            .Field("customer", o => o.CustomerId))
        .WithRetry(maxRetries: 3)
        .OnError("orders-high-value-dlq")
        .To("orders-high-value")
        .Build();
}
```

The payload type is never deployed — it type-checks your lambdas and supplies
the JSON property names (camelCased by default; `[JsonPropertyName]` wins, and
`.Builder.WithNamingPolicy(null)` keeps PascalCase).

`Filter` translates the lambda into the broker's condition syntax
(`$.amount > 1000`). Supported: comparisons of a payload property against a
constant (`==`, `!=`, `>`, `<`, `>=`, `<=`), string
`Contains`/`StartsWith`/`EndsWith` (case-insensitive on the broker), bare
boolean properties, `x.HasValue` on nullables (becomes a null comparison),
`!`, and `&&` — which becomes chained filter nodes. `||` fails at build time;
use the raw condition-string overload (`.Filter("$.status == 'active'")`) or
restructure. Anything that has no JSON path (`.Length`, `.Count`, DateTime
parts) is rejected at build time rather than silently never matching.

Two semantic edges worth knowing: a record whose payload is not valid JSON
never matches any condition, and a record *missing* the compared field
follows C# null semantics for `==`/`!=` against values (`x != "v"` passes)
but fails relational comparisons and `x != null`.

## 2. Stages beyond Filter and Map

| Stage | Node behind it |
|---|---|
| `Deduplicate(o => o.CustomerId, window)` | DeduplicateNode |
| `RateLimit(1000, TimeSpan.FromSeconds(1))` | RateLimiterNode |
| `ExtractField(o => o.Payload)` | ExtractFieldNode |
| `Flatten()`, `Cast("age:int32")`, `MaskFields("***", "ssn")`, `Split("$.items")`, `ValueToKey(...)` | the matching transform nodes |
| `RouteIf(o => o.Amount > 1000, "high", "low")` | IfNode (terminal) |
| `RouteBy(o => o.Status, cases, defaultTopic)` | SwitchNode (terminal) |
| `Through("Full.Type.Name", c => c.Set("key", "value"))` | **any** node, incl. plugin connectors |

Entry points: `Pipeline.From<T>(topics...)` (topic read),
`builder.FromSchedule("*/5 * * * *")` (cron), `builder.FromWebhook(port: 8888)`
(HTTP). Cross-cutting: `.WithParameter("region", "eu")` (referenced as
`${param.region}` in configs), `.WithSchedule(...)`, `.WithLabel(...)`.

Anything the fluent chain can't express is available on `.Builder` as an
explicit graph API (`AddNode`, `Connect`).

## 3. Export the result as an artifact

```csharp
var pipeline = new HighValueOrders().Define();

string json = pipeline.ToJson();                       // editor-compatible export JSON
pipeline.Save("high-value-orders.pipeline.json");      // same, to a file
```

The output is the standard pipeline export format — importable via
`POST /api/pipelines/import`, the Control UI's import dialog, or
`surgewave pipelines deploy <file>`. Exports are deterministic (fixed
timestamp), so the file diffs cleanly in git; layout positions are assigned
automatically so the pipeline opens tidily in the visual editor.

## 4. Deploy

**CLI — from the compiled library** (every `ISurgewavePipeline` in the
assembly is discovered and deployed):

```bash
dotnet build
surgewave pipelines deploy bin/Debug/net10.0/MyPipelines.dll --start

surgewave pipelines deploy my-pipeline.pipeline.json      # or from an export file
surgewave pipelines deploy ./MyPipelines/ --watch         # rebuild + redeploy on save
surgewave pipelines list
surgewave pipelines export high-value-orders -o reviewed.pipeline.json
surgewave pipelines start|stop high-value-orders
```

`deploy --replace` updates an existing pipeline of the same name (stopping and
restarting it when running); `--watch` implies `--replace`. The broker endpoint
comes from `--broker-url` / `SURGEWAVE_BROKER_URL` (default
`https://localhost:9093`) and needs `Surgewave:Connect:Enabled=true`.

**Programmatic — from your own tool or CI:**

```csharp
using Kuestenlogik.Surgewave.Pipelines.Publishing;

using var publisher = new PipelinePublisher(new Uri("https://localhost:9093"));
var result = await publisher.PublishAsync(pipeline, new PipelinePublishOptions
{
    Mode = PublishMode.ReplaceByName,   // redeploy semantics
    Start = true,
});
Console.WriteLine($"{result.Name} -> {result.PipelineId}");
```

## 5. Test pipelines like code

The built definition is plain data — assert on it without a broker:

```csharp
[Fact]
public void HighValueFilter_IsTranslated()
{
    var built = new HighValueOrders().Define();
    Assert.Equal("$.amount > 1000", built.Nodes[1].Config["condition"]);
}
```

A complete worked example lives in
[`samples/Kuestenlogik.Surgewave.Samples.PipelineAsCode`](https://github.com/Kuestenlogik/Surgewave/tree/main/samples/Kuestenlogik.Surgewave.Samples.PipelineAsCode).
