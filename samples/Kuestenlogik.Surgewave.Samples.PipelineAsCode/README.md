# Samples.PipelineAsCode — pipelines as a C# library

A Connect pipeline defined entirely in C# with the
`Kuestenlogik.Surgewave.Pipelines` DSL: code-reviewable, git-versionable,
unit-testable — and buildable **without a running broker**.

```csharp
public sealed class HighValueOrdersPipeline : ISurgewavePipeline
{
    public BuiltPipeline Define() => Pipeline
        .From<OrderEvent>("orders")
        .Named("high-value-orders")
        .Filter(o => o.Status == "active" && o.Amount > 1000)
        .Map(m => m.Field("customer", o => o.CustomerId))
        .OnError("orders-high-value-dlq")
        .To("orders-high-value")
        .Build();
}
```

## The authoring decisions, in order

1. **`ISurgewavePipeline` is the packaging contract.** Any class
   implementing it is discovered by `surgewave pipelines deploy <dll>`.
   One library can carry any number of pipelines.
2. **`Pipeline.From<OrderEvent>("orders")`** starts a typed flow. The
   payload type is never deployed — it only type-checks the lambdas and
   supplies the JSON property names (camelCased by default, override
   with `[JsonPropertyName]` or `.Builder.WithNamingPolicy(...)`).
3. **`Filter(o => ...)`** translates the lambda into the broker's
   condition syntax (`$.amount > 1000`). `&&` becomes chained filter
   nodes; `||` is not expressible server-side and fails at build time
   with a hint.
4. **`OnError(...)`** attaches a dead-letter sink via an error
   connection; **`WithRetry(...)`** attaches a retry policy to the
   preceding node.
5. **`Build()`** validates the graph (cycles, dangling connections),
   assigns editor layout positions, and freezes the definition. The
   result opens cleanly in the visual pipeline editor.

## Deploying

```bash
dotnet build

# straight from the compiled library (broker needed only now):
surgewave pipelines deploy bin/Debug/net10.0/Kuestenlogik.Surgewave.Samples.PipelineAsCode.dll --start

# hot reload on save while iterating:
surgewave pipelines deploy . --watch

# or produce a reviewable artifact instead and import it later (CLI or Control UI):
# new HighValueOrdersPipeline().Define().Save("high-value-orders.pipeline.json")
surgewave pipelines deploy high-value-orders.pipeline.json
```

Programmatic deployment uses `PipelinePublisher` from the same package —
see `docs/cookbook/pipeline-as-code.md` for the full recipe.

## Testing

`tests/HighValueOrdersPipelineTests.cs` asserts on the built definition —
node configs, the split `&&` filter, the error routing — without any
broker. `Define().ToJson()` is deterministic, so exported files diff
cleanly in git.
