using Kuestenlogik.Surgewave.Pipelines;

namespace Kuestenlogik.Surgewave.Samples.PipelineAsCode;

/// <summary>
/// A pipeline as code: reads <c>orders</c>, keeps active orders above 1000, reshapes the
/// record, and writes to <c>orders-high-value</c>. Failed records land in a dead-letter topic.
///
/// Build the library, then deploy without writing any JSON by hand:
/// <code>
/// dotnet build
/// surgewave pipelines deploy bin/Debug/net10.0/Kuestenlogik.Surgewave.Samples.PipelineAsCode.dll --start
/// </code>
/// Or export the definition to a file for git review and UI import:
/// <code>
/// new HighValueOrdersPipeline().Define().Save("high-value-orders.pipeline.json");
/// </code>
/// </summary>
public sealed class HighValueOrdersPipeline : ISurgewavePipeline
{
    public BuiltPipeline Define() => Pipeline
        .From<OrderEvent>("orders")
        .Named("high-value-orders")
        .DescribedAs("Routes high-value active orders into their own topic")
        .Filter(o => o.Status == "active" && o.Amount > 1000)
        .Map(m => m
            .Field("order", o => o.OrderId)
            .Field("customer", o => o.CustomerId)
            .Field("amount", o => o.Amount))
        .WithRetry(maxRetries: 3, backoff: TimeSpan.FromSeconds(1))
        .OnError("orders-high-value-dlq")
        .To("orders-high-value")
        .Build();
}
