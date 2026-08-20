namespace Kuestenlogik.Surgewave.Pipelines;

/// <summary>
/// Entry point of the pipeline-as-code DSL. Pipelines built here are plain definitions —
/// no broker is needed until they are published:
/// <code>
/// var pipeline = Pipeline.From&lt;OrderEvent&gt;("orders")
///     .Named("high-value-orders")
///     .Filter(o =&gt; o.Amount &gt; 1000)
///     .To("orders-high-value")
///     .Build();
///
/// pipeline.Save("high-value-orders.pipeline.json");   // deploy later via CLI or UI import
/// </code>
/// </summary>
public static class Pipeline
{
    /// <summary>Creates a named pipeline builder for explicit graph construction.</summary>
    public static PipelineBuilder Create(string name)
    {
        return new PipelineBuilder(name);
    }

    /// <summary>
    /// Starts a typed flow reading payloads of <typeparamref name="T"/> from one or more topics.
    /// Name the pipeline with <c>.Named(...)</c> before exporting or publishing.
    /// </summary>
    public static PipelineFlow<T> From<T>(params string[] topics)
    {
        return new PipelineBuilder().FromTopic<T>(topics);
    }

    /// <summary>Starts an untyped flow reading from one or more topics.</summary>
    public static PipelineFlow From(params string[] topics)
    {
        return new PipelineBuilder().FromTopic(topics);
    }
}
