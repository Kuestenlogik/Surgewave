namespace Kuestenlogik.Surgewave.Samples.PipelineAsCode;

/// <summary>
/// The payload shape flowing through the <c>orders</c> topic. The type is never deployed
/// anywhere — it only gives the DSL something to type-check filters and mappings against,
/// and its property names (camelCased) become the JSON paths in the generated conditions.
/// </summary>
public sealed record OrderEvent(
    string OrderId,
    string CustomerId,
    string Status,
    double Amount);
