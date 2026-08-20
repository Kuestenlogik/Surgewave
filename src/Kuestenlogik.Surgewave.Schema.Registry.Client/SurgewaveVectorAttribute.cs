namespace Kuestenlogik.Surgewave.Schema.Registry.Client;

/// <summary>
/// Declares a property or field as a vector (embedding) with a fixed dimension — the
/// client-side half of the first-class vector schema primitive (#14).
///
/// <para>The Avro and JSON serdes read this attribute when they generate the schema for
/// <c>T</c> and emit the format's vector annotation (Avro: <c>"logicalType": "vector",
/// "dim": N</c>; JSON Schema: <c>"format": "vector"</c> with <c>x-vector-dim</c>/<c>
/// x-vector-dtype</c>). The Schema Registry then enforces that dim and the element dtype
/// never change across schema versions. The element dtype is derived from the member type:
/// <c>float</c> elements are f32, <c>double</c> elements are f64.</para>
///
/// <example>
/// <code>
/// public sealed class DocumentChunk
/// {
///     public string Text { get; set; } = "";
///
///     [SurgewaveVector(768)]
///     public float[] Embedding { get; set; } = [];
/// }
/// </code>
/// </example>
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
public sealed class SurgewaveVectorAttribute : Attribute
{
    /// <summary>Declares the member as a vector of <paramref name="dim"/> elements.</summary>
    public SurgewaveVectorAttribute(int dim)
    {
        Dim = dim;
    }

    /// <summary>Number of elements every value of this field must have.</summary>
    public int Dim { get; }
}
