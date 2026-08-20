namespace Kuestenlogik.Surgewave.Schema.Registry.Vectors;

/// <summary>
/// Element type of a <see cref="VectorType"/>. Avro and Protobuf derive it from the declared
/// element type (float → F32, double → F64); JSON Schema declares it explicitly via
/// <c>x-vector-dtype</c> because JSON numbers are typeless. F16/I8/U8 exist for declarative
/// schemas over quantized embeddings — payload representation is the format's business.
/// </summary>
public enum VectorDtype
{
    F32,
    F64,
    F16,
    I8,
    U8,
}
