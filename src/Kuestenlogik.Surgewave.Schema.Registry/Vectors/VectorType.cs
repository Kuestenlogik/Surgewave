namespace Kuestenlogik.Surgewave.Schema.Registry.Vectors;

/// <summary>
/// A vector (embedding) declared as a first-class schema primitive: a fixed dimension count and
/// an element dtype (#14). The three format handlers map their own syntax onto this one model —
/// Avro <c>"logicalType": "vector", "dim": 768</c> on an array of float/double, JSON Schema
/// <c>"format": "vector"</c> with <c>x-vector-dim</c>/<c>x-vector-dtype</c>, Protobuf
/// <c>[(surgewave.vector).dim = 768]</c> on a repeated float/double field — so validation and
/// the evolution rule live here once instead of three times.
///
/// <para>The evolution rule is deliberately strict equality: a vector field's dimension and
/// dtype must never change, and vector-ness itself must not appear on or disappear from an
/// existing field. Readers of an embedding column allocate, index, and compute against dim —
/// a 768→1536 "widening" silently corrupts every consumer that survived the type check, which
/// is why this is a compatibility error and not a warning.</para>
/// </summary>
public readonly record struct VectorType(int Dim, VectorDtype Dtype)
{
    /// <summary>Largest accepted dimension — a sanity bound, not a technical limit.</summary>
    public const int MaxDim = 1 << 20;

    /// <summary>Validates a dimension value; null when valid, otherwise the error text.</summary>
    public static string? ValidateDim(long dim) => dim switch
    {
        < 1 => $"vector dim must be a positive integer, got {dim}",
        > MaxDim => $"vector dim {dim} exceeds the maximum of {MaxDim}",
        _ => null,
    };

    /// <summary>Parses a dtype name ("f32", "f64", "f16", "i8", "u8"), case-insensitively.</summary>
    public static bool TryParseDtype(string? name, out VectorDtype dtype)
    {
        switch (name?.Trim().ToLowerInvariant())
        {
            case "f32": dtype = VectorDtype.F32; return true;
            case "f64": dtype = VectorDtype.F64; return true;
            case "f16": dtype = VectorDtype.F16; return true;
            case "i8": dtype = VectorDtype.I8; return true;
            case "u8": dtype = VectorDtype.U8; return true;
            default: dtype = default; return false;
        }
    }

    /// <summary>The canonical lower-case name of a dtype ("f32", …).</summary>
    public static string DtypeName(VectorDtype dtype) => dtype switch
    {
        VectorDtype.F32 => "f32",
        VectorDtype.F64 => "f64",
        VectorDtype.F16 => "f16",
        VectorDtype.I8 => "i8",
        VectorDtype.U8 => "u8",
        _ => dtype.ToString().ToLowerInvariant(),
    };

    /// <summary>
    /// The shared evolution rule. Both sides null → no vector involved, compatible. One side
    /// null → vector-ness was added to or removed from an existing field, incompatible. Both
    /// set → dim and dtype must be equal. Returns null when compatible, otherwise the message
    /// (the caller prefixes field/path context).
    /// </summary>
    public static string? CheckCompatible(VectorType? reader, VectorType? writer)
    {
        if (reader is null && writer is null)
        {
            return null;
        }

        if (reader is null || writer is null)
        {
            var (with, without) = reader is null ? ("writer", "reader") : ("reader", "writer");
            return $"vector type declared by {with} but not by {without}; " +
                   "adding or removing the vector annotation on an existing field is a breaking change";
        }

        var r = reader.Value;
        var w = writer.Value;
        if (r.Dim != w.Dim)
        {
            return $"vector dim changed: reader expects {r.Dim}, writer has {w.Dim}";
        }

        if (r.Dtype != w.Dtype)
        {
            return $"vector dtype changed: reader expects {DtypeName(r.Dtype)}, writer has {DtypeName(w.Dtype)}";
        }

        return null;
    }

    public override string ToString() => $"vector(dim={Dim}, dtype={DtypeName(Dtype)})";
}
