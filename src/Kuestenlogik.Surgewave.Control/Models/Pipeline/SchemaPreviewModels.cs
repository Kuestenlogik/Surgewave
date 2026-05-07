namespace Kuestenlogik.Surgewave.Control.Models.Pipeline;

/// <summary>
/// Field information extracted from a schema for structured preview.
/// </summary>
public sealed record SchemaFieldInfo(string Name, string Type, string? Description, bool IsRequired);

/// <summary>
/// A structured record representation with schema-derived fields.
/// </summary>
public sealed record StructuredRecord(List<SchemaFieldInfo> Fields, List<Dictionary<string, string?>> Rows);
