namespace Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

/// <summary>
/// Schema information.
/// </summary>
public record SchemaInfo
{
    public int Id { get; init; }
    public string Subject { get; init; } = string.Empty;
    public int Version { get; init; }
    public string SchemaType { get; init; } = "AVRO";
    public string SchemaString { get; init; } = string.Empty;
}
