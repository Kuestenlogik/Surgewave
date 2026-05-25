namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Abstraction over the schema store — enables local (embedded) and remote
/// (standalone) implementations. Inference, evolution, and migration services
/// depend on this interface rather than the concrete <see cref="SchemaStore"/>.
/// </summary>
public interface ISchemaStore
{
    IReadOnlyList<string> GetSubjects(bool includeDeleted = false);
    IReadOnlyList<int> GetVersions(string subject, bool includeDeleted = false);
    Schema? GetSchema(string subject, int version);
    Schema? GetLatestSchema(string subject);
    Schema? GetSchemaById(int id);
    Schema RegisterSchema(string subject, string schemaString, SchemaType schemaType, IReadOnlyList<SchemaReference>? references = null);
    IReadOnlyList<int> DeleteSubject(string subject, bool permanent = false);
    int? DeleteVersion(string subject, int version, bool permanent = false);
    CompatibilityMode GetCompatibility(string subject);
    void SetCompatibility(string subject, CompatibilityMode compatibility);
    IReadOnlyList<Schema> GetSchemasForCompatibilityCheck(string subject, CompatibilityMode mode);
}
