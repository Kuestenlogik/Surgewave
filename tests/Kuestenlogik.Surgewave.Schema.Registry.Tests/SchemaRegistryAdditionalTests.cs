using Kuestenlogik.Surgewave.Schema.Registry;
using Kuestenlogik.Surgewave.Schema.Registry.Evolution;
using Kuestenlogik.Surgewave.Schema.Registry.Inference;
using Kuestenlogik.Surgewave.Schema.Registry.Linking;
using Kuestenlogik.Surgewave.Schema.Registry.Migration;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Schema.Registry.Tests;

// ─── Fake handler for testing without real Avro/Json/Proto deps ───────────────

internal sealed class FakeSchemaHandler : ISchemaTypeHandler
{
    private readonly bool _validateResult;
    private readonly string? _validateError;
    private readonly bool _compatibleResult;
    private readonly string[]? _compatibilityErrors;

    public string TypeName { get; }

    public FakeSchemaHandler(
        string typeName = "FAKE",
        bool validateResult = true,
        string? validateError = null,
        bool compatibleResult = true,
        string[]? compatibilityErrors = null)
    {
        TypeName = typeName;
        _validateResult = validateResult;
        _validateError = validateError;
        _compatibleResult = compatibleResult;
        _compatibilityErrors = compatibilityErrors;
    }

    public (bool IsValid, string? Error) Validate(string schemaString) =>
        (_validateResult, _validateError);

    public CompatibilityResult CheckCompatibility(
        string newSchemaString,
        IReadOnlyList<Schema> existingSchemas,
        CompatibilityMode mode) =>
        new(_compatibleResult, _compatibilityErrors);
}

internal sealed class ThrowingSchemaHandler : ISchemaTypeHandler
{
    public string TypeName => "THROW";

    public (bool IsValid, string? Error) Validate(string schemaString) =>
        throw new InvalidOperationException("Simulated validate failure");

    public CompatibilityResult CheckCompatibility(
        string newSchemaString,
        IReadOnlyList<Schema> existingSchemas,
        CompatibilityMode mode) =>
        throw new InvalidOperationException("Simulated compatibility failure");
}

// ─── SchemaStore additional tests ─────────────────────────────────────────────

public sealed class SchemaStoreAdditionalTests : IDisposable
{
    private readonly ILoggerFactory _loggerFactory;
    private readonly SchemaStore _store;

    private const string Schema1 = """{"type":"object","properties":{"id":{"type":"integer"}}}""";
    private const string Schema2 = """{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}}}""";

    public SchemaStoreAdditionalTests()
    {
        _loggerFactory = LoggerFactory.Create(_ => { });
        _store = new SchemaStore(_loggerFactory.CreateLogger<SchemaStore>());
    }

    public void Dispose()
    {
        _store.Dispose();
        _loggerFactory.Dispose();
    }

    [Fact]
    public void GetSubjects_IncludeDeleted_ShowsAllSubjects()
    {
        _store.RegisterSchema("subject-a", Schema1, SchemaType.Json);
        _store.RegisterSchema("subject-b", Schema1, SchemaType.Json);
        _store.DeleteSubject("subject-a");

        var withoutDeleted = _store.GetSubjects(includeDeleted: false);
        var withDeleted = _store.GetSubjects(includeDeleted: true);

        Assert.DoesNotContain("subject-a", withoutDeleted);
        Assert.Contains("subject-b", withoutDeleted);
        Assert.Contains("subject-a", withDeleted);
        Assert.Contains("subject-b", withDeleted);
    }

    [Fact]
    public void GetVersions_DeletedSubject_ReturnsEmpty()
    {
        _store.RegisterSchema("deleted-subj", Schema1, SchemaType.Json);
        _store.DeleteSubject("deleted-subj");

        var versions = _store.GetVersions("deleted-subj", includeDeleted: false);

        Assert.Empty(versions);
    }

    [Fact]
    public void GetVersions_DeletedSubject_WithIncludeDeleted_ReturnsVersions()
    {
        _store.RegisterSchema("del-subj-v", Schema1, SchemaType.Json);
        _store.RegisterSchema("del-subj-v", Schema2, SchemaType.Json);
        _store.DeleteSubject("del-subj-v");

        var versions = _store.GetVersions("del-subj-v", includeDeleted: true);

        Assert.Equal(2, versions.Count);
    }

    [Fact]
    public void GetVersions_NonExistentSubject_ReturnsEmpty()
    {
        var versions = _store.GetVersions("non-existent-subject");

        Assert.Empty(versions);
    }

    [Fact]
    public void GetSchema_ByVersion_NonExistentSubject_ReturnsNull()
    {
        var schema = _store.GetSchema("no-such-subject", 1);

        Assert.Null(schema);
    }

    [Fact]
    public void GetSchema_ByVersion_NonExistentVersion_ReturnsNull()
    {
        _store.RegisterSchema("mysubject", Schema1, SchemaType.Json);

        var schema = _store.GetSchema("mysubject", 999);

        Assert.Null(schema);
    }

    [Fact]
    public void GetLatestSchema_NonExistentSubject_ReturnsNull()
    {
        var schema = _store.GetLatestSchema("no-such");

        Assert.Null(schema);
    }

    [Fact]
    public void GetLatestSchema_DeletedSubject_ReturnsNull()
    {
        _store.RegisterSchema("temp-sub", Schema1, SchemaType.Json);
        _store.DeleteSubject("temp-sub");

        var schema = _store.GetLatestSchema("temp-sub");

        Assert.Null(schema);
    }

    [Fact]
    public void GetSchemaById_NonExistentId_ReturnsNull()
    {
        var schema = _store.GetSchemaById(99999);

        Assert.Null(schema);
    }

    [Fact]
    public void DeleteSubject_NonExistentSubject_ReturnsEmpty()
    {
        var deleted = _store.DeleteSubject("no-such-subject");

        Assert.Empty(deleted);
    }

    [Fact]
    public void DeleteSubject_Permanent_RemovesAllVersions()
    {
        _store.RegisterSchema("perm-del", Schema1, SchemaType.Json);
        _store.RegisterSchema("perm-del", Schema2, SchemaType.Json);

        var deleted = _store.DeleteSubject("perm-del", permanent: true);

        Assert.Equal(2, deleted.Count);
        Assert.Empty(_store.GetSubjects(includeDeleted: true));
    }

    [Fact]
    public void DeleteVersion_NonExistentSubject_ReturnsNull()
    {
        var result = _store.DeleteVersion("no-such", 1);

        Assert.Null(result);
    }

    [Fact]
    public void DeleteVersion_NonExistentVersion_ReturnsNull()
    {
        _store.RegisterSchema("exists", Schema1, SchemaType.Json);

        var result = _store.DeleteVersion("exists", 999);

        Assert.Null(result);
    }

    [Fact]
    public void DeleteVersion_Permanent_RemovesFromVersionList()
    {
        _store.RegisterSchema("ver-del-sub", Schema1, SchemaType.Json);
        _store.RegisterSchema("ver-del-sub", Schema2, SchemaType.Json);

        _store.DeleteVersion("ver-del-sub", 1, permanent: true);

        var versions = _store.GetVersions("ver-del-sub");
        Assert.Single(versions);
        Assert.DoesNotContain(1, versions);
        Assert.Contains(2, versions);
    }

    [Fact]
    public void DeleteVersion_LastVersion_Permanent_RemovesSubject()
    {
        _store.RegisterSchema("last-ver-sub", Schema1, SchemaType.Json);

        _store.DeleteVersion("last-ver-sub", 1, permanent: true);

        Assert.Empty(_store.GetVersions("last-ver-sub"));
    }

    [Fact]
    public void LookupSchemaId_WrongSubject_ReturnsNull()
    {
        _store.RegisterSchema("actual-subject", Schema1, SchemaType.Json);

        var id = _store.LookupSchemaId("wrong-subject", Schema1, SchemaType.Json);

        Assert.Null(id);
    }

    [Fact]
    public void LookupSchemaId_NonExistentSchema_ReturnsNull()
    {
        _store.RegisterSchema("mysubject", Schema1, SchemaType.Json);

        var id = _store.LookupSchemaId("mysubject", Schema2, SchemaType.Json);

        Assert.Null(id);
    }

    [Fact]
    public void SetCompatibility_CreatesSubjectConfigIfMissing()
    {
        _store.SetCompatibility("brand-new-subject", CompatibilityMode.FullTransitive);

        var mode = _store.GetCompatibility("brand-new-subject");

        Assert.Equal(CompatibilityMode.FullTransitive, mode);
    }

    [Fact]
    public void GetCompatibility_UnknownSubject_ReturnsGlobal()
    {
        _store.GlobalCompatibility = CompatibilityMode.ForwardTransitive;

        var mode = _store.GetCompatibility("unknown-subject");

        Assert.Equal(CompatibilityMode.ForwardTransitive, mode);
    }

    [Fact]
    public void GetSchemasForCompatibilityCheck_Backward_ReturnsLatestOnly()
    {
        _store.RegisterSchema("compat-sub", Schema1, SchemaType.Json);
        _store.RegisterSchema("compat-sub", Schema2, SchemaType.Json);

        var schemas = _store.GetSchemasForCompatibilityCheck("compat-sub", CompatibilityMode.Backward);

        Assert.Single(schemas);
        Assert.Equal(2, schemas[0].Version);
    }

    [Fact]
    public void GetSchemasForCompatibilityCheck_Forward_ReturnsLatestOnly()
    {
        _store.RegisterSchema("fwd-sub", Schema1, SchemaType.Json);
        _store.RegisterSchema("fwd-sub", Schema2, SchemaType.Json);

        var schemas = _store.GetSchemasForCompatibilityCheck("fwd-sub", CompatibilityMode.Forward);

        Assert.Single(schemas);
        Assert.Equal(2, schemas[0].Version);
    }

    [Fact]
    public void GetSchemasForCompatibilityCheck_Full_ReturnsLatestOnly()
    {
        _store.RegisterSchema("full-sub", Schema1, SchemaType.Json);
        _store.RegisterSchema("full-sub", Schema2, SchemaType.Json);

        var schemas = _store.GetSchemasForCompatibilityCheck("full-sub", CompatibilityMode.Full);

        Assert.Single(schemas);
    }

    [Fact]
    public void GetSchemasForCompatibilityCheck_BackwardTransitive_ReturnsAll()
    {
        _store.RegisterSchema("bwt-sub", Schema1, SchemaType.Json);
        _store.RegisterSchema("bwt-sub", Schema2, SchemaType.Json);

        var schemas = _store.GetSchemasForCompatibilityCheck("bwt-sub", CompatibilityMode.BackwardTransitive);

        Assert.Equal(2, schemas.Count);
    }

    [Fact]
    public void GetSchemasForCompatibilityCheck_ForwardTransitive_ReturnsAll()
    {
        _store.RegisterSchema("fwt-sub", Schema1, SchemaType.Json);
        _store.RegisterSchema("fwt-sub", Schema2, SchemaType.Json);

        var schemas = _store.GetSchemasForCompatibilityCheck("fwt-sub", CompatibilityMode.ForwardTransitive);

        Assert.Equal(2, schemas.Count);
    }

    [Fact]
    public void GetSchemasForCompatibilityCheck_FullTransitive_ReturnsAll()
    {
        _store.RegisterSchema("full-t-sub", Schema1, SchemaType.Json);
        _store.RegisterSchema("full-t-sub", Schema2, SchemaType.Json);

        var schemas = _store.GetSchemasForCompatibilityCheck("full-t-sub", CompatibilityMode.FullTransitive);

        Assert.Equal(2, schemas.Count);
    }

    [Fact]
    public void GetSchemasForCompatibilityCheck_None_ReturnsEmpty()
    {
        _store.RegisterSchema("none-sub", Schema1, SchemaType.Json);

        var schemas = _store.GetSchemasForCompatibilityCheck("none-sub", CompatibilityMode.None);

        Assert.Empty(schemas);
    }

    [Fact]
    public void GetSchemasForCompatibilityCheck_NonExistentSubject_ReturnsEmpty()
    {
        var schemas = _store.GetSchemasForCompatibilityCheck("no-sub", CompatibilityMode.Backward);

        Assert.Empty(schemas);
    }

    [Fact]
    public void MultipleSubjects_IdsAreUnique()
    {
        var s1 = _store.RegisterSchema("subj-x", Schema1, SchemaType.Json);
        var s2 = _store.RegisterSchema("subj-y", Schema2, SchemaType.Json);

        Assert.NotEqual(s1.Id, s2.Id);
    }

    [Fact]
    public void RegisterSchema_WithReferences_StoresReferences()
    {
        var refs = new List<SchemaReference>
        {
            new("CommonSchema", "common-value", 1)
        };

        var schema = _store.RegisterSchema("ref-sub", Schema1, SchemaType.Protobuf, refs);

        Assert.NotNull(schema.References);
        Assert.Single(schema.References);
        Assert.Equal("CommonSchema", schema.References[0].Name);
    }

    [Fact]
    public void SchemaStore_Persistence_SaveAndReload()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "surgewave-test-" + Guid.NewGuid().ToString("N")[..8]);
        try
        {
            using var store1 = new SchemaStore(_loggerFactory.CreateLogger<SchemaStore>(), tempDir);
            store1.RegisterSchema("persist-sub", Schema1, SchemaType.Json);
            store1.RegisterSchema("persist-sub", Schema2, SchemaType.Json);
            store1.GlobalCompatibility = CompatibilityMode.Full;

            using var store2 = new SchemaStore(_loggerFactory.CreateLogger<SchemaStore>(), tempDir);
            var versions = store2.GetVersions("persist-sub");

            Assert.Equal(2, versions.Count);
            Assert.Equal(CompatibilityMode.Full, store2.GlobalCompatibility);
        }
        finally
        {
            try { Directory.Delete(tempDir, recursive: true); } catch { }
        }
    }

    [Fact]
    public void SchemaStore_Persistence_LoadFromNonExistentDir_NoException()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "surgewave-nonexistent-" + Guid.NewGuid().ToString("N")[..8]);
        // Don't create the directory - should handle gracefully
        using var store = new SchemaStore(_loggerFactory.CreateLogger<SchemaStore>(), tempDir);

        Assert.Empty(store.GetSubjects());
    }
}

// ─── CompatibilityChecker additional tests ────────────────────────────────────

public sealed class CompatibilityCheckerAdditionalTests
{
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<CompatibilityChecker> _logger;

    public CompatibilityCheckerAdditionalTests()
    {
        _loggerFactory = LoggerFactory.Create(_ => { });
        _logger = _loggerFactory.CreateLogger<CompatibilityChecker>();
    }

    private CompatibilityChecker CreateChecker(params ISchemaTypeHandler[] handlers)
    {
        var registry = new SchemaTypeHandlerRegistry(handlers);
        return new CompatibilityChecker(_logger, registry);
    }

    [Fact]
    public void CheckCompatibility_NoneMode_NoExistingSchemas_IsCompatible()
    {
        var checker = CreateChecker(new FakeSchemaHandler());

        var result = checker.CheckCompatibility("schema", "FAKE", [], CompatibilityMode.None);

        Assert.True(result.IsCompatible);
    }

    [Fact]
    public void CheckCompatibility_NoneMode_WithExistingSchemas_IsCompatible()
    {
        var checker = CreateChecker(new FakeSchemaHandler(compatibleResult: false));
        var existing = new Schema[]
        {
            new() { Id = 1, Subject = "s", Version = 1, SchemaType = SchemaType.Json, SchemaString = "x" }
        };

        var result = checker.CheckCompatibility("schema", "FAKE", existing, CompatibilityMode.None);

        Assert.True(result.IsCompatible);
    }

    [Fact]
    public void CheckCompatibility_NoExistingSchemas_IsCompatible()
    {
        var checker = CreateChecker(new FakeSchemaHandler(compatibleResult: false));

        var result = checker.CheckCompatibility("schema", "FAKE", [], CompatibilityMode.Backward);

        Assert.True(result.IsCompatible);
    }

    [Fact]
    public void CheckCompatibility_UnknownHandler_IsCompatible()
    {
        var checker = CreateChecker();
        var existing = new Schema[]
        {
            new() { Id = 1, Subject = "s", Version = 1, SchemaType = SchemaType.Json, SchemaString = "x" }
        };

        var result = checker.CheckCompatibility("schema", "UNKNOWNTYPE", existing, CompatibilityMode.Backward);

        Assert.True(result.IsCompatible);
    }

    [Fact]
    public void CheckCompatibility_HandlerThrows_ReturnsIncompatible()
    {
        var checker = CreateChecker(new ThrowingSchemaHandler());
        var existing = new Schema[]
        {
            new() { Id = 1, Subject = "s", Version = 1, SchemaType = SchemaType.Json, SchemaString = "x" }
        };

        var result = checker.CheckCompatibility("schema", "THROW", existing, CompatibilityMode.Backward);

        Assert.False(result.IsCompatible);
        Assert.NotEmpty(result.Messages!);
    }

    [Fact]
    public void CheckCompatibility_ViaSchemaTypeEnum_IsCompatible()
    {
        var checker = CreateChecker(new FakeSchemaHandler("JSON"));
        var existing = new Schema[]
        {
            new() { Id = 1, Subject = "s", Version = 1, SchemaType = SchemaType.Json, SchemaString = "x" }
        };

        var result = checker.CheckCompatibility("schema", SchemaType.Json, existing, CompatibilityMode.Backward);

        Assert.True(result.IsCompatible);
    }

    [Fact]
    public void CheckCompatibility_HandlerReturnsIncompatible_ReturnsFalse()
    {
        var errors = new[] { "Field 'id' type changed", "Field 'name' removed" };
        var checker = CreateChecker(new FakeSchemaHandler(compatibleResult: false, compatibilityErrors: errors));
        var existing = new Schema[]
        {
            new() { Id = 1, Subject = "s", Version = 1, SchemaType = SchemaType.Json, SchemaString = "x" }
        };

        var result = checker.CheckCompatibility("new-schema", "FAKE", existing, CompatibilityMode.Backward);

        Assert.False(result.IsCompatible);
        Assert.NotNull(result.Messages);
    }

    [Fact]
    public void ValidateSchema_ValidHandler_ReturnsTrue()
    {
        var checker = CreateChecker(new FakeSchemaHandler("FAKE"));

        var (isValid, error) = checker.ValidateSchema("schema", "FAKE");

        Assert.True(isValid);
        Assert.Null(error);
    }

    [Fact]
    public void ValidateSchema_InvalidSchema_ReturnsFalse()
    {
        var checker = CreateChecker(new FakeSchemaHandler(validateResult: false, validateError: "Parse error"));

        var (isValid, error) = checker.ValidateSchema("bad schema", "FAKE");

        Assert.False(isValid);
        Assert.Equal("Parse error", error);
    }

    [Fact]
    public void ValidateSchema_ThrowingHandler_ReturnsFalseWithError()
    {
        var checker = CreateChecker(new ThrowingSchemaHandler());

        var (isValid, error) = checker.ValidateSchema("schema", "THROW");

        Assert.False(isValid);
        Assert.NotNull(error);
        Assert.Contains("Simulated", error);
    }

    [Fact]
    public void ValidateSchema_UnknownType_ReturnsFalseWithError()
    {
        var checker = CreateChecker();

        var (isValid, error) = checker.ValidateSchema("schema", "UNKNOWN");

        Assert.False(isValid);
        Assert.Contains("Unsupported", error);
    }

    [Fact]
    public void ValidateSchema_ViaSchemaTypeEnum_UsesUppercaseName()
    {
        var checker = CreateChecker(new FakeSchemaHandler("AVRO"));

        var (isValid, _) = checker.ValidateSchema("schema", SchemaType.Avro);

        Assert.True(isValid);
    }

    [Fact]
    public void GetSupportedTypes_ReturnAllRegisteredHandlers()
    {
        var checker = CreateChecker(
            new FakeSchemaHandler("TYPE1"),
            new FakeSchemaHandler("TYPE2"),
            new FakeSchemaHandler("TYPE3"));

        var types = checker.GetSupportedTypes().ToList();

        Assert.Equal(3, types.Count);
    }

    [Fact]
    public void IsTypeSupported_KnownType_ReturnsTrue()
    {
        var checker = CreateChecker(new FakeSchemaHandler("MYTYPE"));

        Assert.True(checker.IsTypeSupported("MYTYPE"));
        Assert.True(checker.IsTypeSupported("mytype")); // case-insensitive
    }

    [Fact]
    public void IsTypeSupported_UnknownType_ReturnsFalse()
    {
        var checker = CreateChecker(new FakeSchemaHandler("KNOWN"));

        Assert.False(checker.IsTypeSupported("UNKNOWN"));
    }
}

// ─── SchemaTypeHandlerRegistry tests ─────────────────────────────────────────

public sealed class SchemaTypeHandlerRegistryTests
{
    [Fact]
    public void GetHandler_CaseInsensitive_ReturnsHandler()
    {
        var handler = new FakeSchemaHandler("AVRO");
        var registry = new SchemaTypeHandlerRegistry([handler]);

        Assert.NotNull(registry.GetHandler("avro"));
        Assert.NotNull(registry.GetHandler("AVRO"));
        Assert.NotNull(registry.GetHandler("Avro"));
    }

    [Fact]
    public void GetHandler_UnknownType_ReturnsNull()
    {
        var registry = new SchemaTypeHandlerRegistry([new FakeSchemaHandler("KNOWN")]);

        Assert.Null(registry.GetHandler("UNKNOWN"));
    }

    [Fact]
    public void IsSupported_KnownType_ReturnsTrue()
    {
        var registry = new SchemaTypeHandlerRegistry([new FakeSchemaHandler("JSON")]);

        Assert.True(registry.IsSupported("JSON"));
        Assert.True(registry.IsSupported("json"));
    }

    [Fact]
    public void IsSupported_UnknownType_ReturnsFalse()
    {
        var registry = new SchemaTypeHandlerRegistry([new FakeSchemaHandler("AVRO")]);

        Assert.False(registry.IsSupported("PROTOBUF"));
    }

    [Fact]
    public void GetSupportedTypes_ReturnsAllRegisteredTypes()
    {
        var registry = new SchemaTypeHandlerRegistry(
        [
            new FakeSchemaHandler("AVRO"),
            new FakeSchemaHandler("JSON"),
            new FakeSchemaHandler("PROTOBUF"),
        ]);

        var types = registry.GetSupportedTypes().ToList();

        Assert.Equal(3, types.Count);
    }

    [Fact]
    public void EmptyRegistry_IsSupported_ReturnsFalse()
    {
        var registry = new SchemaTypeHandlerRegistry([]);

        Assert.False(registry.IsSupported("ANYTHING"));
        Assert.Null(registry.GetHandler("ANYTHING"));
        Assert.Empty(registry.GetSupportedTypes());
    }
}

// ─── Schema/enum/record types tests ──────────────────────────────────────────

public sealed class SchemaModelTests
{
    [Fact]
    public void Schema_Record_Equality()
    {
        var s1 = new Schema
        {
            Id = 1, Subject = "sub", Version = 1,
            SchemaType = SchemaType.Avro, SchemaString = "test"
        };
        var s2 = new Schema
        {
            Id = 1, Subject = "sub", Version = 1,
            SchemaType = SchemaType.Avro, SchemaString = "test"
        };

        Assert.Equal(s1.Id, s2.Id);
        Assert.Equal(s1.Subject, s2.Subject);
    }

    [Fact]
    public void CompatibilityResult_Compatible_HasNoMessages()
    {
        var result = new CompatibilityResult(true);

        Assert.True(result.IsCompatible);
        Assert.Null(result.Messages);
    }

    [Fact]
    public void CompatibilityResult_Incompatible_HasMessages()
    {
        var msgs = new[] { "Error 1", "Error 2" };
        var result = new CompatibilityResult(false, msgs);

        Assert.False(result.IsCompatible);
        Assert.Equal(2, result.Messages!.Count);
    }

    [Fact]
    public void SchemaReference_Properties()
    {
        var ref1 = new SchemaReference("MyRef", "my-subject", 3);

        Assert.Equal("MyRef", ref1.Name);
        Assert.Equal("my-subject", ref1.Subject);
        Assert.Equal(3, ref1.Version);
    }

    [Fact]
    public void SchemaMetadata_Properties()
    {
        var meta = new SchemaMetadata("orders", 42, 5, "AVRO");

        Assert.Equal("orders", meta.Subject);
        Assert.Equal(42, meta.Id);
        Assert.Equal(5, meta.Version);
        Assert.Equal("AVRO", meta.SchemaType);
    }

    [Fact]
    public void SubjectConfig_DefaultCompatibilityIsBackward()
    {
        var config = new SubjectConfig { Subject = "test" };

        Assert.Equal(CompatibilityMode.Backward, config.Compatibility);
        Assert.False(config.IsDeleted);
    }

    [Fact]
    public void SubjectConfig_CanBeMarkedDeleted()
    {
        var config = new SubjectConfig { Subject = "test", IsDeleted = true };

        Assert.True(config.IsDeleted);
    }

    [Fact]
    public void SchemaType_AllValues_Defined()
    {
        Assert.Equal(4, Enum.GetValues<SchemaType>().Length);
        Assert.Contains(SchemaType.Avro, Enum.GetValues<SchemaType>());
        Assert.Contains(SchemaType.Json, Enum.GetValues<SchemaType>());
        Assert.Contains(SchemaType.Protobuf, Enum.GetValues<SchemaType>());
        Assert.Contains(SchemaType.FlatBuffers, Enum.GetValues<SchemaType>());
    }

    [Fact]
    public void CompatibilityMode_AllValues_Defined()
    {
        var values = Enum.GetValues<CompatibilityMode>();

        Assert.Equal(7, values.Length);
        Assert.Contains(CompatibilityMode.None, values);
        Assert.Contains(CompatibilityMode.Backward, values);
        Assert.Contains(CompatibilityMode.BackwardTransitive, values);
        Assert.Contains(CompatibilityMode.Forward, values);
        Assert.Contains(CompatibilityMode.ForwardTransitive, values);
        Assert.Contains(CompatibilityMode.Full, values);
        Assert.Contains(CompatibilityMode.FullTransitive, values);
    }
}

// ─── SchemaLinkingState tests ─────────────────────────────────────────────────

public sealed class SchemaLinkingStateTests : IDisposable
{
    private readonly string _tempDir;

    public SchemaLinkingStateTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-linking-state-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    [Fact]
    public void GetLink_NotFound_ReturnsNull()
    {
        var state = new SchemaLinkingState();

        var link = state.GetLink("cluster-a", "user-value");

        Assert.Null(link);
    }

    [Fact]
    public void SetLink_ThenGetLink_ReturnsSameLink()
    {
        var state = new SchemaLinkingState();
        var link = new SchemaLink
        {
            Subject = "orders-value",
            SourceCluster = "cluster-a",
            TargetCluster = "cluster-b",
            SourceVersion = 3,
            TargetVersion = 3,
            Status = SchemaSyncStatus.Synced,
            LastSyncedAt = DateTimeOffset.UtcNow,
        };

        state.SetLink("cluster-a", "orders-value", link);
        var found = state.GetLink("cluster-a", "orders-value");

        Assert.NotNull(found);
        Assert.Equal("orders-value", found.Subject);
        Assert.Equal(SchemaSyncStatus.Synced, found.Status);
    }

    [Fact]
    public void GetAllLinks_EmptyState_ReturnsEmpty()
    {
        var state = new SchemaLinkingState();

        Assert.Empty(state.GetAllLinks());
    }

    [Fact]
    public void GetAllLinks_MultipleLinks_ReturnsAll()
    {
        var state = new SchemaLinkingState();
        state.SetLink("c1", "s1", new SchemaLink { Subject = "s1", SourceCluster = "c1", TargetCluster = "c2", LastSyncedAt = DateTimeOffset.UtcNow });
        state.SetLink("c1", "s2", new SchemaLink { Subject = "s2", SourceCluster = "c1", TargetCluster = "c2", LastSyncedAt = DateTimeOffset.UtcNow });
        state.SetLink("c2", "s3", new SchemaLink { Subject = "s3", SourceCluster = "c2", TargetCluster = "c1", LastSyncedAt = DateTimeOffset.UtcNow });

        var all = state.GetAllLinks();

        Assert.Equal(3, all.Count);
    }

    [Fact]
    public void GetLinksForSubject_ReturnsLinksAcrossClusters()
    {
        var state = new SchemaLinkingState();
        state.SetLink("c1", "orders-value", new SchemaLink { Subject = "orders-value", SourceCluster = "c1", TargetCluster = "c2", LastSyncedAt = DateTimeOffset.UtcNow });
        state.SetLink("c2", "orders-value", new SchemaLink { Subject = "orders-value", SourceCluster = "c2", TargetCluster = "c3", LastSyncedAt = DateTimeOffset.UtcNow });
        state.SetLink("c1", "other-subject", new SchemaLink { Subject = "other-subject", SourceCluster = "c1", TargetCluster = "c2", LastSyncedAt = DateTimeOffset.UtcNow });

        var links = state.GetLinksForSubject("orders-value");

        Assert.Equal(2, links.Count);
        Assert.All(links, l => Assert.Equal("orders-value", l.Subject));
    }

    [Fact]
    public void GetConflicts_ReturnsOnlyConflicts()
    {
        var state = new SchemaLinkingState();
        state.SetLink("c1", "s1", new SchemaLink { Subject = "s1", SourceCluster = "c1", TargetCluster = "c2", Status = SchemaSyncStatus.Synced, LastSyncedAt = DateTimeOffset.UtcNow });
        state.SetLink("c1", "s2", new SchemaLink { Subject = "s2", SourceCluster = "c1", TargetCluster = "c2", Status = SchemaSyncStatus.Conflict, LastSyncedAt = DateTimeOffset.UtcNow });
        state.SetLink("c1", "s3", new SchemaLink { Subject = "s3", SourceCluster = "c1", TargetCluster = "c2", Status = SchemaSyncStatus.Failed, LastSyncedAt = DateTimeOffset.UtcNow });

        var conflicts = state.GetConflicts();

        Assert.Single(conflicts);
        Assert.Equal("s2", conflicts[0].Subject);
    }

    [Fact]
    public void SaveAndLoad_RoundTrip()
    {
        var state = new SchemaLinkingState();
        state.SetLink("cluster-a", "user-value", new SchemaLink
        {
            Subject = "user-value",
            SourceCluster = "cluster-a",
            TargetCluster = "cluster-b",
            SourceVersion = 5,
            TargetVersion = 4,
            Status = SchemaSyncStatus.Conflict,
            LastSyncedAt = DateTimeOffset.Parse("2026-01-01T00:00:00Z"),
        });

        var path = Path.Combine(_tempDir, "state.json");
        state.SaveToFile(path);

        var loaded = SchemaLinkingState.LoadFromFile(path);

        var link = loaded.GetLink("cluster-a", "user-value");
        Assert.NotNull(link);
        Assert.Equal("user-value", link.Subject);
        Assert.Equal(5, link.SourceVersion);
        Assert.Equal(4, link.TargetVersion);
        Assert.Equal(SchemaSyncStatus.Conflict, link.Status);
    }

    [Fact]
    public void LoadFromFile_NonExistent_ReturnsEmptyState()
    {
        var state = SchemaLinkingState.LoadFromFile(Path.Combine(_tempDir, "doesnotexist.json"));

        Assert.Empty(state.GetAllLinks());
    }
}

// ─── SchemaLinkingMetrics tests ────────────────────────────────────────────────

public sealed class SchemaLinkingMetricsTests
{
    [Fact]
    public void RecordSync_IncrementsSynced()
    {
        var metrics = new SchemaLinkingMetrics();

        metrics.RecordSync("cluster-a");
        metrics.RecordSync("cluster-a");
        metrics.RecordSync("cluster-b");

        Assert.Equal(3, metrics.SchemasSynced);
        Assert.Equal(2, metrics.PerClusterSyncCount["cluster-a"]);
        Assert.Equal(1, metrics.PerClusterSyncCount["cluster-b"]);
    }

    [Fact]
    public void RecordConflict_IncrementsDetected()
    {
        var metrics = new SchemaLinkingMetrics();

        metrics.RecordConflict();
        metrics.RecordConflict();

        Assert.Equal(2, metrics.ConflictsDetected);
    }

    [Fact]
    public void RecordConflictResolved_IncrementsResolved()
    {
        var metrics = new SchemaLinkingMetrics();

        metrics.RecordConflict();
        metrics.RecordConflictResolved();

        Assert.Equal(1, metrics.ConflictsDetected);
        Assert.Equal(1, metrics.ConflictsResolved);
    }

    [Fact]
    public void RecordError_IncrementsErrors()
    {
        var metrics = new SchemaLinkingMetrics();

        metrics.RecordError();
        metrics.RecordError();
        metrics.RecordError();

        Assert.Equal(3, metrics.SyncErrors);
    }

    [Fact]
    public void RecordSyncCycleComplete_SetsLastSyncAt()
    {
        var metrics = new SchemaLinkingMetrics();
        Assert.Null(metrics.LastSyncAt);

        metrics.RecordSyncCycleComplete();

        Assert.NotNull(metrics.LastSyncAt);
        Assert.True(metrics.LastSyncAt > DateTimeOffset.UtcNow.AddMinutes(-1));
    }

    [Fact]
    public void DefaultMetrics_AllZero()
    {
        var metrics = new SchemaLinkingMetrics();

        Assert.Equal(0, metrics.SchemasSynced);
        Assert.Equal(0, metrics.ConflictsDetected);
        Assert.Equal(0, metrics.ConflictsResolved);
        Assert.Equal(0, metrics.SyncErrors);
        Assert.Null(metrics.LastSyncAt);
        Assert.Empty(metrics.PerClusterSyncCount);
    }
}

// ─── SubjectPatternMatcher tests ──────────────────────────────────────────────

public sealed class SubjectPatternMatcherTests
{
    [Theory]
    [InlineData("orders-value", "*", true)]
    [InlineData("orders-value", "orders-*", true)]
    [InlineData("orders-value", "*-value", true)]
    [InlineData("orders-value", "orders-value", true)]
    [InlineData("orders-value", "events-*", false)]
    [InlineData("orders-value", "orders-key", false)]
    public void Matches_GlobPatterns(string subject, string pattern, bool expected)
    {
        Assert.Equal(expected, SubjectPatternMatcher.Matches(subject, pattern));
    }

    [Fact]
    public void MatchesAny_MatchesFirstPattern()
    {
        var patterns = new[] { "events-*", "orders-*", "*-key" };

        Assert.True(SubjectPatternMatcher.MatchesAny("orders-value", patterns));
    }

    [Fact]
    public void MatchesAny_NoMatch_ReturnsFalse()
    {
        var patterns = new[] { "events-*", "signals-*" };

        Assert.False(SubjectPatternMatcher.MatchesAny("orders-value", patterns));
    }

    [Fact]
    public void Matches_NullSubject_ThrowsArgumentNull()
    {
        Assert.ThrowsAny<ArgumentException>(() => SubjectPatternMatcher.Matches(null!, "*"));
    }

    [Fact]
    public void Matches_NullPattern_ThrowsArgumentNull()
    {
        Assert.ThrowsAny<ArgumentException>(() => SubjectPatternMatcher.Matches("subject", null!));
    }

    [Fact]
    public void Matches_QuestionMarkWildcard()
    {
        Assert.True(SubjectPatternMatcher.Matches("ab", "a?"));
        Assert.False(SubjectPatternMatcher.Matches("abc", "a?"));
    }
}

// ─── Config types tests ────────────────────────────────────────────────────────

public sealed class SchemaRegistryConfigTests
{
    [Fact]
    public void SchemaRegistryConfig_Defaults()
    {
        var config = new SchemaRegistryConfig();

        Assert.Equal("./data/schemas", config.DataPath);
        Assert.Equal(CompatibilityMode.Backward, config.DefaultCompatibility);
    }

    [Fact]
    public void SchemaRegistryConfig_CustomValues()
    {
        var config = new SchemaRegistryConfig
        {
            DataPath = "/var/surgewave/schemas",
            DefaultCompatibility = CompatibilityMode.FullTransitive,
        };

        Assert.Equal("/var/surgewave/schemas", config.DataPath);
        Assert.Equal(CompatibilityMode.FullTransitive, config.DefaultCompatibility);
    }
}

public sealed class SchemaInferenceConfigTests
{
    [Fact]
    public void SchemaInferenceConfig_Defaults()
    {
        var config = new SchemaInferenceConfig();

        Assert.True(config.Enabled);
        Assert.Equal(100, config.SampleSize);
        Assert.Equal(60, config.RefreshIntervalSeconds);
        Assert.True(config.AutoRegister);
        Assert.Single(config.ExcludedTopics);
        Assert.Equal("__*", config.ExcludedTopics[0]);
    }

    [Fact]
    public void SchemaInferenceConfig_CustomValues()
    {
        var config = new SchemaInferenceConfig
        {
            Enabled = false,
            SampleSize = 500,
            RefreshIntervalSeconds = 30,
            AutoRegister = false,
            ExcludedTopics = ["__*", "internal-*"],
        };

        Assert.False(config.Enabled);
        Assert.Equal(500, config.SampleSize);
        Assert.Equal(2, config.ExcludedTopics.Count);
    }
}

public sealed class SchemaEvolutionConfigTests
{
    [Fact]
    public void SchemaEvolutionConfig_Defaults()
    {
        var config = new SchemaEvolutionConfig();

        Assert.False(config.Enabled);
        Assert.Equal(60, config.CheckIntervalSeconds);
        Assert.True(config.AutoGenerateCode);
        Assert.True(config.NotifyAssistant);
        Assert.Equal("Surgewave.Models", config.DefaultNamespace);
    }
}

// ─── SchemaMigrationException tests ──────────────────────────────────────────

public sealed class SchemaMigrationExceptionTests
{
    [Fact]
    public void SchemaMigrationException_WithMessage()
    {
        var ex = new SchemaMigrationException("Migration failed: type mismatch");

        Assert.Equal("Migration failed: type mismatch", ex.Message);
        Assert.Null(ex.InnerException);
    }

    [Fact]
    public void SchemaMigrationException_WithMessageAndInner()
    {
        var inner = new InvalidOperationException("inner");
        var ex = new SchemaMigrationException("Outer message", inner);

        Assert.Equal("Outer message", ex.Message);
        Assert.Same(inner, ex.InnerException);
    }

    [Fact]
    public void SchemaMigrationException_IsException()
    {
        var ex = new SchemaMigrationException("test");

        Assert.IsAssignableFrom<Exception>(ex);
    }
}

// ─── SchemaChange / FieldChange model tests ────────────────────────────────────

public sealed class SchemaChangeModelTests
{
    [Fact]
    public void SchemaChange_HasCorrectDefaults()
    {
        var change = new SchemaChange
        {
            SubjectName = "test-subject",
            OldVersion = 1,
            NewVersion = 2,
            ChangeType = SchemaChangeType.FieldAdded,
            FieldChanges = [],
        };

        Assert.Equal(BreakingLevel.None, change.Breaking);
        Assert.NotEqual(default, change.DetectedAt);
    }

    [Fact]
    public void FieldChange_HasCorrectDefaults()
    {
        var fc = new FieldChange
        {
            FieldName = "email",
            Type = FieldChangeType.Added,
        };

        Assert.Null(fc.OldType);
        Assert.Null(fc.NewType);
        Assert.Null(fc.OldFieldName);
        Assert.False(fc.HasDefault);
        Assert.Null(fc.DefaultValue);
        Assert.Equal(BreakingLevel.None, fc.Breaking);
    }

    [Fact]
    public void SchemaChange_WithFieldChanges()
    {
        var changes = new List<FieldChange>
        {
            new() { FieldName = "id", Type = FieldChangeType.TypeChanged, OldType = "integer", NewType = "string", Breaking = BreakingLevel.Major },
            new() { FieldName = "email", Type = FieldChangeType.Added, NewType = "string", HasDefault = true, Breaking = BreakingLevel.None },
        };

        var change = new SchemaChange
        {
            SubjectName = "user-value",
            OldVersion = 1,
            NewVersion = 2,
            ChangeType = SchemaChangeType.Multiple,
            FieldChanges = changes,
            Breaking = BreakingLevel.Major,
        };

        Assert.Equal(2, change.FieldChanges.Count);
        Assert.Equal(BreakingLevel.Major, change.Breaking);
    }

    [Fact]
    public void FieldChangeType_AllValues()
    {
        var values = Enum.GetValues<FieldChangeType>();

        Assert.Contains(FieldChangeType.Added, values);
        Assert.Contains(FieldChangeType.Removed, values);
        Assert.Contains(FieldChangeType.TypeChanged, values);
        Assert.Contains(FieldChangeType.Renamed, values);
        Assert.Contains(FieldChangeType.MadeNullable, values);
        Assert.Contains(FieldChangeType.MadeRequired, values);
    }
}
