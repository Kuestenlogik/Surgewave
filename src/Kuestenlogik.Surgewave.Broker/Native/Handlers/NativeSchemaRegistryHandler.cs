using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Schema.Registry;
using SchemaOps = Kuestenlogik.Surgewave.Broker.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Broker.Native.Handlers;

/// <summary>
/// Handler for native protocol Schema Registry operations.
/// </summary>
public sealed class NativeSchemaRegistryHandler : NativeHandlerBase
{
    public NativeSchemaRegistryHandler(SchemaStore schemaStore, CompatibilityChecker compatibilityChecker)
    {
        Register<SchemaOps.ListSubjectsRequest, SchemaOps.ListSubjectsResult>(
            SurgewaveOpCode.ListSubjects, _ => new SchemaOps.ListSubjectsOperation(schemaStore));
        Register<SchemaOps.GetSubjectVersionsRequest, SchemaOps.GetSubjectVersionsResult>(
            SurgewaveOpCode.GetSubjectVersions, _ => new SchemaOps.GetSubjectVersionsOperation(schemaStore));
        Register<SchemaOps.RegisterSchemaRequest, SchemaOps.RegisterSchemaResult>(
            SurgewaveOpCode.RegisterSchema, _ => new SchemaOps.RegisterSchemaOperation(schemaStore, compatibilityChecker));
        Register<SchemaOps.GetSchemaByIdRequest, SchemaOps.GetSchemaByIdResult>(
            SurgewaveOpCode.GetSchemaById, _ => new SchemaOps.GetSchemaByIdOperation(schemaStore));
        Register<SchemaOps.GetSchemaByVersionRequest, SchemaOps.GetSchemaByVersionResult>(
            SurgewaveOpCode.GetSchemaByVersion, _ => new SchemaOps.GetSchemaByVersionOperation(schemaStore));
        Register<SchemaOps.DeleteSubjectRequest, SchemaOps.DeleteSubjectResult>(
            SurgewaveOpCode.DeleteSubject, _ => new SchemaOps.DeleteSubjectOperation(schemaStore));
        Register<SchemaOps.DeleteSchemaVersionRequest, SchemaOps.DeleteSchemaVersionResult>(
            SurgewaveOpCode.DeleteSchemaVersion, _ => new SchemaOps.DeleteSchemaVersionOperation(schemaStore));
        Register<SchemaOps.CheckCompatibilityRequest, SchemaOps.CheckCompatibilityResult>(
            SurgewaveOpCode.CheckCompatibility, _ => new SchemaOps.CheckCompatibilityOperation(schemaStore, compatibilityChecker));
        Register<SchemaOps.GetCompatibilityConfigRequest, SchemaOps.GetCompatibilityConfigResult>(
            SurgewaveOpCode.GetCompatibilityConfig, _ => new SchemaOps.GetCompatibilityConfigOperation(schemaStore));
        Register<SchemaOps.SetCompatibilityConfigRequest, SchemaOps.SetCompatibilityConfigResult>(
            SurgewaveOpCode.SetCompatibilityConfig, _ => new SchemaOps.SetCompatibilityConfigOperation(schemaStore));
        RegisterNoRequest<SchemaOps.GetSchemaTypesResult>(
            SurgewaveOpCode.GetSchemaTypes, _ => new SchemaOps.GetSchemaTypesOperation(compatibilityChecker));
    }
}
