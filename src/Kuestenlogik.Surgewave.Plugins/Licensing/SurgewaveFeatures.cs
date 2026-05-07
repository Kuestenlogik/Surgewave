namespace Kuestenlogik.Surgewave.Plugins.Licensing;

/// <summary>
/// Well-known feature identifiers for Surgewave licensing.
/// Enterprise features require a license provider; community features are always available.
/// </summary>
public static class SurgewaveFeatures
{
    // -- Enterprise Features (require license) --

    public const string Replication = "Surgewave.Replication";
    public const string TieredStorage = "Surgewave.Storage.Tiering";
    public const string StorageNvmeDirect = "Surgewave.Storage.NvmeDirect";
    public const string StorageArrow = "Surgewave.Storage.Arrow";
    public const string StorageDuckDb = "Surgewave.Storage.DuckDb";
    public const string StorageParquet = "Surgewave.Storage.Parquet";
    public const string AI = "Surgewave.AI";
    public const string MultiTenancy = "Surgewave.MultiTenancy";
    public const string DataMesh = "Surgewave.DataMesh";
    public const string Functions = "Surgewave.Functions";
    public const string Privacy = "Surgewave.Privacy";
    public const string TransportSharedMemory = "Surgewave.Transport.SharedMemory";
    public const string Operator = "Surgewave.Operator";

    /// <summary>All enterprise feature identifiers.</summary>
    public static IReadOnlySet<string> EnterpriseFeatures { get; } = new HashSet<string>
    {
        Replication, TieredStorage, StorageNvmeDirect,
        StorageArrow, StorageDuckDb, StorageParquet, AI,
        MultiTenancy, DataMesh, Functions, Privacy,
        TransportSharedMemory, Operator,
    };

    /// <summary>Check if a feature requires a license.</summary>
    public static bool IsEnterpriseFeature(string featureName) =>
        EnterpriseFeatures.Contains(featureName);

    // -- Community Features (always available) --

    public const string Clustering = "Surgewave.Clustering";
    public const string Streams = "Surgewave.Streams";
    public const string Wasm = "Surgewave.Wasm";
    public const string ApiGraphQL = "Surgewave.Api.GraphQL";
    public const string ApiGrpc = "Surgewave.Api.Grpc";
    public const string Gateway = "Surgewave.Gateway";
    public const string Cdc = "Surgewave.Cdc";
    public const string Edge = "Surgewave.Edge";
    public const string ConnectEnterprise = "Surgewave.Connect.Enterprise";
    public const string Control = "Surgewave.Control";
    public const string ChaosTesting = "Surgewave.Testing.Chaos";
}
