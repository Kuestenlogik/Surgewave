namespace Kuestenlogik.Surgewave.Core.Storage;

/// <summary>
/// Well-known storage engine names. Plugins can register additional engines.
/// </summary>
public static class StorageEngines
{
    public const string File = "file";
    public const string Memory = "memory";
    public const string ZeroCopyWal = "zerocopy-wal";
    public const string ZeroCopyMemory = "zerocopy-memory";
    public const string ObjectStore = "objectstore";
}
