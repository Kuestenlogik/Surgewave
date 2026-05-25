using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Storage.Engine.FileSystem;

/// <summary>
/// Registers file storage engines with the StorageRegistry.
/// </summary>
public static class StorageRegistration
{
    private static bool _registered;
    private static readonly object _lock = new();

    /// <summary>
    /// Register file storage engines.
    /// Call this at startup or use the module initializer.
    /// </summary>
    public static void Register()
    {
        if (_registered) return;

        lock (_lock)
        {
            if (_registered) return;

            StorageRegistry.Default.Register("file", () => FileLogSegmentFactory.Create(useMmap: true));
            StorageRegistry.Default.Register("file-nommap", () => FileLogSegmentFactory.Create(useMmap: false));

            _registered = true;
        }
    }
}

/// <summary>
/// Module initializer that auto-registers file storages when assembly loads.
/// </summary>
file static class ModuleInitializer
{
    [System.Runtime.CompilerServices.ModuleInitializer]
    internal static void Initialize()
    {
        StorageRegistration.Register();
    }
}
