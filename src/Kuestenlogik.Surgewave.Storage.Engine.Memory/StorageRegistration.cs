using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Storage.Engine.Memory;

/// <summary>
/// Registers memory storage engines with the StorageRegistry.
/// </summary>
public static class StorageRegistration
{
    private static bool _registered;
    private static readonly Lock _lock = new();

    /// <summary>
    /// Register memory storage engines.
    /// Call this at startup or use the module initializer.
    /// </summary>
    public static void Register()
    {
        if (_registered) return;

        lock (_lock)
        {
            if (_registered) return;

            StorageRegistry.Default.Register("memory", () => new MemoryLogSegmentFactory());
            StorageRegistry.Default.Register("zerocopy-memory", ZeroCopyMemoryLogSegmentFactory.Create);

            _registered = true;
        }
    }
}

/// <summary>
/// Module initializer that auto-registers memory storages when assembly loads.
/// </summary>
file static class ModuleInitializer
{
    [System.Runtime.CompilerServices.ModuleInitializer]
    internal static void Initialize()
    {
        StorageRegistration.Register();
    }
}
