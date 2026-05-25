using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Configuration;

namespace Kuestenlogik.Surgewave.Plugins;

/// <summary>
/// Plugin that provides an additional storage engine for the Surgewave Broker.
/// Storage engine plugins are loaded early during startup (before DI container is built)
/// and create <see cref="ILogSegmentFactory"/> instances based on the configured storage engine name.
/// </summary>
public interface IStorageEnginePlugin : IPlugin
{
    /// <summary>
    /// The primary storage engine name this plugin handles (case-insensitive).
    /// A single plugin can handle multiple engines by returning the primary name here
    /// and checking the exact name in <see cref="CreateFactory"/>.
    /// </summary>
    string StorageEngineName { get; }

    /// <summary>
    /// All storage engine names this plugin can handle (e.g., "arrow", "arrow-mmap", "arrow-highcompression").
    /// </summary>
    IReadOnlyList<string> SupportedModes { get; }

    /// <summary>
    /// Creates a log segment factory for the specified storage engine.
    /// </summary>
    /// <param name="storageEngine">The exact storage engine name from configuration.</param>
    /// <param name="configuration">The application configuration for reading engine-specific settings.</param>
    ILogSegmentFactory CreateFactory(string storageEngine, IConfiguration configuration);
}
