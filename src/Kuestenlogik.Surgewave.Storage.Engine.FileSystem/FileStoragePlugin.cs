using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Configuration;

namespace Kuestenlogik.Surgewave.Storage.Engine.FileSystem;

/// <summary>
/// Built-in storage engine plugin for file-based storage.
/// </summary>
public sealed class FileStoragePlugin : IStorageEnginePlugin
{
    public string FeatureId => "Surgewave.Storage.File";
    public string DisplayName => "File Storage";
    public string StorageEngineName => "file";
    public IReadOnlyList<string> SupportedModes { get; } = ["file", "zerocopy-wal"];

    public ILogSegmentFactory CreateFactory(string storageEngine, IConfiguration configuration)
        => FileLogSegmentFactory.Create(useMmap: true);
}
