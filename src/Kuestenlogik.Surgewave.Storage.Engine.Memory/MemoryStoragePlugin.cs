using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Configuration;

namespace Kuestenlogik.Surgewave.Storage.Engine.Memory;

/// <summary>
/// Built-in storage engine plugin for in-memory storage.
/// </summary>
public sealed class MemoryStoragePlugin : IStorageEnginePlugin
{
    public string FeatureId => "Surgewave.Storage.Memory";
    public string DisplayName => "Memory Storage";
    public string StorageEngineName => "memory";
    public IReadOnlyList<string> SupportedModes { get; } = ["memory"];

    public ILogSegmentFactory CreateFactory(string storageEngine, IConfiguration configuration)
        => new MemoryLogSegmentFactory();
}

/// <summary>
/// Built-in storage engine plugin for zero-copy memory storage.
/// </summary>
public sealed class ZeroCopyMemoryStoragePlugin : IStorageEnginePlugin
{
    public string FeatureId => "Surgewave.Storage.ZeroCopyMemory";
    public string DisplayName => "Zero-Copy Memory Storage";
    public string StorageEngineName => "zerocopy-memory";
    public IReadOnlyList<string> SupportedModes { get; } = ["zerocopy-memory"];

    public ILogSegmentFactory CreateFactory(string storageEngine, IConfiguration configuration)
        => ZeroCopyMemoryLogSegmentFactory.Create();
}
