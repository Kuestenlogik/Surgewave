using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Storage.Engine.Memory;
using Microsoft.Extensions.Configuration;

namespace Kuestenlogik.Surgewave.Samples.StorageEngine;

/// <summary>
/// Smallest possible <see cref="IStorageEnginePlugin"/>: wraps the
/// built-in <see cref="MemoryLogSegmentFactory"/> under the engine
/// name <c>sample-memory</c>. Operators activate by setting
/// <c>Surgewave:Storage:Engine=sample-memory</c>.
///
/// Real storage engines (Arrow, NVMe-direct, S3-backed …) implement
/// their own <see cref="ILogSegmentFactory"/> and may consult the
/// configuration for engine-specific tuning. The point of this sample
/// is to show the minimum surface area for a working plugin.
/// </summary>
public sealed class SampleMemoryStorageEnginePlugin : IStorageEnginePlugin
{
    public string FeatureId => "Kuestenlogik.Surgewave.Samples.StorageEngine";
    public string DisplayName => "Sample Memory Storage Engine";

    public string StorageEngineName => "sample-memory";
    public IReadOnlyList<string> SupportedModes => [ "sample-memory" ];

    public ILogSegmentFactory CreateFactory(string storageEngine, IConfiguration configuration)
    {
        // Engine-specific tuning would read from configuration here, e.g.:
        //   var initialCapacity = configuration.GetValue<int>("Surgewave:Storage:SampleMemory:InitialCapacity", 4096);
        // The MemoryLogSegmentFactory takes no parameters, so we just return it.
        return new MemoryLogSegmentFactory();
    }
}
