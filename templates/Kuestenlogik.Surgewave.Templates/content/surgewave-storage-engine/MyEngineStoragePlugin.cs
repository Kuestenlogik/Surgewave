using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Configuration;

namespace MyEngine;

/// <summary>
/// Surgewave storage-engine plugin. The broker activates this plugin when
/// the operator sets <c>Surgewave:Storage:Engine</c> to
/// <see cref="StorageEngineName"/> (or any other value in
/// <see cref="SupportedModes"/>).
/// </summary>
public sealed class MyEngineStoragePlugin : IStorageEnginePlugin
{
    public string FeatureId => "FEATURE_ID";
    public string DisplayName => "MyEngine";
    public string StorageEngineName => "ENGINE_NAME";
    public IReadOnlyList<string> SupportedModes => [ "ENGINE_NAME" ];

    public ILogSegmentFactory CreateFactory(string storageEngine, IConfiguration configuration)
    {
        // TODO: return your real ILogSegmentFactory implementation. The stub
        // below makes the project compile + the test pass against the contract;
        // replace it with a factory backed by your chosen storage engine.
        throw new NotImplementedException(
            "Replace this stub with a real ILogSegmentFactory for engine '" + storageEngine + "'.");
    }
}
