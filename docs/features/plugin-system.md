# Plugin System

Surgewave uses a unified plugin system for extending the broker with enterprise features, storage engines, protocol adapters, and pipeline nodes.

## Plugin Types

| Interface | Purpose | Discovery |
|-----------|---------|-----------|
| `IBrokerPlugin` | Enterprise features (DataMesh, Privacy, etc.) | `BrokerPluginActivator.ActivatePlugins()` |
| `IProtocolPlugin` | Protocol adapters (MQTT, WebSocket, AMQP) | `BrokerPluginActivator.ActivateProtocols()` |
| `IStorageEnginePlugin` | Storage engines (Arrow, DuckDb, etc.) | `BrokerPluginActivator.Discover<>()` |
| `ITieredStoragePlugin` | Tiered storage providers (S3, Azure, GCP) | `TieredStorageInitializer` |
| `IPipelineNode` | Pipeline nodes (Source, Sink, Processor) | `PluginDiscovery` |

## Installing Plugins

```bash
# Install from .swpkg file
surgewave plugin install path/to/plugin.swpkg

# List installed plugins
surgewave plugin list

# Uninstall
surgewave plugin uninstall <plugin-id>
```

## Plugin Package Format (.swpkg)

Surgewave Plugin Packages are ZIP archives containing:
- `plugin.json` -- manifest (id, version, targets)
- `lib/` -- DLL assemblies per role (broker, worker, control)
- `deps/` -- external dependencies

## Creating Plugins

Implement one of the plugin interfaces and package as .swpkg:

```csharp
public sealed class MyBrokerPlugin : IBrokerPlugin
{
    public string FeatureId => "MyCompany.MyFeature";
    public string DisplayName => "My Feature";

    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:MyFeature:Enabled", false);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
        => services.AddMyFeature(configuration);

    public void Configure(object host, IServiceProvider services)
    {
        if (host is IEndpointRouteBuilder endpoints)
            endpoints.MapMyFeatureApi();
    }
}
```

## Storage Engine Plugins

```csharp
public sealed class MyStoragePlugin : IStorageEnginePlugin
{
    public string FeatureId => "MyCompany.Storage";
    public string DisplayName => "My Storage Engine";
    public string StorageEngineName => "my-engine";
    public IReadOnlyList<string> SupportedModes { get; } = ["my-engine"];

    public ILogSegmentFactory CreateFactory(string storageEngine, IConfiguration configuration)
        => new MyLogSegmentFactory();
}
```

Configure: `Surgewave:Storage:Engine = "my-engine"`
