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
surgewave plugins install path/to/plugin.swpkg

# List installed plugins
surgewave plugins list

# Uninstall
surgewave plugins uninstall <plugin-id>
```

### Where plugins live

An installed plugin is data, not part of the installation, so it survives an
upgrade. Three directories are searched, in this order — a plugin id found later
replaces the same id found earlier, and the broker logs which copy it shadowed:

| Scope | Windows | Linux / macOS | Who put it there |
|---|---|---|---|
| installation | `<install>/plugins` | `<install>/plugins` | shipped with the artefact; replaced wholesale on upgrade |
| machine | `%ProgramData%\Surgewave\plugins` | `/var/lib/surgewave/plugins` | an operator, for this host |
| user | `~/.surgewave/plugins` | `~/.surgewave/plugins` | one account, for a broker it runs itself |

`surgewave plugins install` writes to the **machine** scope by default, because a
broker usually runs under a service account: a plugin in someone's home directory
is invisible to it. When that directory is not writable the command fails and
names it — it does not quietly fall back to the user scope, which would report
success for an install the broker could never see.

```bash
# For every account on this machine (needs elevation)
surgewave plugins install plugin.swpkg

# For this account only — a broker running as a service will not see it
surgewave plugins install plugin.swpkg --scope user

# One explicit directory, ignoring the scopes entirely
surgewave plugins install plugin.swpkg --directory /srv/surgewave/plugins
```

`surgewave plugins list` shows which scope each plugin came from and names every
directory it searched.

### Configuration

| Setting | Effect |
|---|---|
| `Surgewave:PluginsDirectory` | One directory, and nothing else is searched. What a container image mounting a single directory wants. |
| `Surgewave:Plugins:AllowUserScope` | `false` drops the per-user directory. A plugin there is code the broker runs that an unprivileged account could place on disk, so an administrator can refuse it. Put it in the installation's `appsettings.json` or in machine-scope configuration — anywhere the account it restricts cannot edit it. |
| `Surgewave:Instance` / `SURGEWAVE_INSTANCE` | Adds one path segment to the machine and user scopes, so several brokers share a host without sharing state. Unset means the scope is the root itself. Not the cluster id: two nodes of one cluster on one host share that, and they are exactly the pair that must not share a directory. |
| `SURGEWAVE_DATA_DIR` | Moves every scope under one root. Intended for tests and throwaway environments — one tree to create, one to delete. |

Connectors follow the same three scopes under `connectors/` instead of
`plugins/`, configurable through `Surgewave:Connect:PluginsDirectory`.

## Kafka is a plugin, too

The Kafka wire protocol is not part of the broker — it ships as `kuestenlogik.surgewave.protocol.kafka`, exactly like MQTT or AMQP. `surgewave-broker.dll` holds no reference to `Kuestenlogik.Surgewave.Protocol.Kafka`, so a published broker contains the Kafka assembly only under `plugins/kuestenlogik.surgewave.protocol.kafka/`.

**Delete that directory and the broker speaks native only.** No rebuild, no config change:

```
plugins/kuestenlogik.surgewave.protocol.kafka/   ← remove to drop Kafka support
```

A Kafka client then gets its connection closed at the magic-byte probe, because no handler claims those bytes. The broker says so on startup — it reports what the listener actually speaks, not what the config asks for:

```
Kafka is enabled in configuration but no Kafka protocol plugin is installed — the broker
listens native-only on localhost:9092 and will close Kafka clients. Install the plugin
(plugins/kuestenlogik.surgewave.protocol.kafka) or set Surgewave:Kafka:Enabled=false.
```

`Surgewave:Kafka:Enabled` (default `true`) is the second switch: with the plugin installed it turns the wire protocol off without removing anything. Both switches lead to the same listener behaviour; the difference is that removing the plugin also removes the code from the process.

Kafka shares the broker's main listener (protocol detection on the first bytes), so no separate port appears or disappears either way.

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
