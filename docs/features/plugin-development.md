# Plugin Development Guide

This guide walks you through creating, packaging, installing, and publishing Surgewave plugins.

## Quick Start

Create a working plugin in five steps:

**1. Create a .NET 10 class library**

```bash
dotnet new classlib -n Acme.Surgewave.Connector.Foo --framework net10.0
cd Acme.Surgewave.Connector.Foo
```

**2. Reference Kuestenlogik.Surgewave.Plugins**

```bash
dotnet add package Kuestenlogik.Surgewave.Plugins
```

**3. Implement a plugin interface**

```csharp
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Plugins.Pipeline;

[ConnectorMetadata(Name = "Foo Source", Description = "Reads events from Foo API",
    Author = "Acme Corp", Tags = "integration,cloud")]
public sealed class FooSourceNode : ISourceNode
{
    public string FeatureId => "Acme.Surgewave.Connector.Foo.Source";
    public string DisplayName => "Foo Source";
    public int InputPorts => 0;
    public int OutputPorts => 1;
    public string Version => "1.0.0";

    public ConfigDef Config => new ConfigDef()
        .Define("foo.api.url", ConfigType.String, Importance.High, "Foo API base URL")
        .Define("foo.api.key", ConfigType.Password, Importance.High, "API key for authentication")
        .Define("foo.poll.interval.ms", ConfigType.Int, 5000, Importance.Medium, "Poll interval in milliseconds");
}
```

**4. Create `plugin.json` in the project root**

```json
{
  "$schema": "https://raw.githubusercontent.com/your-org/surgewave/main/schemas/surgewave-plugin.schema.json",
  "id": "Acme.Surgewave.Connector.Foo",
  "name": "Foo Connector",
  "version": "1.0.0",
  "description": "Source connector for the Foo API",
  "authors": ["Acme Corp"],
  "license": "MIT",
  "icon": "icon.png",
  "assemblies": ["Acme.Surgewave.Connector.Foo.dll"],
  "dependencies": {},
  "minRuntimeVersion": "0.1.0"
}
```

**5. Build, pack, and install**

```bash
dotnet build -c Release
surgewave plugin pack --project . --output artifacts/pkg/
surgewave plugin install artifacts/pkg/Acme.Surgewave.Connector.Foo-1.0.0.swpkg
```

## Plugin Types

Every plugin implements `IPlugin` (base interface with `FeatureId` and `DisplayName`). Choose the specific sub-interface based on what you are building:

| Interface | Namespace | Purpose | When to use |
|-----------|-----------|---------|-------------|
| `ISourceNode` | `Kuestenlogik.Surgewave.Plugins.Pipeline` | Produces data from external systems | Polling-based data ingestion (databases, APIs, file systems) |
| `ISinkNode` | `Kuestenlogik.Surgewave.Plugins.Pipeline` | Writes data to external systems | Sending data out (databases, APIs, object stores) |
| `IProcessorNode` | `Kuestenlogik.Surgewave.Plugins.Pipeline` | Transforms data in a pipeline | Enrichment, aggregation, filtering with multiple I/O ports |
| `ITriggerNode` | `Kuestenlogik.Surgewave.Plugins.Pipeline` | Triggers on events (cron, webhook) | Event-driven start nodes (unlike source, not polling-based) |
| `ISingleMessageTransform` | `Kuestenlogik.Surgewave.Plugins.Pipeline` | Inline per-record transform | Lightweight field mapping, filtering, renaming -- runs inline on connections |
| `IBrokerPlugin` | `Kuestenlogik.Surgewave.Plugins` | Extends the broker with features | Enterprise features, custom DI services, HTTP endpoints |
| `IProtocolPlugin` | `Kuestenlogik.Surgewave.Plugins` | Protocol adapters | Adding MQTT, AMQP, WebSocket, or custom protocol support |
| `IStorageEnginePlugin` | `Kuestenlogik.Surgewave.Plugins` | Custom storage engines | Alternative log segment storage (Arrow, DuckDB, Parquet) |
| `ITieredStoragePlugin` | `Kuestenlogik.Surgewave.Plugins` | Tiered storage providers | Offloading segments to S3, Azure Blob, GCP Cloud Storage |

### IPlugin (base)

```csharp
public interface IPlugin
{
    string FeatureId { get; }
    string DisplayName { get; }
}
```

All interfaces inherit from `IPlugin`. Pipeline nodes (`ISourceNode`, `ISinkNode`, `IProcessorNode`, `ITriggerNode`) additionally inherit from `IPipelineNode` which adds:

```csharp
public interface IPipelineNode : IPlugin
{
    int InputPorts { get; }   // 0 = start/source node
    int OutputPorts { get; }  // 0 = end/sink node
    ConfigDef Config { get; }
    string Version { get; }
}
```

## The plugin.json Manifest

Every `.swpkg` package must contain a `plugin.json` at its root. Full field reference:

```json
{
  "$schema": "https://raw.githubusercontent.com/your-org/surgewave/main/schemas/surgewave-plugin.schema.json",
  "id": "Acme.Surgewave.Connector.Foo",
  "name": "Foo Connector",
  "version": "1.0.0",
  "description": "Source and sink connectors for the Foo platform",
  "authors": ["Acme Corp", "Jane Developer"],
  "license": "MIT",
  "projectUrl": "https://github.com/acme/surgewave-connector-foo",
  "tags": ["integration", "cloud", "foo"],
  "icon": "icon.png",
  "assemblies": ["Acme.Surgewave.Connector.Foo.dll"],
  "dependencies": {
    "Foo.Client.SDK": "3.2.1"
  },
  "surgewaveDependencies": [
    {
      "id": "Kuestenlogik.Surgewave.Connect",
      "version": ">=0.1.0",
      "optional": false
    }
  ],
  "minRuntimeVersion": "0.1.0"
}
```

### Field Reference

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique package identifier (reverse-DNS style: `Acme.Surgewave.Connector.Foo`) |
| `name` | Yes | Human-readable display name |
| `version` | Yes | Semantic version (e.g., `1.0.0`) |
| `assemblies` | Yes | DLLs to scan for `IPlugin` implementations. Other DLLs in `lib/` load as dependencies but are not scanned. |
| `description` | No | Short description for Marketplace and CLI |
| `authors` | No | Array of author names |
| `license` | No | SPDX license identifier (e.g., `MIT`, `Apache-2.0`) |
| `projectUrl` | No | URL to source repository or documentation |
| `tags` | No | Array of keywords for categorization and search |
| `icon` | No | Path to icon file inside the package (PNG or SVG, recommended 128x128 or 256x256). Displayed in Surgewave Control and Marketplace. |
| `dependencies` | No | External NuGet dependencies as `{ "PackageName": "Version" }` |
| `surgewaveDependencies` | No | Dependencies on other Surgewave plugins. Supports version constraints: exact (`1.0.0`), range (`>=1.0.0`), caret (`^1.0.0` = same major), tilde (`~1.0.0` = same major.minor). |
| `minRuntimeVersion` | No | Minimum Surgewave runtime version required |
| `sha256` | No | Package checksum (auto-populated by `surgewave plugin pack`) |
| `$schema` | No | JSON Schema URL for editor auto-complete |

## Building and Packaging

### Using the surgewave CLI

The CLI `pack` command creates an `.swpkg` from your build output:

```bash
# Install the CLI globally
dotnet tool install -g Kuestenlogik.Surgewave.Cli

# Build your plugin
dotnet build -c Release

# Pack into .swpkg
surgewave plugin pack --project src/MyPlugin/ --output artifacts/pkg/

# Options
surgewave plugin pack --project src/MyPlugin/ --output artifacts/pkg/ --configuration Release --manifest path/to/custom-manifest.json
```

The `pack` command:
1. Locates build output in `artifacts/bin/<ProjectName>/release/` or `bin/Release/net10.0/`
2. Reads `plugin.json` from the project directory (or `--manifest` path)
3. Creates a ZIP archive named `<Id>-<Version>.swpkg`
4. Computes and displays the SHA256 checksum

### Auto-pack on publish (Kuestenlogik.Surgewave.Sdk)

`Kuestenlogik.Surgewave.Sdk` is the meta-package for plugin development — one
`<PackageReference>` pulls in the plugin contracts (`Kuestenlogik.Surgewave.Plugins`),
the MSBuild pack/install/sign tasks (`Kuestenlogik.Surgewave.Build`), and the embedded-
runtime test fixtures (`Kuestenlogik.Surgewave.Testing`):

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net10.0</TargetFramework>
    <SurgewavePackPlugin>true</SurgewavePackPlugin>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Kuestenlogik.Surgewave.Sdk" PrivateAssets="all" />
  </ItemGroup>
</Project>
```

Then:

```bash
dotnet publish -c Release
# .swpkg is automatically packed under artifacts/pub/packages/
```

Control the behavior with MSBuild properties:

| Property | Default | Description |
|----------|---------|-------------|
| `SurgewavePackPlugin` | `false` | Set to `true` to enable auto-packing on publish |
| `SurgewaveSppOutputDir` | `artifacts/pub/packages/` (artifacts layout) or `pluginPackage/` (classic) | Output directory for `.swpkg` files |
| `SurgewaveInstallPlugin` | `false` | Set to `true` to install the packed `.swpkg` into a plugins directory |
| `SurgewavePluginsDir` | — | Required when `SurgewaveInstallPlugin=true`; the broker's plugins folder |
| `SurgewaveSigningKey` | — | Path to an ECDSA P-256 PEM private key for `.swpkg` signing |
| `SurgewaveCleanupPublish` | `true` (when packing) | Delete the staging publish output after pack/install |

> **Want only one of the three?** Reference the sub-packages directly:
> `Kuestenlogik.Surgewave.Plugins` for the contract surface, `Kuestenlogik.Surgewave.Build` for the
> MSBuild tasks, `Kuestenlogik.Surgewave.Testing` for the test fixtures. The Sdk just
> bundles them — no surprise dependencies.

### Package Structure

A `.swpkg` file is a standard ZIP archive with this layout:

```
MyPlugin-1.0.0.swpkg (ZIP)
+-- plugin.json       # Required: manifest
+-- lib/
|   +-- MyPlugin.dll        # Your plugin assembly
|   +-- SomeDependency.dll  # Third-party dependencies
+-- icon.png                # Optional: plugin icon
+-- README.md               # Optional: documentation
+-- LICENSE                  # Optional: license file
```

The `assemblies` array in the manifest controls which DLLs are scanned for `IPlugin` implementations. All other DLLs in `lib/` are loaded as dependencies.

## Installing and Managing Plugins

### Install from local file

```bash
surgewave plugin install MyPlugin-1.0.0.swpkg
```

### Install all plugins in a directory

```bash
surgewave plugin install artifacts/plugins/
```

### Recursive install (all .swpkg files in subdirectories)

```bash
surgewave plugin install artifacts/**
```

### Install from a configured plugin source

```bash
# Add a source first
surgewave plugin source add myregistry https://registry.example.com --type http

# Install by package ID
surgewave plugin install Acme.Surgewave.Connector.Foo --source myregistry
```

### Install from NuGet

```bash
surgewave plugin install Acme.Surgewave.Connector.Foo --from-nuget
surgewave plugin install Acme.Surgewave.Connector.Foo --from-nuget --version 1.2.0
```

### Install from URL

```bash
surgewave plugin install --from-url https://releases.example.com/MyPlugin-1.0.0.swpkg
```

### Dependency resolution

```bash
# Install with automatic dependency resolution (default)
surgewave plugin install Acme.Surgewave.Connector.Foo --from-nuget

# Skip dependency resolution
surgewave plugin install Acme.Surgewave.Connector.Foo --from-nuget --no-deps

# Preview what would be installed
surgewave plugin install Acme.Surgewave.Connector.Foo --from-nuget --dry-run
```

### List installed plugins

```bash
surgewave plugin list
```

### Uninstall

```bash
surgewave plugin uninstall Acme.Surgewave.Connector.Foo
```

### Force overwrite

```bash
surgewave plugin install MyPlugin-1.0.0.swpkg --force
```

## Example: Creating a Source Connector

A source connector reads data from an external system and produces records into Surgewave topics.

```csharp
using Kuestenlogik.Surgewave.Connect;
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Plugins.Pipeline;

[ConnectorMetadata(
    Name = "PostgreSQL Source",
    Description = "Captures changes from PostgreSQL tables using logical replication",
    Author = "Acme Corp",
    Tags = "database,sql,cdc",
    Icon = "Database",
    DocumentationUrl = "https://docs.example.com/connectors/postgresql-source")]
public sealed class PostgresSourceNode : ISourceNode
{
    public string FeatureId => "Acme.Surgewave.Connector.Postgres.Source";
    public string DisplayName => "PostgreSQL Source";
    public int InputPorts => 0;   // Source: no inputs
    public int OutputPorts => 1;  // One output port for captured records
    public string Version => "1.0.0";

    public ConfigDef Config => new ConfigDef()
        .Define("connection.url", ConfigType.String, Importance.High,
            "JDBC-style connection URL (e.g., postgresql://host:5432/mydb)")
        .Define("connection.user", ConfigType.String, Importance.High,
            "Database username")
        .Define("connection.password", ConfigType.Password, Importance.High,
            "Database password")
        .Define("table.include.list", ConfigType.String, Importance.High,
            "Comma-separated list of tables to capture (e.g., public.orders,public.users)")
        .Define("slot.name", ConfigType.String, "surgewave_slot", Importance.Medium,
            "Logical replication slot name")
        .Define("snapshot.mode", ConfigType.String, "initial", Importance.Medium,
            "Snapshot mode: initial, never, when_needed",
            EditorHint.Select, options: ["initial", "never", "when_needed"])
        .Define("poll.interval.ms", ConfigType.Int, 1000, Importance.Low,
            "Poll interval in milliseconds");
}
```

The `ConnectorMetadataAttribute` provides metadata for the Surgewave Control pipeline editor UI and the Marketplace:

- **Name**: Displayed in the node palette
- **Description**: Shown as tooltip/detail text
- **Tags**: Used for categorization (comma-separated) -- drives the `Category` grouping in the UI
- **Icon**: MudBlazor icon name or `resource:Namespace.Icons.MyIcon.svg` for embedded SVG
- **Author**, **DocumentationUrl**, **LicenseUrl**: Shown in the plugin detail view

## Example: Creating a Broker Plugin

Broker plugins extend the Surgewave Broker with custom services and HTTP endpoints. They participate in the broker's DI container and lifecycle.

```csharp
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

public sealed class AuditLogPlugin : IBrokerPlugin
{
    public string FeatureId => "Acme.Surgewave.AuditLog";
    public string DisplayName => "Audit Log";

    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue("Surgewave:AuditLog:Enabled", false);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<AuditLogOptions>(configuration.GetSection("Surgewave:AuditLog"));
        services.AddSingleton<IAuditLogService, AuditLogService>();
    }

    public void Configure(object host, IServiceProvider services)
    {
        // Map HTTP endpoints after app.Build()
        if (host is IEndpointRouteBuilder endpoints)
        {
            endpoints.MapGet("/api/audit", async (IAuditLogService audit) =>
            {
                var entries = await audit.GetRecentAsync(100);
                return Results.Ok(entries);
            });
        }
    }
}

public sealed class AuditLogOptions
{
    public bool Enabled { get; set; }
    public string StoragePath { get; set; } = "data/audit";
    public int RetentionDays { get; set; } = 90;
}

public interface IAuditLogService
{
    Task<IReadOnlyList<AuditEntry>> GetRecentAsync(int count);
}
```

The broker activates plugins automatically at startup via `BrokerPluginActivator`:

1. Scans loaded assemblies for `IBrokerPlugin` implementations
2. Calls `IsConfigEnabled()` -- skips disabled plugins
3. Checks enterprise license (if `SurgewaveFeatures.IsEnterpriseFeature(featureId)` returns true)
4. Calls `ConfigureServices()` to register into DI
5. After `app.Build()`, calls `Configure()` to map endpoints

Enable in `appsettings.json`:

```json
{
  "Surgewave": {
    "AuditLog": {
      "Enabled": true,
      "StoragePath": "data/audit",
      "RetentionDays": 90
    }
  }
}
```

## Example: Creating a Storage Engine Plugin

Storage engine plugins provide alternative log segment implementations. They are loaded early during startup, before the DI container is built.

```csharp
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Configuration;

public sealed class RocksDbStoragePlugin : IStorageEnginePlugin
{
    public string FeatureId => "Acme.Surgewave.Storage.RocksDb";
    public string DisplayName => "RocksDB Storage Engine";
    public string StorageEngineName => "rocksdb";
    public IReadOnlyList<string> SupportedModes { get; } = ["rocksdb", "rocksdb-optimistic"];

    public ILogSegmentFactory CreateFactory(string storageEngine, IConfiguration configuration)
    {
        var path = configuration.GetValue("Surgewave:Storage:DataDirectory", "data/logs");
        var optimistic = storageEngine == "rocksdb-optimistic";

        return new RocksDbLogSegmentFactory(path, optimistic);
    }
}
```

Activate by setting the storage engine in configuration:

```json
{
  "Surgewave": {
    "Storage": {
      "Engine": "rocksdb"
    }
  }
}
```

The `SupportedModes` list declares all engine names this plugin handles. The exact name from configuration is passed to `CreateFactory()`, so a single plugin can support multiple variants.

## Example: Creating a Protocol Plugin

Protocol plugins add support for alternative wire protocols (MQTT, AMQP, WebSocket, etc.).

```csharp
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

public sealed class MqttProtocolPlugin : IProtocolPlugin
{
    public string FeatureId => "Acme.Surgewave.Protocol.Mqtt";
    public string DisplayName => "MQTT Protocol";
    public int DefaultPort => 1883;

    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue("Surgewave:Protocols:Mqtt:Enabled", false);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<MqttOptions>(configuration.GetSection("Surgewave:Protocols:Mqtt"));
        services.AddHostedService<MqttListenerService>();
    }

    public void Configure(object host, IServiceProvider services)
    {
        // Optional: map HTTP health endpoint for MQTT
        if (host is IEndpointRouteBuilder endpoints)
        {
            endpoints.MapGet("/api/protocols/mqtt/status", () => Results.Ok(new { Protocol = "MQTT", Status = "running" }));
        }
    }
}
```

## Example: Creating a Single Message Transform

SMTs run inline on connections between pipeline nodes -- they have no separate task or topic.

```csharp
using Kuestenlogik.Surgewave.Plugins;
using Kuestenlogik.Surgewave.Plugins.Configuration;
using Kuestenlogik.Surgewave.Plugins.Pipeline;

[PluginMetadata(Name = "Add Timestamp", Description = "Adds a processing timestamp header",
    Tags = "transform,logic")]
public sealed class AddTimestampTransform : ISingleMessageTransform
{
    public string FeatureId => "Acme.Surgewave.Transform.AddTimestamp";
    public string DisplayName => "Add Timestamp";

    private string _headerName = "processing-timestamp";

    public ConfigDef Config => new ConfigDef()
        .Define("header.name", ConfigType.String, "processing-timestamp", Importance.Medium,
            "Name of the header to add");

    public void Configure(IDictionary<string, string> config)
    {
        if (config.TryGetValue("header.name", out var name))
            _headerName = name;
    }

    public (byte[]? Key, byte[] Value, IDictionary<string, string>? Headers)? Apply(
        byte[]? key, byte[] value, IDictionary<string, string>? headers)
    {
        headers ??= new Dictionary<string, string>();
        headers[_headerName] = DateTimeOffset.UtcNow.ToString("O");
        return (key, value, headers);
    }
}
```

## Testing Plugins

The `Kuestenlogik.Surgewave.Testing` package provides helpers for unit testing plugins without running a full broker.

```bash
dotnet add package Kuestenlogik.Surgewave.Testing
```

### Testing with TestLogManager

`TestLogManager.CreateInMemory()` creates an in-memory `LogManager` backed by `MemoryLogSegmentFactory` -- no disk I/O, no persistence:

```csharp
using Kuestenlogik.Surgewave.Testing;
using Xunit;

public class MyPluginTests
{
    [Fact]
    public void Source_node_config_has_required_fields()
    {
        var node = new PostgresSourceNode();

        Assert.Equal("Acme.Surgewave.Connector.Postgres.Source", node.FeatureId);
        Assert.Equal(0, node.InputPorts);
        Assert.Equal(1, node.OutputPorts);

        var requiredKeys = node.Config.Keys
            .Where(k => k.Importance == Importance.High)
            .Select(k => k.Name)
            .ToList();

        Assert.Contains("connection.url", requiredKeys);
        Assert.Contains("connection.user", requiredKeys);
        Assert.Contains("connection.password", requiredKeys);
    }

    [Fact]
    public async Task Storage_engine_creates_factory()
    {
        var plugin = new RocksDbStoragePlugin();

        Assert.Contains("rocksdb", plugin.SupportedModes);
        Assert.Equal("rocksdb", plugin.StorageEngineName);
    }

    [Fact]
    public void Transform_adds_timestamp_header()
    {
        var transform = new AddTimestampTransform();
        transform.Configure(new Dictionary<string, string>());

        var result = transform.Apply(null, [0x01, 0x02], null);

        Assert.NotNull(result);
        Assert.True(result.Value.Headers!.ContainsKey("processing-timestamp"));
    }

    [Fact]
    public void Broker_plugin_registers_services()
    {
        var plugin = new AuditLogPlugin();
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Surgewave:AuditLog:Enabled"] = "true"
            })
            .Build();

        Assert.True(plugin.IsConfigEnabled(config));

        plugin.ConfigureServices(services, config);

        Assert.Contains(services, sd => sd.ServiceType == typeof(IAuditLogService));
    }
}
```

### Testing with in-memory LogManager

For storage engine plugins, use `TestLogManager` to create an in-memory environment:

```csharp
[Fact]
public async Task LogManager_writes_and_reads()
{
    using var logManager = TestLogManager.CreateInMemory();

    // Use logManager to test your plugin's integration with the storage layer
}
```

## Publishing

### Local registry

```bash
# Publish to a local directory-based registry
surgewave plugin publish MyPlugin-1.0.0.swpkg --registry-path ./registry

# Publish to a named registry from configuration
surgewave plugin publish MyPlugin-1.0.0.swpkg --registry my-local-registry

# Overwrite an existing version
surgewave plugin publish MyPlugin-1.0.0.swpkg --registry-path ./registry --force
```

### NuGet feed

Package your plugin as a NuGet package and push to any NuGet feed:

```bash
dotnet nuget push MyPlugin.1.0.0.nupkg --source https://api.nuget.org/v3/index.json --api-key YOUR_API_KEY
```

Then consumers install with:

```bash
surgewave plugin install MyPlugin --from-nuget
```

### GitHub Releases

Attach the `.swpkg` file to a GitHub Release, then install via URL:

```bash
surgewave plugin install --from-url https://github.com/acme/surgewave-connector-foo/releases/download/v1.0.0/Acme.Surgewave.Connector.Foo-1.0.0.swpkg
```

### Plugin sources

Configure reusable plugin sources for your team:

```bash
# Add a plugin source
surgewave plugin source add company-registry https://registry.internal.example.com --type http

# Search available plugins
surgewave plugin search --source company-registry "postgres"

# Install from source by ID
surgewave plugin install Acme.Surgewave.Connector.Foo --source company-registry
```

## Best Practices

**Packaging**

- One plugin per `.swpkg` unless components are tightly coupled (e.g., a source and sink for the same system).
- Pin `minRuntimeVersion` to the lowest Surgewave version your plugin supports. This prevents install failures on older runtimes.
- Include `icon.png` (128x128 or 256x256 PNG/SVG) for display in Surgewave Control and the Marketplace.
- Use descriptive `tags` in the manifest -- they drive category grouping in the pipeline editor UI.

**Code**

- Use `ConnectorMetadataAttribute` (for connectors) or `PluginMetadataAttribute` (for other plugins) to provide display metadata. The pipeline editor reads these at runtime.
- Define all configuration via `ConfigDef` with appropriate `Importance` levels. `High` importance keys show first in the configuration UI.
- Use `EditorHint` to guide the UI: `Password` for secrets, `Select` for fixed options, `Code` for code/SQL editors, `Topic` for topic name pickers with auto-complete.
- Keep plugins stateless where possible. Configuration is injected through `ConfigDef` / `IConfiguration`, not constructors.

**Discovery**

- The `assemblies` array in the manifest controls which DLLs get scanned for `IPlugin` types. List only your plugin DLLs, not third-party dependencies.
- Class names must be unique across all loaded plugins. Use fully qualified namespaces.
- Plugins are instantiated via `Activator.CreateInstance()` -- they must have a public parameterless constructor.

**Versioning**

- Follow semantic versioning. Breaking config changes require a major version bump.
- Use `surgewaveDependencies` to declare dependencies on other Surgewave plugins with version constraints (`^1.0.0` for compatible, `~1.2.0` for patch-level, `>=1.0.0` for minimum).
- The `--dry-run` flag on `surgewave plugin install --from-nuget` shows the full dependency tree before installing.
