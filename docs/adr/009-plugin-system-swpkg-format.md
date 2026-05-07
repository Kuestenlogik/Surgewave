# ADR-009: Plugin System & .swpkg Package Format

## Status

Accepted

## Date

2026-04

## Context

Surgewave needs a plugin system that allows enterprise features, storage engines, protocol adapters, pipeline nodes, and connectors to be developed, packaged, and deployed independently from the core broker. Kafka's monolithic architecture makes it difficult to add or remove capabilities without rebuilding the entire system. Surgewave's goal is a modular architecture where the broker binary ships with zero enterprise compile-time dependencies, and all extensions are loaded at runtime.

The plugin system must solve several problems simultaneously: discovery (finding plugins at startup), isolation (preventing dependency conflicts between plugins), activation (two-phase startup for DI registration and endpoint mapping), packaging (distributing plugins as self-contained units), and lifecycle management (installing, listing, and uninstalling plugins via CLI).

### Alternatives Considered

- **NuGet packages only:** Would work for compile-time references but not for runtime-only deployment. NuGet has no concept of plugin manifests, assembly scanning directives, or Surgewave-specific dependency constraints.
- **Shared directory with loose DLLs:** Simple but fragile. No metadata, no dependency management, no isolation between plugins that require different versions of the same library.
- **MEF/MAF composition:** .NET's Managed Extensibility Framework adds complexity and does not support the two-stage activation pattern (ConfigureServices before Build, Configure after Build) that ASP.NET Core requires.
- **gRPC sidecar plugins (like HashiCorp's go-plugin):** Provides strong isolation but adds network overhead and operational complexity for what should be in-process extensions.

## Decision

Introduce a four-level plugin interface hierarchy rooted in `IPlugin`, a `.swpkg` (Surgewave Plugin Package) ZIP-based distribution format, and a reflection-based `BrokerPluginActivator` for discovery and two-stage activation.

### Interface Hierarchy

All plugins implement `IPlugin`, which provides `FeatureId` and `DisplayName`. Specialized interfaces extend it:

- **`IBrokerPlugin`** --- enterprise features that register DI services and optionally map HTTP endpoints. Gated by `ILicenseProvider` and `SurgewaveFeatures.IsEnterpriseFeature()`.
- **`IProtocolPlugin`** --- protocol adapters (MQTT, AMQP, WebSocket, gRPC) that declare a `DefaultPort` and register their own middleware. Not license-gated (community features).
- **`IStorageEnginePlugin`** --- storage backends that expose a `StorageEngineName`, `SupportedModes`, and a `CreateFactory()` method returning `ILogSegmentFactory`. Loaded early before DI container build.
- **`IPipelineNode`** --- universal pipeline building blocks with `InputPorts`, `OutputPorts`, `Config`, and `Version`. Further specialized into `ISourceNode`, `IProcessorNode`, `ISinkNode`, and `ITriggerNode`.

### .swpkg Package Format

An `.swpkg` file is a ZIP archive containing:

- `manifest.json` --- a `PluginManifest` with id, name, version, description, authors, license, tags, icon, `minRuntimeVersion`, NuGet `dependencies`, Surgewave `surgewaveDependencies` (with semver constraints: `^`, `~`, `>=`, `>`, `<=`, `<`, `*`, exact), SHA-256 hash, and an `assemblies` array listing which DLLs to scan for `IPlugin` implementations.
- `lib/` --- plugin assemblies and their dependencies.
- Optional `icon.png` or `icon.svg`.

### Discovery and Activation

`BrokerPluginActivator.Discover<T>()` scans all loaded assemblies whose name starts with `Kuestenlogik.Surgewave.` for concrete, non-abstract types implementing `T`. Instances are created via `Activator.CreateInstance`. Activation follows a two-stage pattern:

1. **ConfigureServices** --- called during `WebApplicationBuilder` setup, before `app.Build()`. Plugins register their DI services.
2. **Configure** --- called after `app.Build()`, receiving the `WebApplication` host. Plugins map endpoints and middleware.

Enterprise plugins are checked against `SurgewaveFeatures.IsEnterpriseFeature()` and the optional `ILicenseProvider`. If no license is present, enterprise plugins are skipped with a warning.

### Assembly Isolation

`PluginLoader` creates a `PluginAssemblyLoadContext` per plugin assembly (collectible, so plugins can be unloaded). The custom load context uses `AssemblyDependencyResolver` to resolve plugin-local dependencies first, falling back to the default context for shared interfaces like `Kuestenlogik.Surgewave.Plugins`.

## Consequences

- **Zero compile-time enterprise dependencies** in the broker. `Program.cs` references only community packages.
- **Hot-installable plugins** via `surgewave plugin install path/to/plugin.swpkg`. The CLI extracts the archive, validates the manifest, and copies assemblies to the plugin directory.
- **Dependency isolation** prevents version conflicts between plugins (e.g., two plugins requiring different Newtonsoft.Json versions).
- **Collectible load contexts** allow plugin unloading, but in practice unload is best done via broker restart to avoid GC root leaks.
- **Reflection-based discovery** adds a small startup cost (assembly scanning), but runs once and results are cached for the broker lifetime.
- **The `IPlugin` base interface** is intentionally minimal (two string properties). All behavioral contracts live in the specialized interfaces, keeping the plugin API surface small and stable.
