# Surgewave Architecture Patterns

This document describes the architectural conventions that Surgewave follows across its ~90
projects. When in doubt, new code should fit one of the patterns below; new patterns
should be documented here before being adopted widely.

The goal of these patterns is to keep Surgewave's core libraries **slim and focused** so
that thin consumers (CLI tooling, tests, samples, third-party plugin authors) don't
inherit ASP.NET Core, plugin-loader runtimes or other infrastructure they don't need.

---

## Table of contents

1. [`.Abstractions` — contract extraction](#1-abstractions--contract-extraction)
2. [`.Hosting` — ASP.NET Core host integration](#2-hosting--aspnet-core-host-integration)
3. [`.Packaging` — pure tooling sibling](#3-packaging--pure-tooling-sibling)
4. [`IValidatableConfig` + `ConfigValidator` — configuration validation](#4-ivalidatableconfig--configvalidator--configuration-validation)
5. [Opt-in feature hooks via `WithX()` extension methods](#5-opt-in-feature-hooks-via-withx-extension-methods)
6. [Quick reference](#quick-reference)

---

## 1. `.Abstractions` — contract extraction

**When to use:** A core library has both heavy runtime implementations and lightweight
contract types (DTOs, interfaces, pure client helpers). Consumers that only need the
contracts should not be forced to load the runtime.

**How it works:**

- Core library (`Foo`) keeps the runtime: server code, heavy dependencies, stateful
  services.
- Sibling assembly `Foo.Abstractions` holds the contract types: DTOs, interfaces,
  pure client-side helpers. Depends on the BCL and `Microsoft.Extensions.*.Abstractions`
  — nothing heavier.
- Namespace stays the same: `Foo.Abstractions` is the **assembly name**, but types still
  live in the `Foo` namespace (or a sub-namespace of it). This matches the Microsoft
  convention (`Microsoft.Extensions.Logging.Abstractions` contains
  `Microsoft.Extensions.Logging.ILogger`).
- The runtime library `Foo` references `Foo.Abstractions` so it can use the contract
  types; consumers that only need contracts reference `Foo.Abstractions` alone.

**Example in Surgewave:** `Kuestenlogik.Surgewave.Connect.Abstractions`
(`src/Kuestenlogik.Surgewave.Connect.Abstractions/`)

Before the extraction, the `Kuestenlogik.Surgewave.Tool` CLI tool pulled ASP.NET Core transitively
because it referenced `Kuestenlogik.Surgewave.Connect` for a handful of pipeline template types.
The templates themselves had zero runtime dependencies — they were just DTOs plus
a `PipelineTemplateManager` with in-memory + HTTP repository clients.

The extraction moved ~15 files (template types, definition DTOs, repository interfaces
and the template manager) into `Kuestenlogik.Surgewave.Connect.Abstractions`. The CLI now references
only the abstractions assembly; `Kuestenlogik.Surgewave.Connect` still uses those same types for its
runtime pipeline orchestrator. CLI container image shrank from 108 MB (`aspnet:10.0`
base) to 94 MB (`runtime:10.0` base).

---

## 2. `.Hosting` — ASP.NET Core host integration

**When to use:** A library needs a minimal amount of code that plugs into an ASP.NET
Core host (`IEndpointRouteBuilder`, `IServiceCollection`, `IHostedService`), but the
library itself is otherwise host-agnostic. The ASP.NET Core dependency should be
**opt-in**, not mandatory.

**How it works:**

- Core library (`Foo`) contains the functionality but no ASP.NET Core references.
- Sibling assembly `Foo.Hosting` has `FrameworkReference Microsoft.AspNetCore.App`
  and holds the endpoint maps, DI extensions and `IHostedService` registrations.
- A host application (the broker, a connector worker, a custom app) references both:
  `Foo` for the functionality, `Foo.Hosting` to wire it into the request pipeline.
- Test projects, the CLI and other non-web consumers reference only `Foo` and pay
  zero ASP.NET Core cost.

Both Microsoft Extensions (`Microsoft.Extensions.Logging.Abstractions` →
`Microsoft.Extensions.Hosting`) and many community libraries follow this split.

**Example in Surgewave:** `Kuestenlogik.Surgewave.Streams.InteractiveQueries.Hosting`
(`src/Kuestenlogik.Surgewave.Streams.InteractiveQueries.Hosting/`)

Surgewave Streams supports Interactive Queries — HTTP endpoints that expose state stores
for ad-hoc querying. Originally this lived inside `Kuestenlogik.Surgewave.Streams`, which meant
every consumer of Streams (including the `Kuestenlogik.Surgewave.Streams.Linq` LINQ provider and
the Streams unit tests) pulled `Microsoft.AspNetCore.App` via framework reference.

The extraction split the code into three layers:

| Assembly | Contents | AspNet dep |
|---|---|---|
| `Kuestenlogik.Surgewave.Streams` (core) | DSL, operators, state stores, runtime | ❌ |
| `Kuestenlogik.Surgewave.Streams.InteractiveQueries` | Pure query surface: registry, DTOs, `RemoteQueryClient` (TCP), wrappers | ❌ |
| `Kuestenlogik.Surgewave.Streams.InteractiveQueries.Hosting` | Two files: `InteractiveQueryRestApi` + `StreamsIQExtensions` (DI registration) | ✅ |

Streams, Streams.Linq and test projects now build without AspNet; only the broker
(which legitimately hosts the REST endpoints) references the `.Hosting` assembly.

Five Hosting extractions landed in the same refactoring wave: `Connect.Hosting`,
`Schema.Registry.Hosting`, `Cdc.Hosting`, `Wasm.Hosting`, and
`Streams.InteractiveQueries.Hosting`.

---

## 3. `.Packaging` — pure tooling sibling

**When to use:** A library defines runtime behaviour *and* a set of tooling types
(manifests, package formats, orchestrators) that are conceptually separate and
reference-free. The tooling types should be available standalone so that build
tools, marketplaces and inspection utilities can use them without the runtime.

**How it works:**

- Core library (`Foo`) keeps the runtime: interface definitions, discovery,
  assembly loading.
- Sibling assembly `Foo.Packaging` holds the tooling: manifest DTOs, package
  managers, dependency resolvers, validation helpers. Depends only on the BCL
  (and maybe `System.IO.Compression` / `System.Text.Json`).
- The core references the packaging assembly where it needs the tooling types
  (e.g. to deserialise a manifest at discovery time).
- Build tasks (`Kuestenlogik.Surgewave.Build`) and third-party plugin authoring tools reference
  only `Foo.Packaging`, not `Foo` — they don't need the runtime plugin loader.

**Example in Surgewave:** `Kuestenlogik.Surgewave.Plugins.Packaging`
(`src/Kuestenlogik.Surgewave.Plugins.Packaging/`)

Originally, the plugin packaging code (`PluginManifest`, `PluginPackageManager`,
`DependencyResolver`, checksum calculator, install result types) lived inside
`Kuestenlogik.Surgewave.Plugins` as a sub-namespace. But `Kuestenlogik.Surgewave.Plugins.Repository` —
the marketplace/NuGet client — was already a *separate* sibling assembly. Same
naming pattern, two different things: one was internal, the other external.

The extraction moved the nine files into their own assembly, producing a
consistent three-way layout:

```
Kuestenlogik.Surgewave.Plugins              interfaces + plugin runtime loader
Kuestenlogik.Surgewave.Plugins.Packaging    .swpkg manifests, package manager, dependency resolver
Kuestenlogik.Surgewave.Plugins.Repository   marketplace/NuGet client
```

All three are now sibling assemblies of `Kuestenlogik.Surgewave.Plugins`, matching the
Microsoft.Extensions.* convention. `Kuestenlogik.Surgewave.Build` (MSBuild tasks for
`dotnet publish -p:SurgewavePackPlugin=true`) now references only
`Kuestenlogik.Surgewave.Plugins.Packaging`, cutting its dependency graph to the minimum.

### Plugin-bundled default configuration (`pluginsettings.json`)

A plugin can ship a `pluginsettings.json` next to its `plugin.json` manifest. The
packager auto-detects the file (or honours an explicit `pluginSettings` field on the
manifest — any plain filename is allowed, e.g. `mqtt-defaults.json`), bundles it at
the root of the `.swpkg` archive under its original name, and
`PluginPackageManager.InstallAsync` extracts it to `plugins/<plugin-id>/<filename>`
next to the plugin's DLLs.

At broker startup the file is layered into `IConfiguration` as the **lowest-priority**
source — so the effective config has three tiers:

```
tier 1 (lowest)  — plugin defaults from plugins/<id>/<filename>   (plugin-shipped)
tier 2           — broker appsettings.json + appsettings.{Env}.json
tier 3 (highest) — environment variables + command-line args
```

User values in `appsettings.json` always win, but plugin authors can ship recommended
defaults that take effect immediately after `surgewave plugin install` — no manual config
editing required. The discovery side (`PluginPackageManager.EnumerateInstalledPluginSettingsFiles`)
reads each plugin's `plugin.json` to find the declared settings filename, so the
manifest is the single source of truth and plugins can pick any filename without the
broker needing to glob. `surgewave config validate` calls the same helper, so what the
broker sees at startup is what the validator checks
(`src/Kuestenlogik.Surgewave/Commands/Config/ConfigValidateCommand.cs`).

The plugin assembly is also auto-discovered by `surgewave config validate` from
`plugins/<id>/`, so a single `--assemblies` argument pointing at the broker's bin
covers both core types and plugin types without DLL-shuffling.

---

## 4. `IValidatableConfig` + `ConfigValidator` — configuration validation

**When to use:** Any new configuration class with more than one non-trivial constraint.
Surgewave has ~90 such classes, and ad-hoc validation scattered across constructors or
use sites quickly becomes inconsistent.

**How it works:**

Two pieces in `Kuestenlogik.Surgewave.Core.Configuration`:

- **`IValidatableConfig`** — interface with one method, `IReadOnlyList<string> Validate()`.
  Returns error messages instead of throwing, so callers can choose to log, surface
  in a UI, or fail-fast. Implementations typically run declarative checks first and
  then add cross-property rules.
- **`ConfigValidator`** — static helper with `ValidateDataAnnotations(object)` (runs
  the standard `[Required]` / `[Range]` / `[RegularExpression]` / `[Url]` / `[MinLength]`
  attributes), `ThrowIfInvalid(IValidatableConfig)` (fail-fast wrapper), and a
  `ConfigValidationException` that aggregates all error messages for startup diagnostics.

**Pattern for a config class:**

```csharp
using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

public sealed class FooConfig : IValidatableConfig
{
    [Required]
    [MinLength(1)]
    public required string ApplicationId { get; init; }

    [Range(1, 65535)]
    public int Port { get; init; } = 9092;

    [Range(0.0, 1.0)]
    public double SamplingRatio { get; init; } = 1.0;

    public bool ExactlyOnce { get; init; }
    public bool EnableIdempotence { get; init; } = true;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        // Cross-property rules that attributes can't express
        if (ExactlyOnce && !EnableIdempotence)
            errors.Add($"{nameof(EnableIdempotence)}: must be true when {nameof(ExactlyOnce)} is set.");

        return errors;
    }
}
```

For configs with no cross-property rules, the simpler form works:

```csharp
public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
```

**Why both DataAnnotations *and* `IValidatableConfig`?**

Surgewave configs come in two flavours:

1. **IOptions-loaded configs** (from appsettings.json via DI) — `IValidateOptions<T>`
   from ASP.NET Core is the idiomatic entry point. An `IValidateOptions<FooConfig>`
   implementation can delegate to `config.Validate()`.
2. **Directly instantiated configs** (`new FooConfig { ... }` in samples, tests,
   programmatic setups) — `IValidateOptions` never runs in this case. `config.Validate()`
   gives callers an explicit way to check the same rules.

One `Validate()` implementation covers both worlds.

**Example in Surgewave:** `src/Kuestenlogik.Surgewave.Streams/StreamsConfig.cs`

`StreamsConfig` has DataAnnotations on its numeric properties (`NumStreamThreads`,
`CommitIntervalMs`, `TransactionTimeoutMs`, etc.), a `RegularExpression` on
`AutoOffsetReset` ("earliest" or "latest"), and two cross-property checks in
`Validate()`: exactly-once requires idempotence, and `PollTimeout` / `ShutdownTimeout`
must be positive. Tests: `tests/Kuestenlogik.Surgewave.Streams.Tests/StreamsConfigValidationTests.cs`.

43 config classes across 25 projects follow this pattern today.

**Operational tooling:** the same `IValidatableConfig` contract powers a CLI command:

```bash
surgewave config validate path/to/appsettings.json --assemblies path/to/surgewave/bin
```

`surgewave config validate` discovers every concrete `IValidatableConfig` type in the
Kuestenlogik.Surgewave assemblies under `--assemblies` (default: directory of the config file +
the CLI's own bin), reads the matching `SectionName` constant on each type, binds
the JSON subtree onto a fresh instance, and reports per-section pass/fail with
human-readable error messages. Use `-v` to see passing sections too. Exit code is
`1` when at least one section fails. Implementation:
`src/Kuestenlogik.Surgewave/Commands/Config/ConfigValidateCommand.cs`.

---

## 5. Opt-in feature hooks via `WithX()` extension methods

**When to use:** A feature is optional, depends on additional infrastructure (sockets,
host integration, external services), and should not be wired into the core runtime
by default. The runtime should expose a clean abstraction; the feature should be
activated by an explicit builder call, not by a config property.

**How it works:**

- Core runtime (`Foo`) defines an interface `IFeatureHook` (or similar) that captures
  everything the feature needs to talk to.
- Core runtime holds an optional nullable field `IFeatureHook?` and delegates all
  feature-related behaviour through the interface.
- The feature's default implementation lives in a sibling assembly (typically a
  `.Hosting` or `.Abstractions` assembly from patterns 1 or 2).
- The sibling assembly provides an extension method `WithFeature(this Foo, ...)` that
  constructs the implementation and registers it with the core runtime.
- Users opt in with one line:

```csharp
var app = new StreamsApplication(config, topology);
app.WithInteractiveQueries(new HostInfo("localhost", 9000));
app.Start();
```

**What this replaces:** Implicit activation through config properties:

```csharp
// Anti-pattern — what we removed:
var config = new StreamsConfig
{
    ApplicationId = "app",
    BootstrapServers = "localhost:9092",
    ApplicationServer = new HostInfo("localhost", 9000)  // silently activates IQ
};
```

Problems with the implicit form: feature activation is invisible at the call site,
the core runtime has a hard dependency on the feature's types, cross-feature coupling
becomes tangled, tests can't mock the feature cleanly.

**Example in Surgewave:** `IPeerQueryProvider` + `WithInteractiveQueries(HostInfo)`
(`src/Kuestenlogik.Surgewave.Streams/IPeerQueryProvider.cs` + extensions in
`src/Kuestenlogik.Surgewave.Streams.InteractiveQueries/StreamsBuilderInteractiveQueriesExtensions.cs`)

Streams' Interactive Queries infrastructure (peer metadata tracking, TCP
`RemoteQueryServer`, `RemoteQueryClient`) used to be implemented directly inside
`StreamsApplication` with `if (_config.ApplicationServer.HasValue)` branches. It
dragged concrete types (`StreamsMetadataState`, `RemoteQueryServer`, `RemoteQueryClient`)
into the Streams core, blocking the ASP.NET Core cleanup and making tests brittle.

The refactoring:

1. Introduced `IPeerQueryProvider` in `Kuestenlogik.Surgewave.Streams` — lifecycle hooks
   (`Start(context)`, `DisposeAsync`), metadata queries (`AllMetadata`,
   `AllMetadataForStore`, `FindByKey`), peer registration (`RegisterPeerAsync`)
   and a `LocalHost` accessor.
2. Implemented `PeerQueryProvider : IPeerQueryProvider` in
   `Kuestenlogik.Surgewave.Streams.InteractiveQueries` wrapping the concrete TCP machinery.
3. Added `StreamsBuilderInteractiveQueriesExtensions.WithInteractiveQueries(this app, HostInfo)`
   as the opt-in entry point.
4. Removed the direct fields from `StreamsApplication`, removed
   `StreamsConfig.ApplicationServer`, made the old public IQ methods into extension
   methods in the IQ assembly.

Tests can now plug in a `FakePeerQueryProvider` with zero sockets
(see `tests/Kuestenlogik.Surgewave.Streams.Tests/PeerQueryProviderFakeTests.cs`), and alternative
transports (gRPC, in-memory, mesh networks) can ship as drop-in replacements.

---

## Quick reference

| Pattern | File/assembly suffix | Use when |
|---|---|---|
| Contract extraction | `.Abstractions` | Split pure DTOs / interfaces from a heavy runtime library |
| ASP.NET host integration | `.Hosting` | Isolate the AspNetCore dependency so core code can run aspnet-free |
| Pure tooling sibling | `.Packaging` (or task-specific) | Standalone tooling types (manifests, builders, validators) without runtime |
| Config validation | `IValidatableConfig` + `ConfigValidator` | Any config class with ≥ 2 non-trivial constraints |
| Opt-in feature | `IFeatureHook` + `WithFeature()` extension | Feature depends on infrastructure and should be activated explicitly, not by config property |

All five patterns were established during a refactoring wave documented in commits
`f36e143`, `3db4911` and `891742e`. They replaced ad-hoc mixing of runtime, contracts
and host integration in several large libraries, and removed an implicit-config-driven
feature activation from Streams. New code should prefer them over the older approaches.
