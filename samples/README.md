# Surgewave Plugin Samples

Reference implementations for the three Surgewave plugin shapes
(broker, protocol, storage engine). Each sample is a complete,
buildable, single-file plugin with a passing test and a per-sample
README that walks through every authoring decision.

Use these as templates when you want to copy a working starting
point. For scaffolding from scratch use the
[`dotnet new` templates](../templates/Kuestenlogik.Surgewave.Templates/)
instead — same skeleton, fewer keystrokes.

## Index

| Sample | Plugin shape | What it does |
|---|---|---|
| [Samples.BrokerPlugin](Kuestenlogik.Surgewave.Samples.BrokerPlugin/) | `IBrokerPlugin` | Registers a request-counter hosted service that increments on every Produce/Fetch the broker handles. |
| [Samples.ProtocolPlugin](Kuestenlogik.Surgewave.Samples.ProtocolPlugin/) | `IProtocolPlugin` | Adds an `/echo` HTTP endpoint that returns its request body unchanged. Demonstrates the HTTP-host-sharing path (`DefaultPort=0`). |
| [Samples.StorageEngine](Kuestenlogik.Surgewave.Samples.StorageEngine/) | `IStorageEnginePlugin` | Wraps the built-in `Memory` log-segment factory under the engine name `"sample-memory"`. Smallest possible storage engine. |

## Common patterns

Every sample:

1. **`plugin.json` next to the csproj** — picked up by Plugin SDK B
   (`ValidatePluginManifestTask`) at build time so a malformed manifest
   fails the build before the `.swpkg` is packed.
2. **`Kuestenlogik.Surgewave.Build` package reference** with
   `PrivateAssets="all"` — pulls the validation + pack targets without
   leaking the dev-tooling into the published assembly.
3. **`sealed` plugin class** — satisfies the
   [SRWV001 analyzer rule](../src/Kuestenlogik.Surgewave.Analyzers/PluginShouldBeSealedAnalyzer.cs)
   from Plugin SDK E.
4. **Parameterless constructor** — required by `BrokerPluginActivator`
   (SRWV004).
5. **`ILogger<T>` for any output** — `Console.WriteLine` would trip
   SRWV010.
6. **One xUnit test** that constructs the plugin and asserts the
   contract members behave as documented. Run with
   `dotnet test samples/<sample>/tests/`.

## Packing a sample as `.swpkg`

```bash
cd samples/Kuestenlogik.Surgewave.Samples.BrokerPlugin
dotnet publish -c Release -p:SurgewavePackPlugin=true
```

The packed `.swpkg` lands under `pluginPackage/` (or
`artifacts/pub/packages/` when the project uses the .NET artifacts
output layout). Install into a broker via:

```bash
surgewave plugins install ./pluginPackage/Kuestenlogik.Surgewave.Samples.BrokerPlugin-0.1.0.swpkg
```
