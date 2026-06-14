# Sample: Storage Engine Plugin (Sample Memory)

The smallest possible `IStorageEnginePlugin`: wraps the built-in
[`MemoryLogSegmentFactory`](../../src/Kuestenlogik.Surgewave.Storage.Engine.Memory/MemoryLogSegmentFactory.cs)
under the engine name `sample-memory`. ~30 lines of code.

## Activating the engine

Set the engine in the broker config (`appsettings.json` or env):

```json
{
  "Surgewave": {
    "Storage": {
      "Engine": "sample-memory"
    }
  }
}
```

When the broker starts it looks up the plugin whose
`StorageEngineName` matches `Surgewave:Storage:Engine` and calls
`CreateFactory(...)` to get the `ILogSegmentFactory` it uses for all
partitions.

## Why an alias instead of using `Memory` directly?

The built-in `Memory` engine is registered by the
`Storage.Engine.Memory` assembly directly. This sample registers
*another* alias (`sample-memory`) to demonstrate how a third-party
plugin would expose a new engine name — operators don't have to know
the plugin author's class layout, they only know the engine-name
string they put in config.

## Files

| File | What it does |
|---|---|
| `plugin.json` | v1 manifest; id `kuestenlogik.surgewave.samples.storage-engine`. |
| `SampleMemoryStorageEnginePlugin.cs` | The `IStorageEnginePlugin` entry point — returns a fresh `MemoryLogSegmentFactory`. |
| `tests/SampleMemoryStorageEnginePluginTests.cs` | Asserts engine name + that `CreateFactory` returns a usable factory. |
