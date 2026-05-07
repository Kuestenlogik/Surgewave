# Plugin-bundled default configuration

Surgewave plugins can ship recommended configuration defaults inside their `.swpkg`
package. The broker layers those defaults into its `IConfiguration` at the
**lowest** priority, so they take effect immediately after `surgewave plugin
install` — but the user's `appsettings.json` still wins on every key.

## Why

Without this, every plugin author has two unappealing options:

1. Hard-code defaults in C# property initialisers — invisible to operators
   until the plugin is loaded, undocumented, untweakable per-environment.
2. Ask every user to copy a config snippet from the plugin's README into their
   `appsettings.json` after install — error-prone, easy to forget, drifts as
   the plugin evolves.

A bundled `pluginsettings.json` is the third option: the plugin author writes
the recommended defaults once in JSON, ships them with the package, and
operators inherit them automatically — but can still override any value in
their own `appsettings.json` without merge conflicts.

## How it works

### Three configuration tiers

```
tier 1 (lowest)  ← pluginsettings.json from plugins/<id>/<file>     plugin author
tier 2           ← broker appsettings.json + appsettings.{Env}.json operator
tier 3 (highest) ← environment variables + command-line arguments   ops automation
```

The broker calls `builder.Configuration.AddPluginDefaults(pluginsDir)` early
in `Program.cs`, which inserts every installed plugin's `pluginsettings.json`
at the **front** of `builder.Configuration.Sources`. ASP.NET Core processes
sources in list order with later sources overriding earlier ones, so plugin
defaults end up at the lowest precedence.

The Connect Worker (`surgewave-connect`) uses the same call.

### Plugin author workflow

1. Add a `pluginsettings.json` next to your `plugin.json` in the project root:

   ```json
   {
     "Surgewave": {
       "MyPlugin": {
         "Enabled": false,
         "Port": 8080,
         "MaxConnections": 1000
       }
     }
   }
   ```

2. Either reference it explicitly in `plugin.json` (recommended for clarity)
   or rely on auto-detect — the packager picks up `pluginsettings.json` next
   to the manifest if it exists:

   ```json
   {
     "id": "my-org.my-plugin",
     "name": "My Plugin",
     "version": "1.0.0",
     "assemblies": [ "MyOrg.MyPlugin.dll" ],
     "pluginSettings": "pluginsettings.json"
   }
   ```

3. Add it to the `<Content>` items in your `.csproj` so `dotnet publish` copies
   it to the publish output (where the packager looks for it):

   ```xml
   <ItemGroup>
     <Content Include="plugin.json" CopyToOutputDirectory="PreserveNewest" CopyToPublishDirectory="PreserveNewest" />
     <Content Include="pluginsettings.json" Condition="Exists('pluginsettings.json')"
              CopyToOutputDirectory="PreserveNewest" CopyToPublishDirectory="PreserveNewest" />
   </ItemGroup>
   ```

4. Build the package:

   ```bash
   dotnet publish -c Release -p:SurgewavePackPlugin=true
   ```

   The resulting `.swpkg` archive contains both `plugin.json` and your
   `pluginsettings.json` at the root.

### Custom filenames

The default filename is `pluginsettings.json`, but the manifest's `pluginSettings`
field accepts any plain filename — for example, an MQTT plugin might prefer
`mqtt-defaults.json` for clarity:

```json
{
  "id": "kuestenlogik.surgewave.protocol.mqtt",
  "pluginSettings": "mqtt-defaults.json"
}
```

The packager bundles the file under its original name; install extracts it to
`plugins/<id>/mqtt-defaults.json`; the broker reads `plugin.json` to find the
filename and layers the file into `IConfiguration` exactly the same way.

Path separators (`/`, `\`, `..`) in the manifest field are rejected at pack
time as a defence-in-depth measure against archive escape.

### Operator workflow

Install a plugin:

```bash
surgewave plugin install kuestenlogik.surgewave.protocol.mqtt-0.1.0.swpkg
```

The `.swpkg` is extracted to `plugins/kuestenlogik.surgewave.protocol.mqtt/`, including the
bundled `pluginsettings.json`. On the next broker restart, the file is
layered into `IConfiguration`. No `appsettings.json` edit required to start
the adapter — usually only one explicit override is needed:

```json
{
  "Surgewave": {
    "Mqtt": { "Enabled": true }
  }
}
```

That value goes into `appsettings.json` (tier 2) and overrides the plugin
default (`Enabled: false` in tier 1). Every other field comes from the plugin
default.

### Inspect what's loaded

`surgewave config view` renders the merged effective configuration with optional
source attribution per leaf:

```bash
surgewave config view appsettings.json --explain
```

```
Effective configuration: appsettings.json
Layered with 1 plugin default(s).

Effective
└── Surgewave
    ├── Mqtt
    │   ├── Enabled = true [from appsettings.json]
    │   ├── Port = 1883 [from plugins/kuestenlogik.surgewave.protocol.mqtt/pluginsettings.json]
    │   ├── MaxClients = 1000 [from plugins/kuestenlogik.surgewave.protocol.mqtt/pluginsettings.json]
    │   └── ...
    └── BrokerId = 0 [from appsettings.json]

Sources:
  - appsettings.json
  - plugins/kuestenlogik.surgewave.protocol.mqtt/pluginsettings.json
```

Other useful commands:

```bash
# List installed plugins
surgewave plugin list

# Inspect manifest, assemblies, total size, bundled defaults
surgewave plugin show kuestenlogik.surgewave.protocol.mqtt

# Print just the plugin's bundled defaults (pipe-friendly)
surgewave plugin defaults kuestenlogik.surgewave.protocol.mqtt | jq .Surgewave.Mqtt

# Validate the merged config (broker + plugin defaults) against IValidatableConfig types
surgewave config validate appsettings.json
```

## Surgewave:PluginsDirectory

Both the broker and the Connect Worker honour `Surgewave:PluginsDirectory` from
`IConfiguration` (with `--plugin-path` as a CLI override on the worker).
Default: `./plugins` relative to the executable. Use this to point at a
shared plugins directory across multiple Surgewave services running on the same
host.

## What `pluginsettings.json` is not for

- **Required configuration values**. If a plugin needs a database connection
  string or an API key to function, that belongs in the broker's
  `appsettings.json` (tier 2) — there is no useful default to ship.
- **User overrides for other plugins**. The file is scoped to a single plugin's
  install directory. To override values across plugins, use the broker's own
  `appsettings.json`.
- **Hot reload for `IOptions<T>` consumers.** Plugin defaults files have
  `ReloadOnChange=true` enabled by default, so the underlying `IConfiguration`
  picks up file modifications immediately. Consumers using
  `IOptionsMonitor<T>` see the new values without a restart. Consumers using
  the legacy `IOptions<T>` snapshot still need a restart — that is a property
  of the consumer side, not the configuration source. Plugin authors who want
  hot-reloadable defaults should subscribe to
  `IOptionsMonitor<T>.OnChange(...)` in their service. To disable the watcher
  entirely (e.g. in tests or environments where filesystem watchers cause
  noise), pass `reloadOnChange: false` to `AddPluginDefaults`.

## Reference

- Implementation: `src/Kuestenlogik.Surgewave.Plugins.Packaging/PluginPackageManager.cs`
  (`PackAsync`, `InstallAsync`, `EnumerateInstalledPluginSettingsFiles`)
- Hosting integration: `src/Kuestenlogik.Surgewave.Plugins.Packaging.Hosting/PluginDefaultsConfigurationExtensions.cs`
- Manifest schema: [`schemas/plugin-manifest/v1.json`](../../schemas/plugin-manifest/v1.json)
- Architecture rationale: [`ARCHITECTURE.md`](../../ARCHITECTURE.md) §3 (`.Packaging` sibling)
- Tests: `tests/Kuestenlogik.Surgewave.Plugins.Tests/Packaging/PluginSettingsBundlingTests.cs`
