# Build your first Surgewave plugin

This guide walks you through creating, packaging, installing and validating
a Surgewave plugin from scratch. By the end you will have:

- A .NET class library that implements `IBrokerPlugin`
- A `plugin.json` manifest with a `pluginsettings.json` for default config
- A `.swpkg` package built via `dotnet publish`
- The plugin installed in a local broker and verified via `surgewave config validate`

**Prerequisites:** .NET 10 SDK, a built Surgewave broker (or the published artifacts).

## 1. Create the project

```bash
mkdir MySurgewavePlugin && cd MySurgewavePlugin
dotnet new classlib -n MySurgewavePlugin --framework net10.0
cd MySurgewavePlugin
```

Add the Surgewave plugin reference:

```xml
<!-- MySurgewavePlugin.csproj -->
<ItemGroup>
  <PackageReference Include="Kuestenlogik.Surgewave.Plugins" Version="0.1.0" />
  <PackageReference Include="Kuestenlogik.Surgewave.Build" PrivateAssets="all" />
</ItemGroup>

<ItemGroup>
  <Content Include="plugin.json" CopyToOutputDirectory="PreserveNewest" CopyToPublishDirectory="PreserveNewest" />
  <Content Include="pluginsettings.json" Condition="Exists('pluginsettings.json')"
           CopyToOutputDirectory="PreserveNewest" CopyToPublishDirectory="PreserveNewest" />
</ItemGroup>
```

## 2. Implement the plugin

Replace `Class1.cs` with your plugin:

```csharp
// MyPlugin.cs
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

public sealed class MyPlugin : IBrokerPlugin
{
    public string FeatureId => "MyOrg.MyPlugin";
    public string DisplayName => "My First Plugin";

    public bool IsConfigEnabled(IConfiguration configuration)
        => configuration.GetValue<bool>("Surgewave:MyPlugin:Enabled", false);

    public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
    {
        // Register your services here. They'll be available via DI in the broker.
    }

    public void Configure(object host, IServiceProvider services)
    {
        var logger = services.GetRequiredService<ILogger<MyPlugin>>();
        logger.LogInformation("My First Plugin is alive!");
    }
}
```

## 3. Create the manifest

Create `plugin.json` in the project root:

```json
{
  "$schema": "https://surgewave.kuest.nlogik.com/schemas/plugin-manifest/v1",
  "id": "my-org.my-plugin",
  "name": "My First Plugin",
  "version": "1.0.0",
  "description": "A hello-world Surgewave broker plugin.",
  "assemblies": [ "MySurgewavePlugin.dll" ],
  "pluginSettings": "pluginsettings.json",
  "tags": [ "example", "hello-world" ]
}
```

## 4. Ship default configuration

Create `pluginsettings.json` next to the manifest:

```json
{
  "Surgewave": {
    "MyPlugin": {
      "Enabled": false,
      "Greeting": "Hello from my plugin!"
    }
  }
}
```

This file travels inside the `.swpkg` package. When installed, the broker layers
it beneath `appsettings.json` — so your defaults take effect immediately, but
the operator can override any value.

## 5. Build the package

```bash
dotnet publish -c Release -p:SurgewavePackPlugin=true
```

This produces a `.swpkg` file (a ZIP with your DLLs + manifest + defaults) in
the output directory.

## 6. Install the plugin

```bash
surgewave plugin install MySurgewavePlugin-1.0.0.swpkg -d ./plugins --force
```

## 7. Inspect what was installed

```bash
# Full details: manifest, assemblies, bundled defaults
surgewave plugin show my-org.my-plugin -d ./plugins

# Just the defaults (pipe-friendly)
surgewave plugin defaults my-org.my-plugin -d ./plugins

# Generate a copy-paste-ready config section with Enabled=true
surgewave config init --plugin my-org.my-plugin -d ./plugins
```

## 8. Validate the configuration

```bash
# Check the broker's effective config (user appsettings + plugin defaults)
surgewave config validate path/to/appsettings.json --assemblies ./plugins/..

# See where each value comes from
surgewave config view path/to/appsettings.json --explain

# Live validation against the running broker
curl https://localhost:9093/api/config/validate | jq
```

## 9. Enable and restart the broker

Add to your broker's `appsettings.json`:

```json
{
  "Surgewave": {
    "MyPlugin": { "Enabled": true }
  }
}
```

Restart the broker. You should see:

```
info: Activated broker plugin: My First Plugin (feature: MyOrg.MyPlugin)
info: My First Plugin is alive!
```

## 10. Upgrade workflow

When you publish a new version:

```bash
# Build the new .swpkg
dotnet publish -c Release -p:SurgewavePackPlugin=true

# Preview what changed vs. the installed version
surgewave plugin diff my-org.my-plugin path/to/new-package.swpkg -d ./plugins

# Install the upgrade (--force overwrites the old version)
surgewave plugin install new-package.swpkg -d ./plugins --force

# Optionally validate immediately
surgewave plugin install new-package.swpkg -d ./plugins --force --validate-config path/to/appsettings.json
```

## What's next

- Add a config POCO with `IValidatableConfig` and `[Range]`/`[Required]` annotations
  so `surgewave config validate` catches misconfigurations before the broker restarts
- Add `ConfigureAsync()` if your plugin needs async lifecycle (network connections,
  background services)
- Implement `IProtocolPlugin` instead of `IBrokerPlugin` if your plugin provides a
  new wire protocol (TCP listener on a separate port)
- Ship the plugin via the Surgewave Marketplace (`surgewave plugin publish`)

## Reference

- [Plugin manifest schema](../../schemas/plugin-manifest/v1.json)
- [Plugin defaults model](../features/plugin-defaults.md)
- [CONTRIBUTING.md Plugin Development section](../../CONTRIBUTING.md)
- [Protocol plugin documentation](../features/plugins/index.md)
