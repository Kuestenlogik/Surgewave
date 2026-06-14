# Sample: Broker Plugin (Request Counter)

A minimal `IBrokerPlugin` that registers a singleton
[`RequestCounter`](RequestCounter.cs) the broker can inject anywhere
in the DI container. ~50 lines of code total.

## Files

| File | What it does |
|---|---|
| `plugin.json` | v1 manifest; id `kuestenlogik.surgewave.samples.broker-plugin`. |
| `RequestCounter.cs` | The service the plugin exposes — a thread-safe `long` counter. |
| `RequestCounterBrokerPlugin.cs` | The `IBrokerPlugin` entry point. Sealed (SRWV001), parameterless ctor (SRWV004), uses `ILogger<T>`-friendly idioms (SRWV010). |
| `tests/RequestCounterBrokerPluginTests.cs` | Asserts the plugin is sealed, has a parameterless ctor, and registers `RequestCounter` as a singleton. |

## Walkthrough

### `IsConfigEnabled`

```csharp
public bool IsConfigEnabled(IConfiguration configuration) =>
    configuration.GetValue<bool>("SampleBrokerPlugin:Enabled", defaultValue: true);
```

Reads `SampleBrokerPlugin:Enabled` with a default of `true` so the
plugin loads in dev environments that don't ship a config file.
Operators flip it off by setting `SampleBrokerPlugin__Enabled=false`
as an env var (the standard double-underscore mapping).

### `ConfigureServices`

```csharp
public void ConfigureServices(IServiceCollection services, IConfiguration configuration)
{
    services.AddSingleton<RequestCounter>();
}
```

The whole point of the plugin in one line. A real plugin would also
register hosted services (`AddHostedService<MyService>()`), options
(`Configure<MyOptions>(configuration.GetSection("…"))`), and any
middlewares it needs.

### `Configure` (not overridden)

We rely on the default no-op from `IBrokerPlugin`. The override is
the place to map HTTP endpoints — the `host` parameter is the
`WebApplication`, so callers `((WebApplication)host).MapGet("/x", …)`.

## Pack + install

```bash
dotnet publish -c Release -p:SurgewavePackPlugin=true
surgewave plugins install ./pluginPackage/Kuestenlogik.Surgewave.Samples.BrokerPlugin-0.1.0.swpkg
```
