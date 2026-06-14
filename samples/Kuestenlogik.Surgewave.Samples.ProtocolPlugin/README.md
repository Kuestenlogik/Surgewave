# Sample: Protocol Plugin (Echo)

A minimal `IProtocolPlugin` that declares it shares the HTTP port
(`DefaultPort = 0`) and documents — via inline comment — how to map
a `/echo` endpoint onto the broker's existing Kestrel pipeline.

## Why no `Microsoft.AspNetCore.App` reference?

A real protocol plugin that maps endpoints needs to cast the `host`
parameter to `WebApplication` and call `MapPost(...)`. That requires
adding `<FrameworkReference Include="Microsoft.AspNetCore.App" />`
to the csproj. This sample stays at the contract surface so the
project file remains a clean "plugin in 12 lines of csproj" reference.
The endpoint-mapping snippet lives in the
[`EchoProtocolPlugin.Configure`](EchoProtocolPlugin.cs) comment for
copy-paste.

## Files

| File | What it does |
|---|---|
| `plugin.json` | v1 manifest; id `kuestenlogik.surgewave.samples.protocol-plugin`. |
| `EchoProtocolPlugin.cs` | The `IProtocolPlugin` entry point. `DefaultPort = 0` means "share HTTP host". |
| `tests/EchoProtocolPluginTests.cs` | Verifies DefaultPort, IsConfigEnabled default, and the SRWV001/SRWV004 invariants. |
