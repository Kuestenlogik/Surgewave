* This Project aims to be a drop-in replacement for Kafka that has less hurdles to bring it up and running. Much
  like the idea of the Redpanda alternative. But this project is using modern .net 10 and C#14 instead.
* The implementation need to be super-fast and beat the performance of Kafka in all categories (throughput, producer Performance, consumer Performance, P50/P90/P99+ etc.). Try to be competetive with Aeron (https://github.com/aeron-io/aeron) regarding transportation throughput and latency. Optimize agressively using Zero copy, Span<T> and Memory<T> as well as pooling and thread channels.
* Structure the codebase so that it is easy to understand and extensible. Use design pattern and single responsiblities per class. Try to have only one class/struct/record/interface per file. Name the file according to Content, e.g. by class name.
* Update the roadmap (ROADMAP.md) after implementing new Features
* Run tests with verbose output show progress and failures
* Run benchmarks when changing speed sensitive implementations.

## Plugin System

* Enterprise features are separate repos, packaged as `.swpkg` (Surgewave Plugin Package) files
* Install plugins via CLI: `surgewave plugin install path/to/plugin.swpkg`
* List installed plugins: `surgewave plugin list`
* All plugins are discovered at runtime via `IBrokerPlugin`, `IProtocolPlugin`, `IStorageEnginePlugin`
* Community plugins (Connectors, AI Nodes) live in their own repos
* Enterprise plugins (Governance, Storage, Replication, etc.) are sold separately

## Connector Plugins

* All connectors live in `Surgewave.Connectors` repo (`C:\Projekte\KL\Surgewave.Connectors`)
* Build connectors: `cd ..\Surgewave.Connectors && dotnet build -c Release`
* Package connectors: `cd ..\Surgewave.Connectors && .\scripts\collect-plugins.ps1 -Build`
* Install: `surgewave plugin install path/to/connector.swpkg`

## AI Node Plugins

* AI pipeline nodes live in `Surgewave.AI` repo (`C:\Projekte\KL\Surgewave.Ai`)
* Build: `cd ..\Surgewave.Ai && dotnet build -c Release`
* Package: `cd ..\Surgewave.Ai && .\scripts\collect-plugins.ps1 -Build`
* Install: `surgewave plugin install path/to/ai-nodes.swpkg`

## Starting Surgewave

* Broker: `dotnet run --project src/Kuestenlogik.Surgewave.Broker`
* Control UI (port 5050): `dotnet run --project src/Kuestenlogik.Surgewave.Control --urls "http://localhost:5050"`
* With Connect: `dotnet run --project src/Kuestenlogik.Surgewave.Broker -- --Surgewave:Connect:Enabled=true`
* All-in-one: `.\scripts\start-all.ps1`
