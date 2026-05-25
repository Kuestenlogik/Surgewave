# Contributing to Surgewave

Thank you for your interest in contributing to Surgewave!

## Contributor License Agreement (CLA)

Before we can accept your contribution, you must sign our Contributor License Agreement (CLA). This protects both you and the project.

**What this means:**
- Your contributions will be licensed under the same [Apache License 2.0](LICENSE) as the rest of the Surgewave core
- You retain copyright of your contributions
- You grant Küstenlogik the right to relicense your contribution in our premium-tier repositories (e.g. Surgewave.Ai, Surgewave.Governance) under the Business Source License 1.1 — this lets us keep building the commercial extensions that fund Surgewave development

**How it works:**
1. Open a Pull Request
2. The CLA Assistant bot will comment asking you to sign
3. Click the link and sign the CLA (one-time, covers all future contributions)
4. Once signed, the bot marks the PR as ready for review

If you have questions about the CLA, contact: licensing@kuestenlogik.com

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR-USERNAME/Surgewave.git`
3. Create a branch: `git checkout -b feature/my-feature`
4. Install .NET 10 SDK
5. Build: `dotnet build Kuestenlogik.Surgewave.slnx`
6. Test: `dotnet test Kuestenlogik.Surgewave.slnx`

## Development Workflow

### Branch Naming
- `feature/description` — New features
- `fix/description` — Bug fixes
- `docs/description` — Documentation
- `refactor/description` — Code improvements

### Commit Messages
Follow conventional commits:
- `feat: add new feature`
- `fix: resolve bug`
- `docs: update documentation`
- `refactor: improve code structure`
- `test: add tests`
- `chore: maintenance tasks`

### Pull Requests
1. Sign the CLA (first-time only)
2. Ensure all tests pass: `dotnet test Kuestenlogik.Surgewave.slnx`
3. Ensure zero build warnings: `dotnet build Kuestenlogik.Surgewave.slnx -c Release`
4. Add tests for new features
5. Update documentation if needed
6. Update ROADMAP.md if implementing a roadmap item

### Code Style
- Follow existing patterns in the codebase
- One class/interface per file, named by content
- Use `<summary>` XML docs on all public members
- Feature toggles: `Surgewave:FeatureName:Enabled` (default false)
- DI pattern: `AddSurgewaveXxx()` / `MapSurgewaveXxx()`

### Testing
- Unit tests with xUnit v3
- Integration tests in `tests/Kuestenlogik.Surgewave.IntegrationTests/`
- NSubstitute for mocking
- Test naming: `MethodName_Scenario_ExpectedResult`

### Running Coverage
```bash
./scripts/run-coverage.ps1
```

## Plugin Development

Surgewave has a plugin system for extending the broker. See the [Plugin Development Guide](docs/features/plugin-development.md) for creating your own plugins.

### Quick Start
1. Create a .NET 10 class library
2. Reference `Kuestenlogik.Surgewave.Plugins`
3. Implement a plugin interface (`IBrokerPlugin`, `IProtocolPlugin`, `IStorageEnginePlugin`, `ISourceNode`, `ISinkNode`, etc.)
4. Create `plugin.json` manifest in the project root (see *Plugin manifest* below)
5. Optionally create `pluginsettings.json` next to it for your recommended config defaults
6. Pack: `dotnet publish -c Release -p:SurgewavePackPlugin=true` (or `surgewave plugin pack --project .`)
7. Install: `surgewave plugin install <plugin-id>-<version>.swpkg`
8. Inspect: `surgewave plugin show <plugin-id>` and `surgewave config view appsettings.json --explain`

### Plugin manifest (`plugin.json`)

Every Surgewave plugin needs a `plugin.json` next to its source files. Minimal shape:

```json
{
  "id": "my-org.my-plugin",
  "name": "My Plugin",
  "version": "1.0.0",
  "description": "What this plugin does",
  "assemblies": [ "MyOrg.MyPlugin.dll" ],
  "tags": [ "category", "subcategory" ],
  "pluginSettings": "pluginsettings.json"
}
```

The `pluginSettings` field is optional (defaults to `pluginsettings.json` if a
file with that name exists next to the manifest). It tells the packager which
file to bundle as the plugin's default-config payload — any plain filename is
accepted (e.g. `mqtt-defaults.json`), but stick with `pluginsettings.json`
unless you have a strong reason.

### Plugin-bundled defaults (`pluginsettings.json`)

A plugin can ship recommended configuration defaults that the broker layers
beneath the user's `appsettings.json`. Three tiers, lowest precedence first:

```
tier 1 — pluginsettings.json from plugins/<id>/   ← plugin author
tier 2 — broker appsettings.json + appsettings.{Env}.json
tier 3 — environment variables + command-line args
```

User values always win, but plugin defaults take effect immediately after
`surgewave plugin install` — no manual config editing required. Example: an MQTT
plugin can ship recommended `Port`, `MaxClients` and `TopicPrefix` values; the
operator only needs to set `Enabled: true` to start the adapter.

For configuration POCOs, implement `IValidatableConfig` and add a public
`SectionName` constant so `surgewave config validate` can find the type via
reflection:

```csharp
public sealed class MyPluginConfig : IValidatableConfig
{
    public const string SectionName = "Surgewave:MyPlugin";

    [Range(1, 65535)]
    public int Port { get; set; } = 8080;

    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}
```

### Useful CLI commands during development

```bash
# Pack and install in one go
dotnet publish -c Release -p:SurgewavePackPlugin=true
surgewave plugin install artifacts/pub/packages/my-plugin-1.0.0.swpkg --force

# Inspect what was installed (manifest, assemblies, bundled defaults)
surgewave plugin show my-org.my-plugin

# Verify your plugin's config defaults merge correctly with the broker's appsettings
surgewave config view path/to/broker/appsettings.json --explain

# Validate the merged effective config
surgewave config validate path/to/broker/appsettings.json
```

## Project Structure

```
src/            — Source code
tests/          — Test projects
benchmarks/     — Performance benchmarks
docs/           — Documentation (DocFX)
deployments/    — Docker, Helm, K8s, installer, monitoring
scripts/        — Build/test scripts
```

## License

By contributing to Surgewave, you agree that your contributions will be licensed under the [Apache License 2.0](LICENSE) and may be relicensed by Küstenlogik in premium-tier repositories under the Business Source License 1.1 (see the CLA section above).

## Questions?

- Open a [GitHub Issue](https://github.com/Kuestenlogik/Surgewave/issues)
- Check the [documentation](docs/)
