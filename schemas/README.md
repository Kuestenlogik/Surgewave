# Surgewave JSON Schemas

Public JSON Schema files used by Surgewave tooling and plugin authors. Each schema
has a stable URL and a stable file path; new versions live next to the old
ones (`v1.json`, `v2.json`, ...) so plugin manifests pinning a specific version
keep working.

## Available schemas

| Schema | URL | File |
|---|---|---|
| Plugin manifest v1 | `https://surgewave.kuest.nlogik.com/schemas/plugin-manifest/v1` | [`plugin-manifest/v1.json`](plugin-manifest/v1.json) |

## Plugin manifest

The schema for `plugin.json` — the metadata file at the root of every Surgewave
Plugin Package (`.swpkg`). Plugin authors should reference it via the
`$schema` field at the top of their manifest:

```json
{
  "$schema": "https://surgewave.kuest.nlogik.com/schemas/plugin-manifest/v1",
  "id": "my-org.my-plugin",
  "name": "My Plugin",
  "version": "1.0.0",
  "assemblies": [ "MyOrg.MyPlugin.dll" ]
}
```

VS Code, Rider, and other JSON-Schema-aware editors will then auto-complete
the available fields, surface inline documentation, and flag invalid values
(e.g. an `id` with spaces, a `version` that does not match SemVer, or a
`pluginSettings` that contains a path separator).

The schema is the **source of truth** for what fields are accepted by the
Surgewave packager and the broker's plugin discovery layer. Adding a new manifest
field requires updating both the C# `PluginManifest` class
(`src/Kuestenlogik.Surgewave.Plugins.Packaging/PluginManifest.cs`) **and** this schema in
the same change.
