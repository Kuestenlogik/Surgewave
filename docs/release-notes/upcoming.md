---
title: Plugins move out of the install directory
version: 0.5.1
---

0.5.1 settles where a plugin comes from and where it lives. The Bowire workbench leaves the
broker and becomes something you install, and installed plugins stop living inside the
installation directory — so an upgrade no longer overwrites them, and a service reads the
same directory the operator wrote to.

## Highlights

### The Bowire workbench is a plugin now (#154)

The broker no longer serves `/bowire`. The workbench ships as
[Surgewave.Diagnostics.Bowire](https://github.com/Kuestenlogik/Surgewave.Diagnostics.Bowire),
a `.swpkg` carrying both Bowire and the `surgewave://` protocol adapter, so one install gives
you the workbench speaking the native protocol.

The reason is not tidiness. The adapter is built against `Surgewave.Client`, which this
repository produces, so a broker that embedded the workbench could only ever be built against
an already-published adapter — against an older state of its own client. There was no
ordering of the two builds that resolved it. As a plugin, the adapter is built after both
products, like every other extension.

It also puts a debugging surface under deployment control rather than under a configuration
flag. Setting `Enabled=false` left the endpoints, the embedded resources and the dependencies
in the image, one misconfiguration away from being reachable. Not installing the plugin leaves
nothing to reach.

### Installed plugins are data, not part of the installation (#157, #158)

"The plugins directory" was resolved independently in the broker, the CLI, Control, the
Connect worker and the marketplace, and the first two disagreed: the CLI wrote to `plugins`
relative to the working directory while the broker read `plugins` next to its own executable.
Installing reported success, `plugins list` confirmed it, and the broker never saw the
plugin — with no error anywhere, because discovery only reports a directory missing entirely,
and next to the executable one usually exists.

Three scopes now, resolved once and shared by every process:

| Scope | Windows | Linux / macOS | Who put it there |
|---|---|---|---|
| installation | `<install>/plugins` | `<install>/plugins` | shipped with the artefact; replaced on upgrade |
| machine | `%ProgramData%\Surgewave\plugins` | `/var/lib/surgewave/plugins` | an operator, for this host |
| user | `~/.surgewave/plugins` | `~/.surgewave/plugins` | one account, for a broker it runs itself |

`surgewave plugins install` writes to the machine scope, because a broker usually runs under
a service account and a plugin in someone's home directory is invisible to it. When that
directory is not writable the command fails and names it rather than falling back to the user
scope — a silent fallback would report success for an install the broker could never see.
`plugins list` now shows each plugin's scope and names every directory it searched.

Connectors follow the same scopes. `plugins install --from-nuget` used to write to
`~/.surgewave/connectors` while the Connect plugin scanned a working-directory path; those two
have never named the same place.

### An administrator can refuse plugins from user directories (#158)

`Surgewave:Plugins:AllowUserScope=false` drops the per-user directory from the search order. A
plugin there is code the broker executes that an unprivileged account was able to place on
disk — the machine scope needs elevation, a home directory does not. The broker says at
startup when it has refused the scope, rather than leaving the plugin silently absent.

Put it in the installation's `appsettings.json` or in machine-scope configuration: the setting
binds someone who does not start the process, and anyone who does could equally point
`Surgewave:PluginsDirectory` elsewhere.

### Several brokers on one machine (#158)

`Surgewave:Instance` (or `SURGEWAVE_INSTANCE`) adds one path segment to the machine and user
scopes, the way `PGDATA` and `GEOSERVER_DATA_DIR` do. Unset means the scope is the root
itself, so a single-instance host keeps the short paths and naming an instance later moves
only that one. It is deliberately not the cluster id — two nodes of one cluster on one host
share that, and they are exactly the pair that must not share a directory.

`SURGEWAVE_DATA_DIR` moves every scope under one root, for tests and throwaway environments:
one tree to create, one to delete, instead of state scattered across `bin/Debug`, `bin` or the
repository root depending on how the process was started.

## Fixes

### The workbench was mapped without ever being initialised (#154)

`MapBowire()` maps endpoints and registers nothing. Without the matching `AddBowire()` the
recording, schema-change-log and plugin-update surfaces resolved their services with
`GetRequiredService` and threw on first use rather than at startup. The plugin now does both.

### Plugin configuration changed which settings applied, not which plugins existed (#158)

`Surgewave:PluginsDirectory` was honoured when layering plugin default settings and when
building the signer, but assembly loading had `plugins/` next to the executable hardcoded.
Pointing the setting somewhere therefore changed a plugin's configuration without changing
which plugins were loaded.

### Config inspection fell back to the CLI's own directory (#158)

`surgewave config view` resolves plugins relative to the config file being inspected, which is
right — it may be describing an installation on another host. Its last resort was the
directory next to `surgewave.exe`, which describes nothing; an unaccompanied config file was
inspected against whatever happened to sit there. It now falls back to the resolved scopes.

### Untagged builds published themselves as 0.1.0

CI derived its prerelease version from a hardcoded `0.1.0` rather than from the version floor,
so every branch build of a 0.5.x product published `0.1.0-ci.N` — a version sorting below
every release it superseded. The floor also now names the next patch (`0.5.1-dev` after
v0.5.0) rather than the released one, which under SemVer sorted a development build below the
release it contained.

## Breaking changes

**The broker no longer serves `/bowire`.** Install
`Kuestenlogik.Surgewave.Diagnostics.Bowire` to get it back. Deployments relying on the
endpoint will see 404 until they do; deployments that disabled it to keep it out of production
now need no setting at all.

**`surgewave plugins install` writes elsewhere.** The default was `plugins` relative to the
working directory; it is now the machine scope, and the command fails rather than falling back
when that is not writable. Scripts relying on the old default should pass `--directory`
explicitly, or `--scope user` where a per-user install is what was meant. Plugins installed
under the old default are not migrated — the broker never read that directory either, so
anything there was already inert.

**`Surgewave:Connect:PluginsDirectory` defaults to empty** rather than to `plugins`, meaning
all three scopes are scanned. Set it explicitly to restrict the search to one directory.

**`PluginManifest` is a record** rather than a class, so it gains value equality. Source
compatible; recompile anything comparing instances by reference.

## Also in this release

The test stack moved to xunit.v3 4.0 on Microsoft.Testing.Platform, and VSTest is gone from
every project. Coverage runs through `Microsoft.Testing.Extensions.CodeCoverage` — coverlet
does not work under that runner at all, which is worth knowing before reaching for it.

`PackPluginTask` takes the build's version instead of the one written in `plugin.json`, so a
tagged release can no longer publish NuGet packages as one version next to a `.swpkg` calling
itself another. `surgewave plugins pack --version` exposes the same override.
