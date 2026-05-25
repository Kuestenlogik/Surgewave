# ADR-013: Control UI Plugin-First Architecture

| Status   | Date       |
|----------|------------|
| Accepted | 2026-05-19 |

## Context

Surgewave.Control is the broker's web UI — a Blazor Server application that ships in the Apache 2.0 Surgewave core repository. Today it contains Razor pages for every feature surface: community features (topics, brokers, consumer groups, schema registry, plugins, …) and enterprise features (replication, governance/audit/privacy, AI agents, ML scoring, serverless functions, pipeline-chat, performance advisor, …).

After the 2026-05-18 license migration to Open-Core (Apache 2.0 core + BSL 1.1 premium-tier repos — see ADR-011 and the README), the enterprise Razor pages no longer belong in the core repository:

- They reference enterprise concepts (geo-replication, RAG orchestration, governance lineage) that the Apache-licensed broker does not implement on its own.
- They invite a license-compatibility audit for any operator running the core: "is this page provided under Apache 2.0 or BSL 1.1?"
- They produce a UI surface that promises features the core broker cannot deliver without a separately-licensed plugin.

The plugin scaffolding to host enterprise UI from outside the core already exists. `IControlPlugin` (in `Kuestenlogik.Surgewave.Plugins`) carries `GetNavItems()` and a `PageAssembly` exposed to Blazor Router's `AdditionalAssemblies`. `ControlPluginRegistry` discovers implementations from `plugins/`-folder `.swpkg`s at startup. `NavMenu.razor` already renders a `_pluginNavGroups` block at the end of the sidebar from the registry's `NavItems` — but the static `MudNavLink` entries higher up in the menu still point at routes that only enterprise plugins can satisfy.

## Decision

The Surgewave core (`Kuestenlogik.Surgewave.Control`) ships **only** Community-feature Razor pages and navigation items. Every Enterprise feature contributes its Control surface via `IControlPlugin` from its own BSL 1.1 repository.

### Concrete consequences

1. **NavMenu.razor** no longer contains static `MudNavLink` entries for enterprise routes. The dynamic `_pluginNavGroups` block — populated by `ControlPluginRegistry.NavItems` — is the only path for enterprise nav items.
2. **Razor pages** for enterprise features remain physically in the core repo *for now* (folder-level README markers identify their migration target), but their `@page` directives become unreachable without the corresponding plugin once the static NavLinks are removed. They will be physically migrated to their target Premium repos in a follow-up (Phase 7b).
3. **Unreachable routes** — if an operator deep-links to e.g. `/agents` without `Surgewave.Ai` installed, the Blazor router falls through to the default `NotFound` page. A future iteration of `ControlPluginRegistry` may render an "Install Surgewave.X to enable this view" fallback for known enterprise routes; this is not part of the initial cut.

### Migration map (Apache core → separately-licensed extensions)

Each extension owns its feature-id constant in its own repository; the core repo
intentionally does not enumerate them. The "feature key" column below names the
id the extension declares for itself, which a licence provider then gates.

| Page / folder                                     | Migration target          | Feature id declared by extension    |
|---------------------------------------------------|---------------------------|-------------------------------------|
| `Pages/Agents/`                                   | Surgewave.Ai extension    | `Surgewave.Ai`                      |
| `Pages/Chat/PipelineChat.razor`                   | Surgewave.Ai extension    | `Surgewave.Ai`                      |
| `Pages/ML/`                                       | Surgewave.Ai extension    | `Surgewave.Ai`                      |
| `Pages/Assistant/AssistantPage.razor`             | Surgewave.Ai extension    | `Surgewave.Ai`                      |
| `Pages/Alerts/PerformanceAdvisor.razor`           | Surgewave.Ai extension    | `Surgewave.Ai`                      |
| `Pages/Replication/`                              | Replication extension     | `Surgewave.Replication`             |
| `Pages/Privacy/`                                  | Governance extension      | `Surgewave.Governance`              |
| `Pages/Audit/AuditLogViewer.razor`                | Governance extension      | `Surgewave.Governance`              |
| `Pages/Functions/FunctionsPage.razor`             | Functions extension       | `Surgewave.Functions`               |

### What stays in the core

Every page tied to an Apache-2.0 module that ships in the core repository:

- `Pages/Topics/`, `Pages/Brokers/`, `Pages/ConsumerGroups/`, `Pages/Clusters/`, `Pages/Messages/`, `Pages/Health/`, `Pages/Home.razor`
- `Pages/Connectors/`, `Pages/Plugins/`, `Pages/SchemaRegistry/`, `Pages/Pipelines/`
- `Pages/Quotas/`, `Pages/Security/` (ACLs, RBAC roles), `Pages/Settings/`
- `Pages/Cdc/`, `Pages/Wasm/`, `Pages/Operations/` (auto-tuning, cruise-control, rolling upgrades, partition reassignment)
- `Pages/Catalog/`, `Pages/DataMesh/`, `Pages/MultiTenancy/`, `Pages/Dlq/`, `Pages/Queue/`
- `Pages/Metrics/`, `Pages/Alerts/AlertsDashboard.razor`, `Pages/Debug/`, `Pages/Help/`, `Pages/Account/`
- `Pages/Query/SqlQuery.razor`

## Alternatives Considered

- **License-gating inside the core**: keep all pages, hide enterprise ones at render time via `ILicenseProvider.HasFeature`. Cleaner for users (single deploy, one UI), but keeps the Apache-licensed core shipping enterprise-functionality code — exactly the license-boundary problem we want to fix.
- **Conditional compilation flags** (`#if ENTERPRISE`): build-time strip enterprise pages from the core assembly. Works mechanically but produces two divergent core builds (Community vs Enterprise) and breaks the "one binary, plugins add features" promise of the rest of the architecture.
- **Stay status quo**: leave everything in the Apache-licensed core. Rejected — defeats the open-core licensing posture.

## Implementation Plan

- **Phase 7a (now):** ADR (this document) + remove static enterprise NavLinks from `NavMenu.razor` + folder-level README markers in each Pages/ enterprise folder. Razor pages still ship in the core, but become unreachable through the sidebar.
- **Phase 7b (follow-up):** physically move each enterprise Pages/-folder + create `IControlPlugin` wrapper in the target Premium repo, replace the in-repo csproj reference with a `PackageReference` on the published plugin .swpkg. Repository-by-repository: Replication → Governance → Functions → Ai.
- **Phase 7c (optional):** "Install Surgewave.X to enable this view" fallback page for known enterprise routes when the plugin is not installed.

## Consequences

- The Apache-licensed core ships a UI that only renders what the broker can actually serve on its own.
- Premium plugin packages (`Surgewave.Ai.swpkg`, `Surgewave.Replication.swpkg`, …) become responsible for their own UI, navigation, and routing. Plugin authors of the open-source ecosystem follow the same pattern.
- Existing operators who install all premium repos see the same UI as today — the user-visible surface is unchanged after Phase 7b.
- Operators running the core alone see a focused, smaller sidebar that does not advertise features they cannot reach.

## See also

- ADR-011 — Community/Enterprise split (the broader repository-level decision this ADR implements at the UI layer)
- `Kuestenlogik.Surgewave.Plugins.IControlPlugin` — plugin contract
- `Kuestenlogik.Surgewave.Control.Services.ControlPluginRegistry` — discovery implementation
- `Kuestenlogik.Surgewave.Plugins.Licensing.ILicenseProvider` — optional licence gate consulted via `IBrokerPlugin.RequiresLicense`
