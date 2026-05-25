# ADR-004: Feature Toggle Pattern

## Status

Accepted

## Date

2026-03

## Context

Surgewave includes many optional capabilities: MQTT bridge, GraphQL API, Privacy engine, Data Mesh, Schema Registry, Connect framework, Serverless Functions, and others. A minimal deployment (e.g., a lightweight edge broker) should not pay the cost of features it does not use --- neither in memory, startup time, nor attack surface.

Without a consistent pattern, each feature would invent its own way of being enabled or disabled, leading to inconsistent configuration and hard-to-predict behavior.

### Alternatives Considered

- **Separate binaries per feature set:** Maximum isolation but painful to build, distribute, and operate.
- **Compile-time feature flags (#if directives):** No runtime flexibility. Requires different builds for different deployments.
- **Feature flag service (LaunchDarkly-style):** Over-engineered for infrastructure toggles that rarely change at runtime.

## Decision

All optional features follow a consistent configuration pattern:

```
Surgewave:FeatureName:Enabled = true|false (default: false)
```

When a feature is disabled:
- Its services are **not registered** in the DI container.
- Its endpoints are **not mapped**.
- Its UI elements are **hidden** in the Control dashboard.

Feature authors implement this by checking the configuration value during `Program.cs` service registration and conditionally calling the feature's `AddXxx()` extension method.

## Consequences

- **Zero overhead** for disabled features: no memory allocation, no background threads, no exposed endpoints.
- **Consistent pattern** that developers can follow when adding new features. Grep for `Enabled` in `BrokerConfig` to see all toggleable features.
- **Simple deployment tuning:** operators enable exactly what they need via environment variables or config files.
- Feature state must be checked in multiple places (DI registration, endpoint mapping, UI rendering), which can lead to missed spots if not careful. Integration tests validate that disabled features are truly inert.
