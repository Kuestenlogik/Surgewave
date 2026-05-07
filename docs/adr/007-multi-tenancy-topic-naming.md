# ADR-007: Multi-Tenancy Topic Naming Convention

## Status

Accepted

## Date

2026-03

## Context

Surgewave needed namespace-level isolation similar to Apache Pulsar's tenant/namespace model. Without a naming convention, multi-tenant deployments resorted to ad-hoc prefixes (e.g., `acme_prod_orders`), which made it impossible to enforce quotas, apply policies, or reason about ownership consistently.

### Alternatives Considered

- **Flat topic names with metadata:** Keep simple names and attach tenant/namespace as topic metadata. Simpler naming, but policies and quotas would require a separate lookup for every operation.
- **Pulsar-style persistent://tenant/namespace/topic:** Too verbose. The `persistent://` prefix is redundant when all Surgewave topics are persistent by default.
- **Dot-separated names (tenant.namespace.topic):** Conflicts with MQTT topic hierarchy conventions and makes it harder to parse (dots appear in tenant and topic names).

## Decision

Adopt a three-level topic naming convention using `/` as the separator:

```
tenant/namespace/topic
```

Example: `acme-corp/production/orders`

A `TenantTopicResolver` utility parses and builds these names. Quotas are enforced per tenant, policies (retention, replication) per namespace. Topics without a tenant/namespace prefix are placed in a `default/default` context for backward compatibility.

## Consequences

- **Clear ownership hierarchy.** Every topic belongs to a tenant and namespace, enabling systematic policy enforcement.
- **Backward compatible.** Existing topic names without separators are treated as `default/default/topic-name`.
- **MQTT conflict.** MQTT natively uses `/` as a topic-level separator with different semantics. The MQTT bridge maps Surgewave's `/` to `.` in MQTT topic names to avoid ambiguity.
- **Breaking for existing multi-tenant setups** that used their own naming conventions. Migration requires renaming topics, which is opt-in and documented.
