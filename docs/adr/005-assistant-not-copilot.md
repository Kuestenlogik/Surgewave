# ADR-005: Naming: Assistant instead of Copilot

## Status

Accepted

## Date

2026-03

## Context

Surgewave's AI-powered operations assistant was initially named "Copilot" throughout the codebase --- classes, routes, UI labels, and documentation all used this term. After Microsoft's aggressive branding of "Copilot" across their product line (GitHub Copilot, Microsoft 365 Copilot, Windows Copilot), the name created unwanted brand association and potential confusion about whether Surgewave's feature was related to or powered by Microsoft's Copilot products.

### Alternatives Considered

- **Keep "Copilot":** The term is generic, but the brand association is now too strong. Users and stakeholders consistently assumed a Microsoft connection.
- **"Autopilot":** Implies full automation, which overstates the feature's role. It assists operators; it does not replace them.
- **"Advisor":** Too passive. The assistant can take actions, not just advise.

## Decision

Rename "Copilot" to "Surgewave Assistant" across the entire codebase:

- Route: `/copilot` -> `/assistant`
- Classes: `CopilotService` -> `AssistantService`, `CopilotState` -> `AssistantState`, etc.
- UI: All labels and menu items updated.
- Documentation: All references updated.

## Consequences

- **Clear brand separation** from Microsoft's Copilot products. No more confused stakeholders.
- **~20 files renamed** and all internal references updated. This was a mechanical change with no functional impact.
- The term "Assistant" is descriptive and neutral, accurately reflecting the feature's role as an AI-powered operations helper.
- External documentation and blog posts referencing the old name needed updating.
