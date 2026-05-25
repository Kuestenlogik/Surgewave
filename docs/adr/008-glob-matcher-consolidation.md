# ADR-008: Shared GlobMatcher Utility

## Status

Accepted

## Date

2026-03

## Context

Four separate modules had their own glob pattern matching implementations:

- **Replication:** Matching topic patterns for replica assignment.
- **SchemaLinking:** Matching subject patterns for schema propagation.
- **Privacy:** Matching field paths in dot-separated notation (e.g., `user.*.email`).
- **SchemaInference:** Matching topic names for auto-inference rules.

Each implementation had subtle behavioral differences. For example, some treated `*` as matching across dots, others did not. Bug fixes in one module were not propagated to the others. This was a maintenance burden and a source of inconsistent behavior.

### Alternatives Considered

- **Use an existing NuGet package (Microsoft.Extensions.FileSystemGlobbing):** Designed for file paths, not topic/field names. Assumes `/` as separator and does not support configurable separators.
- **Regular expressions instead of globs:** More powerful but harder to read and write for operators configuring topic patterns.
- **Keep separate implementations:** Lowest risk of breaking changes, but highest maintenance cost.

## Decision

Extract a shared `GlobMatcher` class into `Kuestenlogik.Surgewave.Core.Util`. The matcher follows standard glob semantics:

- `*` matches any characters **within** a single segment (between separators).
- `**` matches any characters **across** separators (zero or more segments).
- `?` matches exactly one character.

The separator defaults to `/` (for topic names). An optional `dotIsSeparator` parameter treats `.` as an additional separator, which the Privacy module uses for dot-separated field paths like `user.*.email`.

All four modules were updated to use the shared implementation.

## Consequences

- **Single implementation** to maintain and test. Bug fixes apply everywhere.
- **Consistent behavior** across Replication, SchemaLinking, Privacy, and SchemaInference.
- **One test fix required:** A SchemaInference test expected `*` to match across dots in a topic name (e.g., `events.*` matching `events.user.created`). After consolidation, this correctly requires `events.**` instead. The test was updated.
- The `dotIsSeparator` parameter keeps the API clean while supporting the Privacy module's specific needs without a separate code path.
