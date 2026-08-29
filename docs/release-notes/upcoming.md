---
title: <short theme — keep it under 60 chars; matches the milestone title tail>
version: <X.Y.Z, no leading v>
---

<One-sentence summary of what the release is about. Sets the frame
before Highlights drops into specifics.>

## Highlights

### The metadata quorum can be smaller than the cluster (#167, #168)

Until now every broker voted on metadata, so a metadata write waited for a
majority of *all* brokers and adding brokers made it slower. Two new
settings follow Kafka's shape: `ProcessRoles` (`broker`, `controller`, or
both) and `ControllerQuorumVoters` (`id@host:port`). Nodes outside the
voter list become observers — they receive and serve the metadata log
without voting on it, so the quorum stays the size you chose. See
[Roles and the Controller Quorum](../clustering/raft.md#roles-and-the-controller-quorum).

Combined mode stays the default and nothing changes for a single broker,
an embedded host, or an existing cluster: leave both settings alone and
every node keeps voting exactly as before.

### <Headline of the second major change> (#issue)

<...>

## Fixes

### <Grouped fix headline> (#issue, #issue)

<1-3 sentences per group.>

## Breaking changes

<Only when real. Each change has been on a back-compat ramp through the
prior minor and is removed in this release.>

### <Title of the breaking change> (#issue)

<What changed on the wire / API / package surface, what to do to migrate.>

## Acknowledgements

<Optional. Short. Names contributors who exercised the rc series or
reported the bugs that landed as fixes.>
