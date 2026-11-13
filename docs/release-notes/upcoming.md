---
title: <what this delivery is about, as a sentence — it is the release's heading, not its version>
version: <X.Y.Z, no leading v>
---

<One-sentence summary of what the release is about. Sets the frame
before Highlights drops into specifics.>

## Highlights

### <Headline of the first major change> (#issue)

<2-4 sentences. What changed, why it matters to the user, where to
look in the docs / settings.>

### <Headline of the second major change> (#issue)

<...>

## Fixes

### The replication listener binds the address you name (#285)

`Surgewave:ReplicationBindAddress` says which address the replication listener binds. Unset it is every IPv4
interface, as before, which is what a cluster on several hosts needs. A broker whose peers all run on its own
host sets `127.0.0.1` and is then not reachable on that port from outside. The embedded broker
(`SurgewaveRuntime`) does this by itself whenever its client listener is on loopback, so a cluster started in a
test or on a developer's machine no longer listens on every interface, which on Windows raised a firewall prompt
for each new checkout.

## Breaking changes

<Only when real. Each change has been on a back-compat ramp through the
prior minor and is removed in this release.>

### <Title of the breaking change> (#issue)

<What changed on the wire / API / package surface, what to do to migrate.>

## Acknowledgements

<Optional. Short. Names contributors who exercised the rc series or
reported the bugs that landed as fixes.>
