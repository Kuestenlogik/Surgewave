---
layout: page
title: Surgewave vs Redpanda
subtitle: Both reject the JVM. Surgewave bets on .NET; Redpanda on C++.
description: A side-by-side of Surgewave and Redpanda — runtime, plugin model, embeddability, and licensing.
permalink: /compare/redpanda/
---

Redpanda and Surgewave both share the same starting premise: Kafka's protocol is great,
but the JVM-based broker stack is not. The two projects diverge from there on the runtime,
plugin model, and ecosystem fit.

## TL;DR

| Criterion | Surgewave | Redpanda |
|---|---|---|
| Runtime | .NET 10 / C# 14 | C++ (Seastar) |
| Kafka wire | 4.x | 100&nbsp;% |
| Plugin model | Signed `.swpkg`, ALC isolation | Wasm transforms |
| Embedded mode | Yes (in-process) | No |
| Stream processing | Surgewave.Streams (KIP-1071 aware) | Console transforms |
| AI pipelines | Built-in nodes (RAG, agents, ONNX) | External |
| License (core) | Apache 2.0 | BSL 1.1 (source-available, OSI-non-conforming) |

## Where Redpanda is the better fit

- You run a polyglot stack and don't want to add .NET to it.
- You need the absolute lowest per-broker latency on Linux and you've already
  tuned for thread-per-core Seastar semantics.
- You're on Redpanda Cloud (managed) and want first-party support.

## Where Surgewave is the better fit

- You ship .NET applications and want broker, streams, schema registry, and
  control plane on the same runtime as the rest of your stack &mdash; with
  embedded mode for tests and edge.
- You want to author broker, protocol, or storage plugins in C# and ship them
  as signed `.swpkg` packages, rather than authoring Wasm transforms.
- You want a permissively-licensed core (Apache 2.0) with enterprise extensions
  as separate plugins, not a BSL-on-the-core model.
- You need built-in AI pipeline nodes (retrieval, agents, ONNX) without
  bolting on a separate framework.

## Migration

If you're migrating off Redpanda, the wire protocol is the same Kafka one,
so existing producers/consumers point at Surgewave's `:9092` unchanged. Schema
Registry stays on `:8081` with the same Confluent-compatible REST API.

For Wasm transforms specifically, there is no direct Surgewave equivalent;
you would rewrite the logic as a `.swpkg` pipeline node. The
[Surgewave plugin signing guide](/docs/security/plugin-signing.html) covers
the build + sign + install flow.

## Also see

- [Surgewave vs Kafka](/compare/kafka/) &mdash; the original Kafka comparison
- [Surgewave vs Aeron](/compare/aeron/) &mdash; library vs broker
