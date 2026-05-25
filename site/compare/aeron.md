---
layout: page
title: Surgewave vs Aeron
subtitle: Aeron is a transport library. Surgewave is a persistent broker. Different beasts.
description: A side-by-side of Surgewave and Aeron — broker vs library, persistence, durability, and use-case fit.
permalink: /compare/aeron/
---

Aeron is in a different category from Surgewave. It's a high-performance messaging
**transport library** &mdash; you run it inside your process &mdash; not a persistent
broker. The two get compared because both target the .NET / JVM low-latency
performance niche, but the semantics differ fundamentally.

## TL;DR

| Criterion | Surgewave | Aeron |
|---|---|---|
| Form factor | Standalone broker (with embedded mode) | Library inside your process |
| Persistence | Yes &mdash; segment log + tiered storage | None &mdash; in-memory + optional archive |
| Wire protocol | Kafka 4.x + Native + gRPC + MQTT + AMQP + QUIC | UDP / IPC (custom) |
| Latency | Sub-ms P99 (native protocol) | Microsecond UDP |
| Throughput | &gt;1M msg/s/broker | 10M+ msg/s in-memory |
| Replication | Per-partition ISR + Raft | None (transport only) |
| License (core) | Apache 2.0 | Apache 2.0 |
| Use case | Event streaming, durable logs, microservices | Cross-process / cross-machine RPC, market-data feeds |

## Where Aeron is the better fit

- You're shipping a market-data fan-out where every microsecond matters and
  you don't need durable storage.
- You want the messaging primitive in your process, not a separate broker
  hop.
- You're already on the JVM and want a library, not infrastructure.

## Where Surgewave is the better fit

- You need durable, replayable event logs &mdash; Aeron doesn't persist by
  default.
- You want a Kafka-compatible API for consumers / producers / tools that
  already speak Kafka.
- You ship .NET applications and want broker + streams + schema registry on
  the same runtime, with an embedded mode for tests.
- You need geo-replication, exactly-once semantics, or schema-managed evolution
  &mdash; broker concerns Aeron doesn't address.

## Hybrid scenarios

The two are not mutually exclusive. Teams have used Aeron as the in-process
fan-out path for sub-microsecond consumers and Kafka/Surgewave for the durable
event-sourced backbone. Surgewave's native protocol over TCP/QUIC is faster
than the Kafka wire and closer to Aeron's UDP latency profile, so a single-
stack Surgewave deployment often closes the gap enough that the second runtime
isn't needed.

## Also see

- [Surgewave vs Kafka](/compare/kafka/) &mdash; the broker-to-broker comparison
- [Surgewave vs Redpanda](/compare/redpanda/) &mdash; both reject the JVM
