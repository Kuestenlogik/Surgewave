# Edge → Cloud over QUIC

A 10-minute guide to running a Surgewave edge node that syncs to a central broker over QUIC instead of TCP. This is the right choice for edge deployments where the uplink is lossy, mobile, or otherwise unpredictable — factory floor Wi-Fi, delivery vehicles on cellular, field sensors on satellite links, remote sites with bursty connectivity.

## Why QUIC for the edge

QUIC delivers three things TCP cannot on a bad link:

1. **Per-stream flow control.** On a shared TCP connection, a single lost packet stalls every in-flight request until retransmission succeeds. QUIC isolates streams at the transport layer, so a stall on one metric burst cannot block a control message on the same connection.
2. **0-RTT resumption.** Reconnecting to a broker you recently talked to skips the TLS handshake round-trip entirely — the edge picks up where it left off in a single packet.
3. **Connection migration.** The client identity is tied to a connection ID, not to an IP/port tuple. Wi-Fi → cellular handoffs, NAT rebindings, and switching between uplink interfaces all survive without a reconnect surgewave.

The benchmark numbers: on a clean LAN QUIC is ~5% slower than TCP (expected — userspace crypto). Under 1% packet loss + 10 ms one-way latency, QUIC delivers more throughput than TCP does at **0% loss**. For more context see `ROADMAP.md` section "Transport & Peer-to-Peer Hardening".

## Prerequisites

- **.NET 10** on the edge host.
- **msquic** — built into Windows 11 and Windows Server 2022+; on Linux install `libmsquic` via your package manager.
- A Surgewave broker reachable from the edge. The broker must have a QUIC endpoint enabled (either `Surgewave:Quic:Enabled=true` for client-facing QUIC via `Kuestenlogik.Surgewave.Protocol.Quic`, or standard TCP if the edge is using `SurgewaveTransportType.Tcp`).

## Minimal edge program

```csharp
using Kuestenlogik.Surgewave.Edge;
using Kuestenlogik.Surgewave.Transport;

await using var edge = await EdgeBrokerBuilder
    .Create("factory-floor-1")
    .WithMemoryStorage()
    .WithCloudSync("central-broker.example.com:9094", cfg =>
    {
        cfg.SyncTopics = ["sensor-readings", "machine-alerts"];
        cfg.SyncIntervalSeconds = 5;
        cfg.CompressSync = true;
        // Increase the offline buffer on mobile uplinks where outages of
        // minutes are normal. Messages are replayed when the link recovers.
        cfg.OfflineBufferMaxMb = 512;
    })
    .WithCloudTransport(SurgewaveTransportType.Quic)   // <-- here
    .WithTopics("sensor-readings", "machine-alerts")
    .BuildAsync();

// Produce locally — works offline, buffers for later sync.
await edge.Client.Messaging.SendAsync(
    topic: "sensor-readings",
    partition: 0,
    key: null,
    value: System.Text.Encoding.UTF8.GetBytes("""{"sensor":"motor-7","rpm":1420}"""));

await Task.Delay(TimeSpan.FromMinutes(5));
```

That's the whole integration. `WithCloudTransport(SurgewaveTransportType.Quic)` flips the cloud sync path from TCP to QUIC. Local produce/consume continues to hit the embedded Surgewave runtime directly — only the edge→cloud uplink is QUIC.

## What happens on a bad link

Scenario: the factory floor has a spotty 4G backup link that experiences periodic packet loss spikes of 2–5% during shift changes, plus occasional 30-second blackouts when the signal drops entirely.

With the default TCP transport:

- Each 2% loss spike stalls the connection while TCP retransmits head-of-line bytes. Sensor messages pile up in the offline buffer.
- The 30-second blackouts force a full TCP reconnect + TLS handshake (~3 round trips on a 200 ms RTT link = ~600 ms) before sync can resume.
- Throughput during degraded periods drops to 20–40% of steady-state.

With `SurgewaveTransportType.Quic`:

- Loss spikes affect only the streams carrying the lost packets, not the entire connection. Messages on other streams continue to flow.
- Blackouts are recovered via QUIC's 0-RTT session resumption — the edge resumes sending in a single round trip without re-negotiating TLS.
- The connection ID stays valid across IP/port changes, so if the backup 4G link comes back on a different NAT mapping the existing QUIC connection continues without a fresh handshake.

End result: same code, same topics, same offline buffer semantics — substantially better behaviour on real-world edge uplinks.

## Verifying it actually used QUIC

Check the log output from the edge process:

```
Connected to cloud broker at central-broker.example.com:9094 via Quic
```

On the broker side, the Control UI broker list page now shows a `⚡ QUIC` chip on brokers that are running inter-broker QUIC, and `TCP` on the others — though that chip reflects inter-broker, not edge-client transport. To confirm the edge is actually negotiating QUIC, look at the broker's connection logs for ALPN `surgewave/1` (the QUIC peer transport adapter) or, for the raw Kafka wire protocol over QUIC, check the `QuicBrokerAdapter` listener log.

## Production checklist

1. **Certificates.** The dev default uses a self-signed cert with `TrustAllCertificates = true`. For production:
   - Issue a broker certificate from your cluster CA.
   - Set `Surgewave:Quic:CertificatePath` on the broker.
   - Distribute the CA cert to edge nodes and configure trust explicitly (pluggable validation is a tracked follow-up).
2. **UDP port reachability.** QUIC rides on UDP. Confirm your network path (firewall, NAT, load balancer) actually forwards UDP to the broker — many corporate networks drop non-TCP egress by default.
3. **Fallback transport.** If QUIC is unreachable (msquic missing, UDP blocked), the broker's startup code falls back to TCP with a clear warning. The edge does not currently auto-fall-back — configure it explicitly if you need dual-mode behaviour.
4. **Monitoring.** Track `surgewave_peer_connection_active` and `surgewave_peer_bytes_sent` (metrics follow-up tracked in roadmap) per transport. Visibility on "which connections are QUIC" is currently surfaced in the Control UI broker list.

## See also

- `ROADMAP.md` — Transport & Peer-to-Peer Hardening section for the full QUIC story and open items.
- `src/Kuestenlogik.Surgewave.Edge/EdgeBrokerBuilder.cs` — fluent API reference.
- `src/Kuestenlogik.Surgewave.Transport.Quic/QuicTransport.cs` — the client transport implementation.
- `benchmarks/Kuestenlogik.Surgewave.Benchmarks.QuicVsTcp/` — the A/B benchmark suite including configurable packet-loss scenarios.
