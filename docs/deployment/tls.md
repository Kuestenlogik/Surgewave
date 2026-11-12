# TLS for the gRPC / REST endpoint

Surgewave's gRPC + REST surface on port `9093` binds over plain HTTP by default so the
quick-start works without generating certificates. In production (and any deployment that
talks over a network you don't fully control) the endpoint should run over HTTPS.

This guide describes the `Surgewave:GrpcUseTls` toggle, how to pick the right certificate
source, and how to roll it out without downtime.

## The one-line switch

Set `Surgewave:GrpcUseTls=true` in your `appsettings.json`:

```json
{
  "Surgewave": {
    "GrpcPort": 9093,
    "GrpcUseTls": true
  }
}
```

The broker overrides the Kestrel endpoint configuration at startup and binds
`https://*:9093` instead of `http://*:9093`. The ASP.NET Core development certificate is
used automatically — run this once on the host to trust it:

```bash
dotnet dev-certs https --trust
```

Dev certs are fine for local development, integration tests, and trusted-network
internal deployments. For anything public, supply a proper certificate.

## Bring your own certificate

```json
{
  "Surgewave": {
    "GrpcPort": 9093,
    "GrpcUseTls": true,
    "GrpcCertificatePath": "/etc/surgewave/certs/broker.pfx",
    "GrpcCertificatePassword": "changeit"
  }
}
```

The path can point at a PFX, a PEM with the private key alongside (Kestrel reads standard
combined PFX / PEM formats), or a cert path resolvable by your host's `X509Store`. The
password is only required for PFX files with non-empty passwords.

Password-free production alternatives:

- **Kubernetes secret** — mount the PFX into the broker pod at
  `/etc/surgewave/certs/broker.pfx` via a `Secret` and read the password from an env var that
  the config system picks up (`Surgewave__GrpcCertificatePassword`).
- **Let's Encrypt / ACME** — run a sidecar that renews the cert on disk; Kestrel re-reads
  it on next connection so restarts are unnecessary.
- **Public CA** — a DigiCert / Sectigo / GlobalSign cert in PFX form works identically to
  the above.

## In the container image

The container image serves the endpoint in cleartext. `appsettings.json` turns TLS on and relies on
the ASP.NET Core development certificate, which a workstation has and an image does not, so the
image sets four environment variables that undo it:

```
Surgewave__GrpcUseTls=false
Kestrel__Endpoints__Grpc__Url=http://*:9093
Kestrel__Endpoints__GrpcHttp3__Url=http://localhost:9094
Kestrel__Endpoints__GrpcHttp3__Protocols=Http1AndHttp2
```

The broker says so in a warning at every start:

```
gRPC, REST, /health and /metrics are served without TLS on port 9093. To serve them over HTTPS set
Surgewave:GrpcUseTls=true and Surgewave:GrpcCertificatePath (docs/deployment/tls.md).
```

To serve the endpoint over HTTPS, mount a certificate and name it, as environment variables: an
environment variable wins over a mounted `appsettings.json`, so the same settings in a file lose
to the image's own.

```yaml
services:
  surgewave-broker:
    image: ghcr.io/kuestenlogik/surgewave-broker:0.6.0
    environment:
      - Surgewave__GrpcUseTls=true
      - Surgewave__GrpcCertificatePath=/certs/broker.pfx
      - Surgewave__GrpcCertificatePassword=changeit
    volumes:
      - ./certs:/certs:ro
      - surgewave-data:/app/data
```

The certificate file has to be readable for uid 1654, the user the image runs as. The HTTP/3
endpoint on port 9094 stays on loopback; to serve it as well, set
`Kestrel__Endpoints__GrpcHttp3__Url=https://*:9094`,
`Kestrel__Endpoints__GrpcHttp3__Protocols=Http1AndHttp2AndHttp3` and
`Kestrel__Endpoints__GrpcHttp3__Certificate__Path` (and `__Password`).

## Behaviour differences vs. cleartext mode

| Aspect | `GrpcUseTls=false` (default) | `GrpcUseTls=true` |
|--------|------------------------------|-------------------|
| Endpoint URL | `http://*:9093` | `https://*:9093` |
| HTTP/2 ALPN | Not applicable — the suppression filter `Microsoft.AspNetCore.Server.Kestrel: Error` hides the cleartext warning | ALPN works; the suppression filter is lifted and Kestrel logs at `Warning` again |
| `GrpcHttp3` endpoint on `:9094` | Unchanged — always HTTPS (HTTP/3 requires TLS) | Unchanged |
| Client code | Default `http://broker:9093` | Callers must use `https://broker:9093` |
| Clients without cert trust | Works | Connection refused — trust the cert or use `HttpClientHandler.ServerCertificateCustomValidationCallback` in dev |

## Rolling it out

1. Stand up a single broker with `GrpcUseTls=true`. Point a test client at
   `https://broker:9093` to confirm TLS handshakes succeed. Look for the `ALPN` line in
   Wireshark / `openssl s_client` output if you want to prove HTTP/2 negotiation works.
2. Flip the rest of the brokers. Kestrel re-binds on restart — partition replication
   keeps clients served while each broker cycles.
3. Update client connection strings (Confluent.Kafka `BootstrapServers`, Surgewave Native
   `ISurgewaveClient`, your own REST callers) from `http://` to `https://`. The broker's
   cleartext Kafka protocol on port `9092` is unaffected.
4. Remove the Kestrel log-level suppression in your custom `appsettings` if you had one
   layered over ours — the toggle removes it automatically at startup.

## What the toggle does not change

- The **Kafka protocol** on port `9092` stays cleartext unless you also configure SASL/TLS
  at the Kafka level — that's `Surgewave:Kafka:Tls` territory, not `Surgewave:GrpcUseTls`.
- The **inter-broker replication** port is unaffected. See the mTLS / QUIC docs for
  replication TLS.
- The **QUIC / HTTP/3 endpoint** on `:9094` is already HTTPS and requires no toggle.

## Troubleshooting

- **`Connection refused: The request was aborted: Could not create SSL/TLS secure channel.`**
  The client cannot validate the server cert. In development: `dotnet dev-certs https --trust`.
  In production: install the CA chain in the client's trust store, or configure the client
  to skip validation explicitly (only acceptable for smoke-testing).
- **`HTTP/2 over TLS requires ALPN support`**
  Your .NET runtime pre-dates ALPN-over-HTTPS — upgrade to .NET 10 (Surgewave's minimum).
- **Tests pinning `http://localhost:9093`**
  Flip them to `https://` and add a `SocketsHttpHandler` that trusts the dev cert. Or run
  tests with `Surgewave:GrpcUseTls=false` — the toggle is additive, the default stays cleartext.
