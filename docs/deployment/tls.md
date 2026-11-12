# TLS for the gRPC / REST endpoint

The broker serves gRPC, REST, `/health` and `/metrics` on two HTTP endpoints, port `9093` and port
`9094`. A published broker — the container image, the archives, the installers — serves them in
cleartext until it is given a certificate, and says so in a warning at every start. In production
(and any deployment that talks over a network you don't fully control) the endpoints should run over
HTTPS.

This guide describes the `Surgewave:GrpcUseTls` toggle, how to pick the right certificate
source, and how to roll it out without downtime.

## What the two endpoints serve

`Surgewave:GrpcUseTls` is the one switch. It decides the scheme of both endpoints, whatever scheme
their URLs in the `Kestrel` section are written with, and with it what each one serves: a client and
a server agree on HTTP/2 or HTTP/1.1 in the TLS handshake, so an endpoint without TLS serves one of
the two.

| Endpoint | `GrpcUseTls=false` | `GrpcUseTls=true` |
|----------|--------------------|-------------------|
| `Grpc`, port `9093` (`Surgewave:GrpcPort`) | REST, `/health`, `/metrics` over HTTP/1.1 | gRPC, REST, `/health`, `/metrics` over HTTPS (HTTP/1.1 and HTTP/2) |
| `GrpcHttp3`, port `9094` | gRPC over HTTP/2 without TLS (prior knowledge) | the same as port `9093` over HTTPS, and HTTP/3 |

Without TLS a gRPC client therefore connects to `http://broker:9094`, and a REST client, a health
check and the Control UI to `http://broker:9093`. With TLS both use `https://broker:9093`.

`appsettings.json` sets `GrpcUseTls` to `false`. In the `Development` environment — `dotnet run`,
`scripts/stack.ps1` — `appsettings.Development.json` sets it to `true`, and the broker uses the
ASP.NET Core development certificate. Without the setting at all the `Kestrel` section stands as it
is written.

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

The broker binds `https://*:9093` and the second endpoint over HTTPS as well. Without a certificate
path it uses the ASP.NET Core development certificate — run this once on the host to create and
trust it:

```bash
dotnet dev-certs https --trust
```

A host without that certificate — a server, a container — ends the start with "Unable to configure
HTTPS endpoint"; give it a certificate as described next.

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

The certificate is used for both endpoints. An endpoint that names a certificate of its own in the
`Kestrel` section (`Kestrel:Endpoints:GrpcHttp3:Certificate`) keeps that one.

## In the container image

The container image sets nothing about TLS: it starts with the `appsettings.json` it holds, in
cleartext. The broker says so in a warning at every start:

```
REST, /health and /metrics are served without TLS on port 9093, gRPC over HTTP/2 without TLS on port 9094.
To serve them over HTTPS set Surgewave:GrpcUseTls=true and Surgewave:GrpcCertificatePath (docs/deployment/tls.md).
```

To serve the endpoints over HTTPS, mount a certificate and name it — as environment variables:

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

or in a mounted file. The image runs in the `Production` environment, so a file mounted as
`/app/appsettings.Production.json` lies over the image's `appsettings.json` and needs to hold nothing
but what differs:

```json
{
  "Surgewave": {
    "GrpcUseTls": true,
    "GrpcCertificatePath": "/certs/broker.pfx",
    "GrpcCertificatePassword": "changeit"
  }
}
```

```yaml
    volumes:
      - ./tls.json:/app/appsettings.Production.json:ro
      - ./certs:/certs:ro
      - surgewave-data:/app/data
```

The certificate file has to be readable for uid 1654, the user the image runs as. The image holds
no QUIC library (`libmsquic`), so in it the second endpoint serves HTTP/1.1 and HTTP/2 over HTTPS
and no HTTP/3, without a word in the log.

The health check of the Compose files, `surgewave-broker health ready`, asks the broker over HTTPS
then and accepts a certificate whose name differs, not one whose chain it cannot verify: with a
self-signed certificate or a private CA the container reports unhealthy while the broker serves
its clients, unless the CA is in the image's trust store.

## Behaviour differences vs. cleartext mode

| Aspect | `GrpcUseTls=false` | `GrpcUseTls=true` |
|--------|--------------------|-------------------|
| REST, `/health`, `/metrics` | `http://broker:9093` | `https://broker:9093` |
| gRPC | `http://broker:9094`, HTTP/2 with prior knowledge | `https://broker:9093` |
| Port `9094` | gRPC without TLS | HTTPS, with HTTP/3 where the host has QUIC |
| Clients without cert trust | Works | Connection refused — trust the cert or use `HttpClientHandler.ServerCertificateCustomValidationCallback` in dev |

## Rolling it out

1. Stand up a single broker with `GrpcUseTls=true`. Point a test client at
   `https://broker:9093` to confirm TLS handshakes succeed. Look for the `ALPN` line in
   Wireshark / `openssl s_client` output if you want to prove HTTP/2 negotiation works.
2. Flip the rest of the brokers. Kestrel re-binds on restart — partition replication
   keeps clients served while each broker cycles.
3. Update client connection strings: REST callers and the Control UI (`Broker:ApiUrl`) from
   `http://broker:9093` to `https://broker:9093`, gRPC callers from `http://broker:9094` to
   `https://broker:9093`. The Kafka and native protocol on port `9092` are unaffected.
4. Remove the Kestrel log-level suppression in your custom `appsettings` if you had one
   layered over ours — the toggle removes it automatically at startup.

## What the toggle does not change

- The **Kafka protocol** on port `9092` stays cleartext unless you also configure SASL/TLS
  at the Kafka level — that's `Surgewave:Kafka:Tls` territory, not `Surgewave:GrpcUseTls`.
- The **inter-broker replication** port is unaffected. See the mTLS / QUIC docs for
  replication TLS.
## Troubleshooting

- **`Connection refused: The request was aborted: Could not create SSL/TLS secure channel.`**
  The client cannot validate the server cert. In development: `dotnet dev-certs https --trust`.
  In production: install the CA chain in the client's trust store, or configure the client
  to skip validation explicitly (only acceptable for smoke-testing).
- **`HTTP/2 over TLS requires ALPN support`**
  Your .NET runtime pre-dates ALPN-over-HTTPS — upgrade to .NET 10 (Surgewave's minimum).
- **`HTTP_1_1_REQUIRED` from a gRPC client**
  The client called the REST endpoint of a broker without TLS, port `9093`, which serves HTTP/1.1.
  gRPC without TLS is on port `9094`.
- **Tests pinning `http://localhost:9093`**
  Flip them to `https://` and add a `SocketsHttpHandler` that trusts the dev cert. Or run
  tests with `Surgewave:GrpcUseTls=false`, which alone yields cleartext.
