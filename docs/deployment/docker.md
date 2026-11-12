# Docker Deployment

Deploy Surgewave using Docker containers.

The broker image is `ghcr.io/kuestenlogik/surgewave-broker` (also `kuestenlogik/surgewave-broker` on
Docker Hub). The Compose files under `deployments/docker/` are the reference; this page explains what
they set and why.

## What the image starts with

The image starts without any configuration:

```bash
docker run -d -p 9092:9092 -p 9093:9093 -v surgewave-data:/app/data ghcr.io/kuestenlogik/surgewave-broker:0.6.0
```

That is one broker, its own quorum, which a client on the Docker host reaches on `localhost:9092`
over the Kafka wire protocol and the native protocol, and on `http://localhost:9093` over gRPC and
REST.

| The image | What it means |
|-----------|---------------|
| holds the Kafka protocol plugin in `/app/plugins` | The broker speaks Kafka and the native protocol on port 9092. `/app/plugins` is what the image ships and is replaced with it; do not mount a volume over it. Plugins you install go to `/app/data/plugins`, which the broker searches after it. |
| runs as uid 1654, the unprivileged user of the .NET images | No `user:` is needed. `/app/data`, `/app/data/raft`, `/app/data/plugins` and `/app/logs` exist in the image and belong to that user, and a fresh named volume mounted on one of them takes that owner. A volume mounted on any other path belongs to root, and a bind-mounted host directory keeps the owner it has on the host: make it writable for uid 1654. |
| sets `Surgewave__GrpcUseTls=false`, `Kestrel__Endpoints__Grpc__Url=http://*:9093`, `Kestrel__Endpoints__GrpcHttp3__Url=http://localhost:9094` and `Kestrel__Endpoints__GrpcHttp3__Protocols=Http1AndHttp2` | gRPC, REST, `/health` and `/metrics` are served in cleartext on port 9093, and the broker says so in a warning at every start. `appsettings.json` binds them over HTTPS with the ASP.NET Core development certificate, which an image does not hold. To serve them over HTTPS, see [TLS in the container image](tls.md#in-the-container-image). These are environment variables of the image, and an environment variable wins over a mounted `appsettings.json`: change them as environment variables. |
| keeps `Surgewave__Host` at `localhost` | The listener binds every interface, and clients are told `localhost:9092`. That suits a client on the Docker host with the port published. |

## What a broker in a network of containers needs

A client in another container is told `localhost` too and then looks for the broker in its own
container. A broker that other containers use says its name:

| Setting | Value | Why |
|---------|-------|-----|
| `Surgewave__Host` | the container's DNS name, e.g. `surgewave-1` | The name clients and the other brokers are told to connect to. |
| `Surgewave__BindAddress` | `0.0.0.0` | The address the client listener binds. Without it the listener binds `Host`, which then has to be an IP address or `localhost`. |

## Single broker

`deployments/docker/docker-compose.yml`:

```bash
docker compose -f deployments/docker/docker-compose.yml up -d --wait surgewave-broker
```

A single broker is its own quorum and is ready alone.

## 3-node cluster

`deployments/docker/docker-compose.cluster.yml`:

```bash
docker compose -f deployments/docker/docker-compose.cluster.yml up -d --wait
```

Every broker carries the same two lists and its own id and name:

```yaml
environment:
  Surgewave__BrokerId: 1
  Surgewave__Host: surgewave-1
  Surgewave__BindAddress: 0.0.0.0
  # Who exists: id:host:port:replicationPort
  Surgewave__ClusterNodes: 1:surgewave-1:9092:10092,2:surgewave-2:9092:10092,3:surgewave-3:9092:10092
  # Who votes on metadata: id@host:replicationPort
  Surgewave__ControllerQuorumVoters: 1@surgewave-1:10092,2@surgewave-2:10092,3@surgewave-3:10092
```

`ClusterNodes` without `ControllerQuorumVoters` is a startup error, and an entry of `ClusterNodes`
without the leading broker id is not read.

### Ready takes a quorum

A broker is ready — `/health/ready` answers 200, the health check passes, clients are answered — only
once it has caught up with the metadata log. That takes a leader, and a leader takes a majority of the
voters:

- one broker of three alone is live (`/health/live`) and not ready;
- as soon as a second one runs, both are ready;
- the third starts, or comes back, at any time.

So the brokers are started together and **no broker depends on another broker's health**
(`depends_on: condition: service_healthy`): the first one would wait for a broker that cannot become
healthy alone. Services that use the cluster may wait for a healthy broker.

A cluster started from nothing takes about half a minute to become ready: every broker first looks
for a peer (`RaftPeerDiscoveryTimeoutSeconds`, 30 s) and binds its HTTP endpoint after that. A single
broker that restarts next to two running ones is ready within seconds.

### Clients

The brokers tell clients their service names (`surgewave-1:9092`, …), which resolve inside the
Compose network. Run clients in that network, or make the names resolvable where the client runs.

### Embedded Connect

The cluster file sets `Surgewave__Connect__Enabled=false`. With the embedded Connect worker on, a
broker that does not lead the partition of the worker's pipeline store topic does not finish
starting and never becomes ready. Run Connect as its own worker against the cluster.

## Health checks

The image holds no `curl`. The broker executable asks its own health endpoint, on the address the
broker is configured with:

```yaml
healthcheck:
  test: ["CMD", "dotnet", "/app/surgewave-broker.dll", "health", "ready"]
  interval: 10s
  timeout: 5s
  retries: 5
  start_period: 30s
```

`health ready` exits 0 when the broker serves clients, `health live` when the process answers at all.

| Endpoint | Answers 200 when |
|----------|------------------|
| `/health/live` | the HTTP endpoint is bound; never fails for want of a quorum |
| `/health/ready` | the broker has caught up with the metadata log |
| `/health` | as `/health/ready`, with the broker id, the Raft state and the leader as JSON |

## Environment variables

Any setting of the `Surgewave` section is an environment variable with `__` between the levels; see
the [configuration reference](../reference/config-reference.md).

| Variable | Default | Description |
|----------|---------|-------------|
| `Surgewave__BrokerId` | 0 | Broker id, unique in the cluster |
| `Surgewave__Host` | localhost | Name or address clients and brokers are told |
| `Surgewave__BindAddress` | - | Address the client listener binds; `Host` when unset |
| `Surgewave__Port` | 9092 | Client port (native protocol, Kafka with the plugin) |
| `Surgewave__GrpcPort` | 9093 | gRPC / REST port |
| `Surgewave__ReplicationPort` | 10092 | Inter-broker port |
| `Surgewave__DataDirectory` | ./data | Data path (`/app/data` in the image) |
| `Surgewave__AutoCreateTopics` | true | Auto-create topics |
| `Surgewave__ClusterNodes` | - | Brokers of the cluster, `id:host:port:replicationPort` |
| `Surgewave__ControllerQuorumVoters` | - | Voters, `id@host:replicationPort` |

## Commands

```bash
# Start and wait until the health checks pass
docker compose -f deployments/docker/docker-compose.cluster.yml up -d --wait

# View logs
docker compose -f deployments/docker/docker-compose.cluster.yml logs -f surgewave-1

# Stop
docker compose -f deployments/docker/docker-compose.cluster.yml down

# Stop and remove volumes
docker compose -f deployments/docker/docker-compose.cluster.yml down -v
```

## Next Steps

- [Kubernetes](kubernetes.md) - Production orchestration
- [Helm](helm.md) - Package management
