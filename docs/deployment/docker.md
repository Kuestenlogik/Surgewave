# Docker Deployment

Deploy Surgewave using Docker containers.

The broker image is `ghcr.io/kuestenlogik/surgewave-broker` (also `kuestenlogik/surgewave-broker` on
Docker Hub). The Compose files under `deployments/docker/` are the reference; this page explains what
they set and why.

## What a broker in a container needs

The image's `appsettings.json` is written for a workstation. Four things have to be said for a
container:

| Setting | Value in a container | Why |
|---------|----------------------|-----|
| `Surgewave__Host` | the container's DNS name, e.g. `surgewave-1` | The name clients and the other brokers are told to connect to. |
| `Surgewave__BindAddress` | `0.0.0.0` | The address the client listener binds. Without it the listener binds `Host`, which then has to be an IP address or `localhost`. |
| `Surgewave__GrpcUseTls`, `Kestrel__Endpoints__Grpc__Url`, `Kestrel__Endpoints__GrpcHttp3__Url`, `Kestrel__Endpoints__GrpcHttp3__Protocols` | `false`, `http://*:9093`, `http://localhost:9094`, `Http1AndHttp2` | The image binds gRPC, REST, `/health` and `/metrics` over HTTPS, which needs a certificate. These bind them in cleartext; see [TLS](tls.md) to keep HTTPS. |
| a writable data directory | a volume on `/app/data`, and `user: "0:0"` for a named volume | The image runs as an unprivileged user, and a fresh named volume belongs to root. |

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
