# Docker Deployment

Deploy Surgewave using Docker containers.

The broker image is `ghcr.io/kuestenlogik/surgewave-broker` (also `kuestenlogik/surgewave-broker` on
Docker Hub). The Compose files under `deployments/docker/` are the reference; this page explains what
they set and why.

## What the image starts with

The image starts without any configuration:

```bash
docker run -d -p 9092:9092 -p 9093:9093 -p 9094:9094 -v surgewave-data:/app/data ghcr.io/kuestenlogik/surgewave-broker:0.6.0
```

That is one broker, its own quorum, which a client on the Docker host reaches on `localhost:9092`
over the Kafka wire protocol and the native protocol, on `http://localhost:9093` over REST and on
`http://localhost:9094` over gRPC.

| The image | What it means |
|-----------|---------------|
| holds the Kafka protocol plugin in `/app/plugins` | The broker speaks Kafka and the native protocol on port 9092. `/app/plugins` is what the image ships and is replaced with it; do not mount a volume over it. Plugins you install go to `/app/data/plugins`, which the broker searches after it. |
| runs as uid 1654, the unprivileged user of the .NET images | No `user:` is needed. `/app/data`, `/app/data/raft`, `/app/data/plugins` and `/app/logs` exist in the image and belong to that user, and a fresh named volume mounted on one of them takes that owner. A volume mounted on any other path belongs to root, and a bind-mounted host directory keeps the owner it has on the host: make it writable for uid 1654. A volume an earlier deployment wrote as root is given to that user once, see [A volume written as root](#a-volume-written-as-root). |
| sets nothing about TLS | `appsettings.json` switches TLS off (`Surgewave:GrpcUseTls`), which an image without a certificate needs: REST, `/health` and `/metrics` are served in cleartext on port 9093 and gRPC on port 9094, and the broker says so in a warning at every start. To serve them over HTTPS, set `Surgewave__GrpcUseTls=true` and a certificate, as environment variables or in a mounted `appsettings.Production.json`: see [TLS in the container image](tls.md#in-the-container-image). |
| keeps `Surgewave__Host` at `localhost` | The listener binds every interface, and clients are told `localhost:9092`. That suits a client on the Docker host with the port published. |

## A volume written as root

Before 0.6.0 the Compose files ran the broker as root (`user: "0:0"`), and a named volume written
that way belongs to root. The image's user cannot write to it, and the broker ends its start with

```
The data directory '/app/data' is not writable for user 'app': Access to the path '/app/data/.write-probe-…' is denied. …
```

Give the volume to uid 1654 once, with the broker stopped. The image holds `chown`:

```bash
docker run --rm --user 0 --entrypoint chown -v surgewave-data:/app/data \
  ghcr.io/kuestenlogik/surgewave-broker:0.6.0 -R 1654 /app/data
```

A volume mounted on `/app/logs` is given the same way.

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

A cluster started from nothing is ready within seconds of the second broker's start, and so is a
single broker that restarts next to two running ones. A broker's HTTP endpoint, `/health/live`
included, is bound a few seconds after its process starts, whether or not another broker runs.

### Clients

The brokers tell clients their service names (`surgewave-1:9092`, …), which resolve inside the
Compose network. Run clients in that network, or make the names resolvable where the client runs.

### Embedded Connect

Every broker of the cluster file runs the embedded Connect worker, as the image's
`appsettings.json` has it. The worker is a client of its own broker and starts once that broker
accepts clients; until it has, `/api/pipelines` and the connector API answer 503. The broker's
readiness does not wait for it.

Each worker reads and writes its stores through its own broker: the connector configs
(`surgewave-connect-configs`) and the pipeline definitions (`_surgewave-pipelines`), one partition
with one replica each. Only the worker on the broker that leads those partitions has them. On
the other brokers the worker starts without the stored connectors and pipelines (it logs
"starting fresh"), lists none and answers a change to a pipeline with 500. Use the Connect and
pipeline API of the broker that leads the two partitions, or set
`Surgewave__Connect__Enabled=false` and run Connect as its own worker against the cluster.

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
