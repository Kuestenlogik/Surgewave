# Docker Setup

Run Surgewave in Docker containers for easy deployment and isolation.

## Quick Start

The broker image is `ghcr.io/kuestenlogik/surgewave-broker` (also `kuestenlogik/surgewave-broker` on
Docker Hub). The Compose files in the repository are the quickest way to a running broker:

```bash
# Single broker
docker compose -f deployments/docker/docker-compose.yml up -d --wait surgewave-broker

# Three brokers
docker compose -f deployments/docker/docker-compose.cluster.yml up -d --wait
```

The image also starts on its own, without any configuration: one broker that speaks the Kafka wire
protocol and the native protocol on port 9092, and in cleartext REST on port 9093 and gRPC on port
9094.

```bash
docker run -d -p 9092:9092 -p 9093:9093 -p 9094:9094 -v surgewave-data:/app/data ghcr.io/kuestenlogik/surgewave-broker:0.6.0
```

## Docker Compose

### Single Broker

```yaml
services:
  surgewave-broker:
    image: ghcr.io/kuestenlogik/surgewave-broker:0.6.0
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      # The name clients in this network are told; the listener binds every interface.
      - Surgewave__Host=surgewave-broker
      - Surgewave__BindAddress=0.0.0.0
    volumes:
      - surgewave-data:/app/data
    healthcheck:
      test: ["CMD", "dotnet", "/app/surgewave-broker.dll", "health", "ready"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s
    restart: unless-stopped

volumes:
  surgewave-data:
```

```bash
docker compose up -d --wait
```

### Multi-Broker Cluster

Use `deployments/docker/docker-compose.cluster.yml`. Every broker carries its own id and name and
the same two lists — who exists and who votes on metadata:

```yaml
environment:
  Surgewave__BrokerId: 1
  Surgewave__Host: surgewave-1
  Surgewave__BindAddress: 0.0.0.0
  Surgewave__ClusterNodes: 1:surgewave-1:9092:10092,2:surgewave-2:9092:10092,3:surgewave-3:9092:10092
  Surgewave__ControllerQuorumVoters: 1@surgewave-1:10092,2@surgewave-2:10092,3@surgewave-3:10092
```

A broker is ready only once it has a quorum, so no broker may wait for another broker's health.
[Docker Deployment](../deployment/docker.md) explains the settings and the start order.

## Building the Image from Source

The images are built by the .NET SDK, without a Dockerfile. From a clone of the repository, with the
.NET 10 SDK and a running Docker daemon:

```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker.App -c Release /t:PublishContainer \
  -p:ContainerRepository=my-surgewave -p:ContainerImageTags=dev
```

That loads `my-surgewave:dev` into the local Docker daemon, for the architecture the SDK picks for
the build machine: the image a release publishes, with the Kafka protocol plugin in `/app/plugins`, the
data directories owned by its user and `dotnet /app/surgewave-broker.dll` as its entry point.

`scripts/ci/build-images.sh` builds the broker and the Control image for linux-x64 and linux-arm64 in
one image index each, as a release does; the local Docker daemon needs the containerd image store to
hold such an image.

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `Surgewave__BrokerId` | Unique broker ID | 0 |
| `Surgewave__Host` | Name or address clients and brokers are told | localhost |
| `Surgewave__BindAddress` | Address the client listener binds; `Host` when unset | - |
| `Surgewave__Port` | Client port | 9092 |
| `Surgewave__GrpcPort` | REST port, and the gRPC port with TLS | 9093 |
| `Surgewave__GrpcUseTls` | Serve gRPC and REST over HTTPS; see [TLS](../deployment/tls.md) | false |
| `Surgewave__ReplicationPort` | Inter-broker port | 10092 |
| `Surgewave__DataDirectory` | Data storage path | ./data (`/app/data` in the image) |
| `Surgewave__AutoCreateTopics` | Auto-create topics | true |
| `Surgewave__ClusterNodes` | Brokers of the cluster, `id:host:port:replicationPort` | - |
| `Surgewave__ControllerQuorumVoters` | Voters, `id@host:replicationPort` | - |

## Volume Mounts

| Path | Purpose |
|------|---------|
| `/app/data` | Topic data, offsets and the metadata log |
| `/app/logs` | Log files |
| `/app/data/plugins` | Installed plugins; part of the data volume, or a volume of its own |

The image runs as uid 1654. These directories exist in the image and belong to that user, so a
fresh named volume mounted on one of them is writable; a bind-mounted host directory has to be
writable for uid 1654. `/app/plugins` holds what the image ships, the Kafka protocol plugin: do
not mount a volume over it.

## Health Checks

The image holds no `curl`; the broker executable asks its own health endpoint:

```yaml
services:
  surgewave-broker:
    # ...
    healthcheck:
      test: ["CMD", "dotnet", "/app/surgewave-broker.dll", "health", "ready"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s
```

`/health/ready` answers 200 once the broker has caught up with the metadata log, which in a cluster
takes a majority of the brokers; `/health/live` answers as soon as the HTTP endpoint is bound.

## Resource Limits

```yaml
services:
  surgewave:
    # ...
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G
```

## Logging

```yaml
services:
  surgewave:
    # ...
    logging:
      driver: "json-file"
      options:
        max-size: "100m"
        max-file: "5"
```

## Next Steps

- [Kubernetes Deployment](../deployment/kubernetes.md) - Production orchestration
- [Helm Chart](../deployment/helm.md) - Kubernetes package management
- [Configuration](configuration.md) - All configuration options
