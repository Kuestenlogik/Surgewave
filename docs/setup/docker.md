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

## Docker Compose

### Single Broker

```yaml
services:
  surgewave-broker:
    image: ghcr.io/kuestenlogik/surgewave-broker:0.6.0
    # The image runs as an unprivileged user that cannot write to a fresh named volume.
    user: "0:0"
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      # The name clients are told; the listener binds every interface.
      - Surgewave__Host=surgewave-broker
      - Surgewave__BindAddress=0.0.0.0
      # gRPC, REST, /health and /metrics in cleartext; the image binds them over HTTPS,
      # which needs a certificate.
      - Surgewave__GrpcUseTls=false
      - Kestrel__Endpoints__Grpc__Url=http://*:9093
      - Kestrel__Endpoints__GrpcHttp3__Url=http://localhost:9094
      - Kestrel__Endpoints__GrpcHttp3__Protocols=Http1AndHttp2
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

## Building Custom Image

### Dockerfile

```dockerfile
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
EXPOSE 9092 9093

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish src/Kuestenlogik.Surgewave.Broker.App/Kuestenlogik.Surgewave.Broker.App.csproj \
    -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Kuestenlogik.Surgewave.Broker.dll"]
```

```bash
docker build -t my-surgewave .
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `Surgewave__BrokerId` | Unique broker ID | 0 |
| `Surgewave__Host` | Name or address clients and brokers are told | localhost |
| `Surgewave__BindAddress` | Address the client listener binds; `Host` when unset | - |
| `Surgewave__Port` | Client port | 9092 |
| `Surgewave__GrpcPort` | gRPC / REST port | 9093 |
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
| `/app/plugins` | Installed plugins |

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
