# Docker Setup

Run Surgewave in Docker containers for easy deployment and isolation.

## Quick Start

```bash
docker run -d \
  --name surgewave \
  -p 9092:9092 \
  -p 9093:9093 \
  kuestenlogik/surgewave
```

## Docker Compose

### Single Broker

Create `docker-compose.yml`:

```yaml
version: '3.8'
services:
  surgewave:
    image: kuestenlogik/surgewave:latest
    container_name: surgewave
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      - Surgewave__BrokerId=1
      - Surgewave__Host=0.0.0.0
      - Surgewave__AutoCreateTopics=true
    volumes:
      - surgewave-data:/data
    restart: unless-stopped

volumes:
  surgewave-data:
```

```bash
docker-compose up -d
```

### Multi-Broker Cluster

```yaml
version: '3.8'
services:
  surgewave-1:
    image: kuestenlogik/surgewave:latest
    container_name: surgewave-1
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      - Surgewave__BrokerId=1
      - Surgewave__Host=surgewave-1
      - Surgewave__ClusterNodes=surgewave-1:9092,surgewave-2:9092,surgewave-3:9092
      - Surgewave__UseRaftConsensus=true
    volumes:
      - surgewave-1-data:/data
    networks:
      - surgewave-network

  surgewave-2:
    image: kuestenlogik/surgewave:latest
    container_name: surgewave-2
    ports:
      - "9192:9092"
      - "9193:9093"
    environment:
      - Surgewave__BrokerId=2
      - Surgewave__Host=surgewave-2
      - Surgewave__ClusterNodes=surgewave-1:9092,surgewave-2:9092,surgewave-3:9092
      - Surgewave__UseRaftConsensus=true
    volumes:
      - surgewave-2-data:/data
    networks:
      - surgewave-network

  surgewave-3:
    image: kuestenlogik/surgewave:latest
    container_name: surgewave-3
    ports:
      - "9292:9092"
      - "9293:9093"
    environment:
      - Surgewave__BrokerId=3
      - Surgewave__Host=surgewave-3
      - Surgewave__ClusterNodes=surgewave-1:9092,surgewave-2:9092,surgewave-3:9092
      - Surgewave__UseRaftConsensus=true
    volumes:
      - surgewave-3-data:/data
    networks:
      - surgewave-network

networks:
  surgewave-network:
    driver: bridge

volumes:
  surgewave-1-data:
  surgewave-2-data:
  surgewave-3-data:
```

## Building Custom Image

### Dockerfile

```dockerfile
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
WORKDIR /app
EXPOSE 9092 9093

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet publish src/Kuestenlogik.Surgewave.Broker/Kuestenlogik.Surgewave.Broker.csproj \
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
| `Surgewave__BrokerId` | Unique broker ID | 1 |
| `Surgewave__Host` | Bind address | 0.0.0.0 |
| `Surgewave__Port` | Kafka protocol port | 9092 |
| `Surgewave__GrpcPort` | gRPC API port | 9093 |
| `Surgewave__DataDirectory` | Data storage path | /data |
| `Surgewave__StorageMode` | Storage backend | File |
| `Surgewave__AutoCreateTopics` | Auto-create topics | true |
| `Surgewave__ClusterNodes` | Cluster endpoints | - |

## Volume Mounts

| Path | Purpose |
|------|---------|
| `/data` | Topic data and offsets |
| `/logs` | Log files |
| `/config` | Custom configuration |

## Health Checks

Add health check to docker-compose:

```yaml
services:
  surgewave:
    # ...
    healthcheck:
      test: ["CMD", "curl", "-f", "https://localhost:9093/health"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 10s
```

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
