# Docker Deployment

Deploy Surgewave using Docker containers.

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

```yaml
version: '3.8'
services:
  surgewave:
    image: kuestenlogik/surgewave:latest
    ports:
      - "9092:9092"
      - "9093:9093"
    environment:
      - Surgewave__BrokerId=1
      - Surgewave__AutoCreateTopics=true
    volumes:
      - surgewave-data:/data
    restart: unless-stopped

volumes:
  surgewave-data:
```

### 3-Node Cluster

```yaml
version: '3.8'
services:
  surgewave-1:
    image: kuestenlogik/surgewave:latest
    hostname: surgewave-1
    ports:
      - "9092:9092"
    environment:
      - Surgewave__BrokerId=1
      - Surgewave__Host=surgewave-1
      - Surgewave__ClusterNodes=surgewave-1:9092,surgewave-2:9092,surgewave-3:9092
      - Surgewave__UseRaftConsensus=true
    volumes:
      - surgewave-1-data:/data
    networks:
      - surgewave-net

  surgewave-2:
    image: kuestenlogik/surgewave:latest
    hostname: surgewave-2
    ports:
      - "9192:9092"
    environment:
      - Surgewave__BrokerId=2
      - Surgewave__Host=surgewave-2
      - Surgewave__ClusterNodes=surgewave-1:9092,surgewave-2:9092,surgewave-3:9092
      - Surgewave__UseRaftConsensus=true
    volumes:
      - surgewave-2-data:/data
    networks:
      - surgewave-net

  surgewave-3:
    image: kuestenlogik/surgewave:latest
    hostname: surgewave-3
    ports:
      - "9292:9092"
    environment:
      - Surgewave__BrokerId=3
      - Surgewave__Host=surgewave-3
      - Surgewave__ClusterNodes=surgewave-1:9092,surgewave-2:9092,surgewave-3:9092
      - Surgewave__UseRaftConsensus=true
    volumes:
      - surgewave-3-data:/data
    networks:
      - surgewave-net

networks:
  surgewave-net:

volumes:
  surgewave-1-data:
  surgewave-2-data:
  surgewave-3-data:
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `Surgewave__BrokerId` | 1 | Broker ID |
| `Surgewave__Host` | localhost | Bind address |
| `Surgewave__Port` | 9092 | Kafka port |
| `Surgewave__GrpcPort` | 9093 | gRPC port |
| `Surgewave__DataDirectory` | /data | Data path |
| `Surgewave__StorageMode` | File | Storage backend |
| `Surgewave__AutoCreateTopics` | true | Auto-create |
| `Surgewave__ClusterNodes` | - | Cluster nodes |
| `Surgewave__UseRaftConsensus` | false | Enable Raft |

## Volumes

| Path | Purpose |
|------|---------|
| `/data` | Topic data |
| `/logs` | Log files |
| `/config` | Configuration |

## Health Checks

```yaml
services:
  surgewave:
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
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G
```

## Commands

```bash
# Start
docker-compose up -d

# View logs
docker-compose logs -f surgewave

# Stop
docker-compose down

# Stop and remove volumes
docker-compose down -v
```

## Next Steps

- [Kubernetes](kubernetes.md) - Production orchestration
- [Helm](helm.md) - Package management
