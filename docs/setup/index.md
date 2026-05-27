# Installation Guide

Surgewave can be deployed in multiple ways depending on your needs.

## Quick Install

### Docker (Recommended)

```bash
docker run -d --name surgewave \
  -p 9092:9092 \
  -p 9093:9093 \
  -v surgewave-data:/data \
  kuestenlogik/surgewave
```

### Download Binary

Download from [GitHub Releases](https://github.com/Kuestenlogik/Surgewave/releases):

| Platform | Download |
|----------|----------|
| Windows x64 | `surgewave-win-x64.zip` |
| Linux x64 | `surgewave-linux-x64.tar.gz` |
| macOS x64 | `surgewave-osx-x64.tar.gz` |
| macOS ARM | `surgewave-osx-arm64.tar.gz` |

```bash
# Extract and run
./surgewave-broker
```

### .NET Tool

```bash
dotnet tool install -g Kuestenlogik.Surgewave.Cli
surgewave broker start
```

---

## Deployment Options

| Option | Best For | Guide |
|--------|----------|-------|
| **Docker** | Quick start, development, CI/CD | [Docker Guide](../deployment/docker.md) |
| **Kubernetes** | Production clusters | [Kubernetes Guide](../deployment/kubernetes.md) |
| **Helm** | Kubernetes with config management | [Helm Guide](../deployment/helm.md) |
| **Standalone** | Single-node, VMs | [Standalone Guide](standalone.md) |
| **Embedded** | Testing, microservices | [Embedded Guide](embedded.md) |

---

## System Requirements

| Component | Minimum | Recommended (Production) |
|-----------|---------|--------------------------|
| CPU | 2 cores | 8+ cores |
| Memory | 2 GB | 16+ GB |
| Disk | 10 GB | 500+ GB NVMe SSD |
| Network | 1 Gbps | 10 Gbps |

### Supported Platforms

- **Linux**: Ubuntu 20.04+, Debian 11+, RHEL 8+, Alpine 3.16+
- **Windows**: Windows Server 2019+, Windows 10/11
- **macOS**: macOS 12+ (Intel and Apple Silicon)
- **Container**: Docker, Kubernetes, Podman

---

## Ports

| Port | Protocol | Description |
|------|----------|-------------|
| 9092 | TCP | Kafka protocol (clients) |
| 9093 | TCP | gRPC API |
| 10092 | TCP | Cluster replication |

---

## Next Steps

- [Configuration](configuration.md) - Configure Surgewave for your environment
- [Storage Backends](../storage/index.md) - Choose your storage
- [Clustering](../clustering/index.md) - Multi-broker setup
- [Security](../security/index.md) - Enable authentication
