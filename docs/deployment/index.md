# Deployment Overview

Production deployment options for Surgewave.

## Options

| Option | Complexity | Best For |
|--------|------------|----------|
| [Docker](docker.md) | Low | Single host, dev |
| [Kubernetes](kubernetes.md) | Medium | Production clusters |
| [Helm](helm.md) | Medium | K8s with config management |

## Quick Reference

### Docker

```bash
docker run -d -p 9092:9092 -p 9093:9093 kuestenlogik/surgewave
```

### Kubernetes

```bash
kubectl apply -f https://raw.githubusercontent.com/Kuestenlogik/Surgewave/main/deploy/kubernetes/
```

### Helm

```bash
helm install surgewave ./deploy/helm/surgewave
```

## Production Checklist

- [ ] 3+ broker nodes
- [ ] Replication factor ≥ 3
- [ ] min.insync.replicas = 2
- [ ] TLS enabled
- [ ] SASL authentication
- [ ] ACLs configured
- [ ] Persistent storage
- [ ] Resource limits
- [ ] Health checks
- [ ] Monitoring

## Next Steps

- [Docker](docker.md) - Container deployment
- [Kubernetes](kubernetes.md) - Orchestrated deployment
- [Helm](helm.md) - Package management
