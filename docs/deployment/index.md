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
docker compose -f deployments/docker/docker-compose.cluster.yml up -d --wait
```

### Kubernetes

```bash
kubectl apply -f deployments/kubernetes/surgewave-configmap.yaml \
  -f deployments/kubernetes/surgewave-service.yaml \
  -f deployments/kubernetes/surgewave-statefulset.yaml
```

### Helm

```bash
helm install surgewave ./deployments/helm/surgewave
```

## A broker is ready only with a quorum

A broker reports ready only once it has caught up with the metadata log, which takes a majority of
the brokers. Whatever starts the brokers must start them together and must not wait for one to be
ready before it starts the next; see [Kubernetes](kubernetes.md) and [Docker](docker.md).

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
