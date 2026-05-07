# Helm Chart

Deploy Surgewave using Helm package manager.

## Installation

```bash
# Add repository (future)
helm repo add surgewave https://charts.surgewave.io
helm install surgewave surgewave/surgewave

# From local chart
helm install surgewave ./deploy/helm/surgewave
```

## Configuration

### values.yaml

```yaml
replicaCount: 3

image:
  repository: kuestenlogik/surgewave
  tag: latest
  pullPolicy: IfNotPresent

service:
  type: ClusterIP
  kafka:
    port: 9092
  grpc:
    port: 9093
  external:
    enabled: false
    type: LoadBalancer

persistence:
  enabled: true
  storageClass: fast-ssd
  size: 100Gi

resources:
  requests:
    memory: 2Gi
    cpu: 1
  limits:
    memory: 4Gi
    cpu: 2

surgewave:
  storageMode: File
  autoCreateTopics: true
  defaultNumPartitions: 6
  defaultReplicationFactor: 3
  minInSyncReplicas: 2
  useRaftConsensus: true

security:
  enabled: false
  sasl:
    enabled: false
    mechanism: SCRAM-SHA-256
  tls:
    enabled: false

metrics:
  enabled: true
  serviceMonitor:
    enabled: false
    interval: 30s

podDisruptionBudget:
  enabled: true
  minAvailable: 2
```

## Custom Values

### Production

```yaml
# prod-values.yaml
replicaCount: 5

resources:
  requests:
    memory: 8Gi
    cpu: 4
  limits:
    memory: 16Gi
    cpu: 8

persistence:
  size: 500Gi
  storageClass: fast-ssd

surgewave:
  defaultReplicationFactor: 3
  minInSyncReplicas: 2

security:
  enabled: true
  sasl:
    enabled: true
    mechanism: SCRAM-SHA-256
  tls:
    enabled: true

metrics:
  enabled: true
  serviceMonitor:
    enabled: true
```

```bash
helm install surgewave ./deploy/helm/surgewave -f prod-values.yaml
```

### Development

```yaml
# dev-values.yaml
replicaCount: 1

resources:
  requests:
    memory: 512Mi
    cpu: 250m

persistence:
  size: 10Gi

surgewave:
  defaultReplicationFactor: 1
```

## Upgrade

```bash
# Upgrade with new values
helm upgrade surgewave ./deploy/helm/surgewave -f prod-values.yaml

# Upgrade to new version
helm upgrade surgewave ./deploy/helm/surgewave --set image.tag=v2.0.0
```

## Common Operations

### Scale

```bash
helm upgrade surgewave ./deploy/helm/surgewave --set replicaCount=5
```

### Enable Security

```bash
helm upgrade surgewave ./deploy/helm/surgewave \
    --set security.enabled=true \
    --set security.sasl.enabled=true
```

### Enable Monitoring

```bash
helm upgrade surgewave ./deploy/helm/surgewave \
    --set metrics.enabled=true \
    --set metrics.serviceMonitor.enabled=true
```

## Uninstall

```bash
helm uninstall surgewave

# Keep PVCs
helm uninstall surgewave --keep-history

# Delete PVCs
kubectl delete pvc -l app.kubernetes.io/name=surgewave
```

## Chart Structure

```
surgewave/
├── Chart.yaml          # Chart metadata
├── values.yaml         # Default values
├── templates/
│   ├── _helpers.tpl    # Template helpers
│   ├── statefulset.yaml
│   ├── service.yaml
│   ├── pdb.yaml
│   └── servicemonitor.yaml
└── README.md
```

## Next Steps

- [Monitoring](../monitoring/index.md) - Observability
- [Security](../security/index.md) - Enable security
