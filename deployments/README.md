# Surgewave Deployment

Deployment configurations for Surgewave in production environments.

## Contents

### Helm Chart

| Path | Description |
|------|-------------|
| `helm/surgewave/` | Helm chart for Surgewave deployment |
| `helm/surgewave/Chart.yaml` | Chart metadata |
| `helm/surgewave/values.yaml` | Default configuration values |
| `helm/surgewave/templates/` | Kubernetes resource templates |
| `helm/surgewave/README.md` | Chart documentation with examples |

### Kubernetes Manifests

| Path | Description |
|------|-------------|
| `kubernetes/surgewave-statefulset.yaml` | StatefulSet for three Surgewave brokers, PodDisruptionBudget |
| `kubernetes/surgewave-service.yaml` | Kubernetes Service definitions |
| `kubernetes/surgewave-configmap.yaml` | Configuration ConfigMap |
| `kubernetes/operator/` | Surgewave Kubernetes Operator CRDs and RBAC |

### Docker Compose

| Path | Description |
|------|-------------|
| `docker/docker-compose.yml` | Single broker with gateway, Connect worker, Control UI and marketplace |
| `docker/docker-compose.cluster.yml` | Three brokers; Prometheus and Grafana under the `monitoring` profile |
| `docker/docker-compose.kafka.yml` | Apache Kafka, for benchmark comparison |

### Monitoring

| Path | Description |
|------|-------------|
| `monitoring/grafana-surgewave-dashboard.json` | Grafana dashboard |
| `monitoring/prometheus-alerts.yaml` | Prometheus alerting rules |

### Scripts

| Path | Description |
|------|-------------|
| `scripts/install.sh` | Install Surgewave via Helm |
| `scripts/upgrade.sh` | Upgrade an existing release |
| `scripts/uninstall.sh` | Uninstall Surgewave (with optional PVC cleanup) |

## Quick Start

### Helm (Recommended)

```bash
# Install a 3-broker cluster
helm install surgewave deployments/helm/surgewave \
  --namespace surgewave \
  --create-namespace

# Custom values
helm install surgewave deployments/helm/surgewave -f custom-values.yaml

# Upgrade
helm upgrade surgewave deployments/helm/surgewave

# Or use the convenience scripts
./deployments/scripts/install.sh --replicas 3 --namespace surgewave
./deployments/scripts/uninstall.sh --delete-pvcs
```

### Kubernetes (Direct)

```bash
# Three brokers, in any namespace
kubectl apply -f kubernetes/surgewave-configmap.yaml
kubectl apply -f kubernetes/surgewave-service.yaml
kubectl apply -f kubernetes/surgewave-statefulset.yaml
kubectl rollout status statefulset/surgewave
```

### Docker Compose

```bash
# Single broker
docker compose -f docker/docker-compose.yml up -d --wait surgewave-broker

# Three brokers
docker compose -f docker/docker-compose.cluster.yml up -d --wait
```

### A broker is ready only with a quorum

A broker reports ready (`/health/ready`, the Compose health check, the readiness probe) only once it
has caught up with the metadata log, which takes a majority of the brokers. All three deployments
therefore start the brokers together and let none of them wait for another one's readiness: the
StatefulSets use `podManagementPolicy: Parallel` and a headless service with
`publishNotReadyAddresses: true`, liveness is `/health/live`, which never fails for want of a
quorum, and no Compose broker depends on another broker's health. `scripts/ci/compose-cluster-smoke.sh`
and `scripts/ci/kubernetes-cluster-smoke.sh` bring the deployments up and check them;
`.github/workflows/deployments.yml` runs both, and `scripts/ci/image-smoke.sh`, which starts the broker
image with no configuration on a fresh named volume and produces and consumes a record over the Kafka
wire protocol and over the native protocol.

### Kubernetes Operator

```bash
# Install CRDs and operator
kubectl apply -k kubernetes/operator/

# Create a Surgewave cluster
kubectl apply -f kubernetes/operator/example-cluster.yaml

# Check cluster status
kubectl get surgewaveclusters
kubectl describe surgewavecluster my-surgewave-cluster
```

## Helm Chart Overview

The Helm chart deploys:

- **Broker StatefulSet** with PersistentVolumeClaims, anti-affinity rules, and health probes
- **Headless Service** for stable pod DNS and inter-broker communication
- **Client Service** (ClusterIP) for Kafka/gRPC access
- **Control UI Deployment** (optional) with Service and Ingress
- **ConfigMap** with all broker configuration mapped to environment variables
- **ServiceAccount + RBAC** for pod-level permissions
- **PodDisruptionBudget** for safe rolling upgrades
- **ServiceMonitor** (optional) for Prometheus scraping

### Key Configuration

```yaml
broker:
  replicaCount: 3          # Number of broker replicas
  storage:
    size: 10Gi             # PVC size per broker
  resources:
    limits:
      memory: 4Gi
      cpu: 2

control:
  enabled: true            # Deploy Control UI
  ingress:
    enabled: false         # Optional Ingress

features:
  mqtt:
    enabled: false         # MQTT protocol support
  graphql:
    enabled: false         # GraphQL API
  schemaRegistry:
    enabled: true          # Schema Registry

monitoring:
  serviceMonitor:
    enabled: false         # Prometheus ServiceMonitor
```

## Kubernetes Operator

The operator watches for `SurgewaveCluster` custom resources and manages:

- Broker StatefulSet creation and scaling
- Headless and client Service lifecycle
- ConfigMap generation from spec
- Control UI Deployment (when `controlUiEnabled: true`)
- Status reporting (phase, ready brokers, bootstrap servers)
- Serverless auto-scaling with scale-to-zero support

```yaml
apiVersion: surgewave.kl.io/v1
kind: SurgewaveCluster
metadata:
  name: my-surgewave-cluster
spec:
  brokers: 3
  image: surgewave:latest
  controlUiEnabled: true
  storage:
    size: 50Gi
  resources:
    cpuRequest: "1"
    memoryRequest: "2Gi"
  monitoring:
    enabled: true
    serviceMonitor: true
```

## Monitoring

### Prometheus

Surgewave exposes metrics at `/metrics` endpoint. Enable ServiceMonitor:

```yaml
monitoring:
  serviceMonitor:
    enabled: true
    interval: 30s
```

### Grafana

Import `monitoring/grafana-surgewave-dashboard.json` to visualize:
- Throughput (messages/sec)
- Latency percentiles (P50/P90/P99)
- Storage utilization
- Consumer lag
- Cluster health

### Alerts

Apply Prometheus alerting rules:
```bash
kubectl apply -f monitoring/prometheus-alerts.yaml
```

## Production Recommendations

1. **Storage**: Use SSD-backed persistent volumes (`storageClassName: ssd`)
2. **Memory**: Allocate 4-8GB per broker
3. **Replicas**: Minimum 3 brokers for high availability
4. **Anti-affinity**: Use `hard` anti-affinity to spread across nodes
5. **PDB**: Keep PodDisruptionBudget enabled (`maxUnavailable: 1`)
6. **Monitoring**: Enable Prometheus metrics and ServiceMonitor
7. **TLS**: Enable TLS for production deployments
8. **Resources**: Set both requests and limits to avoid noisy-neighbor issues
