# Surgewave Helm Chart

Deploy Kuestenlogik.Surgewave -- a high-performance, Kafka-compatible event streaming platform -- on Kubernetes.

## Quick Start

```bash
# Install a 3-broker cluster
helm install surgewave deploy/helm/surgewave/ \
  --namespace surgewave \
  --create-namespace

# Or use the install script
./deploy/scripts/install.sh --replicas 3 --namespace surgewave
```

## Configuration

All configuration is done via `values.yaml` or `--set` flags.

### Broker

| Parameter | Description | Default |
|-----------|-------------|---------|
| `broker.replicaCount` | Number of broker replicas | `3` |
| `broker.image.repository` | Broker image repository | `surgewave` |
| `broker.image.tag` | Broker image tag | `latest` |
| `broker.resources.requests.cpu` | CPU request | `500m` |
| `broker.resources.requests.memory` | Memory request | `1Gi` |
| `broker.resources.limits.cpu` | CPU limit | `2` |
| `broker.resources.limits.memory` | Memory limit | `4Gi` |
| `broker.storage.size` | PVC size per broker | `10Gi` |
| `broker.storage.storageClassName` | Storage class (empty = default) | `""` |
| `broker.ports.kafka` | Kafka protocol port | `9092` |
| `broker.ports.grpc` | gRPC API port | `9093` |
| `broker.ports.mqtt` | MQTT protocol port | `1883` |
| `broker.config.autoTopicCreation` | Auto-create topics | `true` |
| `broker.config.defaultPartitions` | Default partitions per topic | `3` |
| `broker.config.defaultReplicationFactor` | Default replication factor | `3` |
| `broker.config.logRetentionHours` | Log retention (hours) | `168` |

### Control UI

| Parameter | Description | Default |
|-----------|-------------|---------|
| `control.enabled` | Deploy the Control UI | `true` |
| `control.replicaCount` | Control UI replicas | `1` |
| `control.port` | Control UI HTTP port | `5050` |
| `control.ingress.enabled` | Create Ingress for Control UI | `false` |
| `control.ingress.hostname` | Ingress hostname | `surgewave.local` |

### Features

| Parameter | Description | Default |
|-----------|-------------|---------|
| `schemaRegistry.enabled` | Enable Schema Registry | `true` |
| `connect.enabled` | Enable Kafka Connect | `false` |
| `features.mqtt.enabled` | Enable MQTT protocol | `false` |
| `features.graphql.enabled` | Enable GraphQL API | `false` |
| `features.dataMesh.enabled` | Enable Data Mesh | `false` |
| `features.multiTenancy.enabled` | Enable Multi-Tenancy | `false` |

### Security

| Parameter | Description | Default |
|-----------|-------------|---------|
| `security.tls.enabled` | Enable TLS | `false` |
| `security.tls.certSecret` | TLS certificate Secret name | `""` |
| `security.sasl.enabled` | Enable SASL authentication | `false` |
| `security.rbac.create` | Create RBAC resources | `true` |

### Monitoring

| Parameter | Description | Default |
|-----------|-------------|---------|
| `monitoring.enabled` | Enable metrics endpoint | `true` |
| `monitoring.serviceMonitor.enabled` | Create Prometheus ServiceMonitor | `false` |
| `monitoring.serviceMonitor.interval` | Scrape interval | `30s` |

## Examples

### Production Cluster

```bash
helm install surgewave deploy/helm/surgewave/ \
  --namespace surgewave \
  --create-namespace \
  --set broker.replicaCount=5 \
  --set broker.resources.requests.cpu=2 \
  --set broker.resources.requests.memory=4Gi \
  --set broker.resources.limits.cpu=4 \
  --set broker.resources.limits.memory=8Gi \
  --set broker.storage.size=100Gi \
  --set antiAffinity=hard \
  --set monitoring.serviceMonitor.enabled=true
```

### With Custom Values File

```yaml
# production-values.yaml
broker:
  replicaCount: 5
  resources:
    requests:
      cpu: "2"
      memory: "4Gi"
    limits:
      cpu: "4"
      memory: "8Gi"
  storage:
    size: 100Gi
    storageClassName: ssd

control:
  ingress:
    enabled: true
    hostname: surgewave.example.com
    annotations:
      cert-manager.io/cluster-issuer: letsencrypt-prod
    tls:
      - secretName: surgewave-control-tls
        hosts:
          - surgewave.example.com

monitoring:
  serviceMonitor:
    enabled: true

security:
  tls:
    enabled: true
    certSecret: surgewave-tls
```

```bash
helm install surgewave deploy/helm/surgewave/ -f production-values.yaml --namespace surgewave --create-namespace
```

### Development (Single Broker)

```bash
helm install surgewave-dev deploy/helm/surgewave/ \
  --namespace surgewave-dev \
  --create-namespace \
  --set broker.replicaCount=1 \
  --set broker.resources.requests.cpu=250m \
  --set broker.resources.requests.memory=512Mi \
  --set broker.storage.size=5Gi \
  --set podDisruptionBudget.enabled=false
```

### With MQTT and GraphQL

```bash
helm install surgewave deploy/helm/surgewave/ \
  --namespace surgewave \
  --create-namespace \
  --set features.mqtt.enabled=true \
  --set features.graphql.enabled=true
```

## Scaling

```bash
# Scale brokers via Helm
helm upgrade surgewave deploy/helm/surgewave/ --set broker.replicaCount=5

# Or directly via kubectl
kubectl scale statefulset surgewave --replicas=5 -n surgewave
```

## Upgrading

```bash
# Rolling upgrade
helm upgrade surgewave deploy/helm/surgewave/ \
  --set broker.image.tag=v2.0.0

# Or use the upgrade script
./deploy/scripts/upgrade.sh --set broker.image.tag=v2.0.0
```

## Uninstalling

```bash
# Remove release (keeps PVCs)
helm uninstall surgewave -n surgewave

# Remove release and PVCs
./deploy/scripts/uninstall.sh --delete-pvcs

# Remove namespace
kubectl delete namespace surgewave
```

## Architecture

```
                    +-------------------+
                    |  Control UI       |
                    |  (Deployment)     |
                    +--------+----------+
                             |
                    +--------+----------+
                    |  Control Service  |
                    |  (ClusterIP)      |
                    +-------------------+

+---------------------------------------------------+
|              Client Service (ClusterIP)            |
+---------------------------------------------------+
                         |
+---------------------------------------------------+
|            Headless Service (DNS)                  |
+---------------------------------------------------+
     |              |              |
+----+----+   +-----+---+   +-----+---+
| surgewave-0 |   | surgewave-1 |   | surgewave-2 |
| (PVC)   |   | (PVC)   |   | (PVC)   |
+---------+   +---------+   +---------+
     StatefulSet (Raft Consensus)
```
