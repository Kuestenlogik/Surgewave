# Surgewave Helm Chart

Deploy Kuestenlogik.Surgewave -- a high-performance, Kafka-compatible event streaming platform -- on Kubernetes.

## Quick Start

```bash
# Install a 3-broker cluster
helm install surgewave deployments/helm/surgewave/ \
  --namespace surgewave \
  --create-namespace

# Or use the install script
./deployments/scripts/install.sh --replicas 3 --namespace surgewave
```

Kubernetes 1.28 or later is required: the broker id is the pod's ordinal, read from the
`apps.kubernetes.io/pod-index` label.

## How the cluster forms

A broker is ready — `/health/ready` answers 200, clients are answered — only once it has caught up
with the metadata log. That takes a leader, and a leader takes a majority of the brokers: of three,
the first one alone is live and not ready; as soon as a second one runs, both are ready; the third
starts or rejoins at any time. The chart is built around that:

| What | Why |
|------|-----|
| `podManagementPolicy: Parallel` | With `OrderedReady` the first pod would never become ready and the second would never start. Updates still replace one pod at a time. |
| Headless service with `publishNotReadyAddresses: true` | The brokers find each other by pod DNS name before they are ready. |
| `Surgewave__BrokerId` from the pod ordinal, `Surgewave__Host` = `<pod>.<release>-headless.<namespace>.svc.<clusterDomain>` | A numeric id and a stable name per pod; the name is what clients and the other brokers are told. The listener binds every interface (`Surgewave__BindAddress`). |
| `ClusterNodes` and `ControllerQuorumVoters` generated for `broker.replicaCount` | The same two lists on every broker; every broker votes. |
| Startup and liveness probe on `/health/live`, readiness probe on `/health/ready` | Liveness never fails for want of a quorum, so a broker that waits for its peers is not restarted; a broker that is not ready is only taken out of the client service. |
| PodDisruptionBudget, `maxUnavailable: 1` | Two of three brokers keep the quorum. |

A broker that finds no peer when it starts waits for one for 30 s before it binds its HTTP endpoint,
and every broker of a new cluster starts that way: a fresh install takes about half a minute to
become ready. `startupProbe.periodSeconds × failureThreshold` has to cover that wait plus the start
itself; raise it for pods with a small CPU limit.

Clients in the cluster bootstrap from the client service (`<release>:9092`) and are then told the
pod names.

## Configuration

All configuration is done via `values.yaml` or `--set` flags.

### Broker

| Parameter | Description | Default |
|-----------|-------------|---------|
| `broker.replicaCount` | Number of brokers; all of them vote on metadata | `3` |
| `broker.image.repository` | Broker image repository | `ghcr.io/kuestenlogik/surgewave-broker` |
| `broker.image.tag` | Broker image tag; empty is the chart's `appVersion` | `""` |
| `broker.resources.requests.cpu` | CPU request | `500m` |
| `broker.resources.requests.memory` | Memory request | `1Gi` |
| `broker.resources.limits.cpu` | CPU limit | `2` |
| `broker.resources.limits.memory` | Memory limit | `4Gi` |
| `broker.storage.size` | PVC size per broker | `10Gi` |
| `broker.storage.storageClassName` | Storage class (empty = default) | `""` |
| `broker.ports.kafka` | Client port (native protocol, Kafka with the plugin) | `9092` |
| `broker.ports.grpc` | gRPC / REST port; also `/health` and `/metrics` | `9093` |
| `broker.ports.replication` | Inter-broker port | `10092` |
| `broker.ports.mqtt` | MQTT protocol port | `1883` |
| `broker.config.autoTopicCreation` | Auto-create topics | `true` |
| `broker.config.defaultPartitions` | Default partitions per topic | `3` |
| `broker.config.defaultReplicationFactor` | Default replication factor | `3` |
| `broker.config.minInsyncReplicas` | Minimum in-sync replicas for a write | `2` |
| `broker.config.logRetentionHours` | Log retention (hours) | `168` |
| `clusterDomain` | The cluster's DNS domain, part of the name each broker is told | `cluster.local` |

### Probes and disruption

| Parameter | Description | Default |
|-----------|-------------|---------|
| `startupProbe.periodSeconds` / `failureThreshold` | How long a broker may take to bind its HTTP endpoint | `5` / `60` |
| `livenessProbe.periodSeconds` / `failureThreshold` | `/health/live` | `10` / `3` |
| `readinessProbe.periodSeconds` / `failureThreshold` | `/health/ready` | `5` / `3` |
| `podDisruptionBudget.enabled` | Create a PodDisruptionBudget | `true` |
| `podDisruptionBudget.maxUnavailable` | Brokers that may be down through a voluntary disruption | `1` |

### Control UI

| Parameter | Description | Default |
|-----------|-------------|---------|
| `control.enabled` | Deploy the Control UI | `true` |
| `control.image.repository` | Control UI image repository | `ghcr.io/kuestenlogik/surgewave-control` |
| `control.image.tag` | Control UI image tag; empty is the chart's `appVersion` | `""` |
| `control.replicaCount` | Control UI replicas | `1` |
| `control.port` | Control UI HTTP port | `5050` |
| `control.ingress.enabled` | Create Ingress for Control UI | `false` |
| `control.ingress.hostname` | Ingress hostname | `surgewave.local` |

### Features

| Parameter | Description | Default |
|-----------|-------------|---------|
| `schemaRegistry.enabled` | Enable Schema Registry | `true` |
| `connect.enabled` | Enable the Connect worker embedded in each broker. Leave off with more than one broker: a broker that does not lead the partition of the worker's pipeline store topic does not finish starting | `false` |
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
helm install surgewave deployments/helm/surgewave/ \
  --namespace surgewave \
  --create-namespace \
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
  replicaCount: 3
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
helm install surgewave deployments/helm/surgewave/ -f production-values.yaml --namespace surgewave --create-namespace
```

### Development (Single Broker)

```bash
helm install surgewave-dev deployments/helm/surgewave/ \
  --namespace surgewave-dev \
  --create-namespace \
  --set broker.replicaCount=1 \
  --set broker.config.defaultReplicationFactor=1 \
  --set broker.config.minInsyncReplicas=1 \
  --set broker.resources.requests.cpu=250m \
  --set broker.resources.requests.memory=512Mi \
  --set broker.storage.size=5Gi \
  --set podDisruptionBudget.enabled=false
```

A single broker is its own quorum and is ready alone; the chart then sets no `ClusterNodes` and no
`ControllerQuorumVoters`.

### With MQTT and GraphQL

```bash
helm install surgewave deployments/helm/surgewave/ \
  --namespace surgewave \
  --create-namespace \
  --set features.mqtt.enabled=true \
  --set features.graphql.enabled=true
```

## Number of brokers

`broker.replicaCount` is written into every broker's `ClusterNodes` and `ControllerQuorumVoters`.
Changing it on an installed release changes the quorum of a running cluster and restarts every
broker; choose it at install time and use an odd number. `kubectl scale` alone does not add a broker
to the cluster: the new pod is not in the lists of the others.

## Upgrading

```bash
# Rolling upgrade: one broker at a time, each ready before the next
helm upgrade surgewave deployments/helm/surgewave/ \
  --set broker.image.tag=0.6.1

# Or use the upgrade script
./deployments/scripts/upgrade.sh --set broker.image.tag=0.6.1
```

## Uninstalling

```bash
# Remove release (keeps PVCs)
helm uninstall surgewave -n surgewave

# Remove release and PVCs
./deployments/scripts/uninstall.sh --delete-pvcs

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
