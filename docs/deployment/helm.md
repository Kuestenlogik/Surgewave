# Helm Chart

Deploy Surgewave using the Helm chart in `deployments/helm/surgewave`.

## Installation

```bash
helm install surgewave ./deployments/helm/surgewave \
  --namespace surgewave --create-namespace
kubectl rollout status -n surgewave statefulset/surgewave
```

This installs three brokers and the Control UI. Kubernetes 1.28 or later is required: the broker id
is the pod's ordinal, which the chart reads from the `apps.kubernetes.io/pod-index` label.

## What the chart sets up

- A **StatefulSet** with `podManagementPolicy: Parallel`. A broker is ready only once it has caught
  up with the metadata log, which takes a majority of the brokers; with the default `OrderedReady` the
  first pod would never become ready and the second would never start. Updates still replace one pod
  at a time.
- A **headless service** with `publishNotReadyAddresses: true`, through which the brokers find each
  other before they are ready, and a **client service** that routes to ready brokers only.
- Per pod: `Surgewave__BrokerId` (the ordinal) and `Surgewave__Host`, the pod's DNS name
  `<pod>.<release>-headless.<namespace>.svc.<clusterDomain>`, which clients and the other brokers are
  told.
- For all pods: `ClusterNodes` and `ControllerQuorumVoters`, generated for `broker.replicaCount`
  brokers. Every broker votes.
- A **startup probe** and a **liveness probe** on `/health/live`, a **readiness probe** on
  `/health/ready`. Liveness never fails for want of a quorum; a broker that waits for its peers is not
  restarted.
- A **PodDisruptionBudget** with `maxUnavailable: 1`.
- The **Control UI**, pointed at the brokers' REST endpoint through the client service.

[Kubernetes Deployment](kubernetes.md) explains each of these.

## Configuration

The values that matter for a cluster; `values.yaml` lists all of them.

```yaml
broker:
  replicaCount: 3
  image:
    repository: ghcr.io/kuestenlogik/surgewave-broker
    tag: ""            # the chart's appVersion
  storage:
    size: 10Gi
    storageClassName: ""   # the cluster's default
  ports:
    kafka: 9092
    grpc: 9093
    replication: 10092
  config:
    defaultPartitions: 3
    defaultReplicationFactor: 3
    minInsyncReplicas: 2

clusterDomain: cluster.local

startupProbe:
  periodSeconds: 5
  timeoutSeconds: 3
  failureThreshold: 60

podDisruptionBudget:
  enabled: true
  maxUnavailable: 1
```

### Startup probe

A broker that finds no peer when it starts waits for one for 30 s before it binds its HTTP endpoint,
and every broker of a new cluster starts that way. `startupProbe.periodSeconds × failureThreshold`
(five minutes by default) has to cover that wait plus the start itself. Raise it for pods with a
small CPU limit rather than lowering it.

### Number of brokers

`broker.replicaCount` is written into every broker's `ClusterNodes` and `ControllerQuorumVoters`.
Changing it on an installed release changes the quorum of a running cluster and restarts every
broker; use an odd number.

### Kafka Connect

`connect.enabled` turns on the Connect worker embedded in each broker. Leave it off with more than one
broker: a broker that does not lead the partition of the worker's pipeline store topic does not
finish starting and never becomes ready.

## Custom Values

### Production

```yaml
# prod-values.yaml
broker:
  replicaCount: 3
  resources:
    requests:
      cpu: "2"
      memory: 4Gi
    limits:
      cpu: "4"
      memory: 8Gi
  storage:
    size: 100Gi
    storageClassName: fast-ssd

antiAffinity: hard

monitoring:
  serviceMonitor:
    enabled: true
```

```bash
helm install surgewave ./deployments/helm/surgewave -f prod-values.yaml \
  --namespace surgewave --create-namespace
```

### Development

```yaml
# dev-values.yaml
broker:
  replicaCount: 1
  resources:
    requests:
      cpu: 250m
      memory: 512Mi
  storage:
    size: 5Gi
  config:
    defaultReplicationFactor: 1
    minInsyncReplicas: 1

podDisruptionBudget:
  enabled: false
```

A single broker is its own quorum and is ready alone.

## Upgrade

```bash
# Upgrade with new values
helm upgrade surgewave ./deployments/helm/surgewave -f prod-values.yaml -n surgewave

# Another broker version: one pod at a time, each ready before the next
helm upgrade surgewave ./deployments/helm/surgewave --set broker.image.tag=0.6.1 -n surgewave
kubectl rollout status -n surgewave statefulset/surgewave
```

## Uninstall

```bash
helm uninstall surgewave -n surgewave

# The volume claims stay; delete them to delete the data
kubectl delete pvc -n surgewave -l app.kubernetes.io/name=surgewave
```

## Chart Structure

```
surgewave/
├── Chart.yaml
├── values.yaml
├── templates/
│   ├── _helpers.tpl
│   ├── broker-configmap.yaml
│   ├── broker-service.yaml
│   ├── broker-statefulset.yaml
│   ├── control-deployment.yaml
│   ├── control-ingress.yaml
│   ├── control-service.yaml
│   ├── pdb.yaml
│   ├── rbac.yaml
│   ├── serviceaccount.yaml
│   └── servicemonitor.yaml
└── README.md
```

## Next Steps

- [Monitoring](../monitoring/index.md) - Observability
- [Security](../security/index.md) - Enable security
