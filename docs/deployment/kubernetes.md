# Kubernetes Deployment

Deploy Surgewave on Kubernetes.

The manifests under `deployments/kubernetes/` bring up three brokers; the [Helm chart](helm.md)
generates the same for any number. This page explains what they contain and why. Kubernetes 1.28 or
later is required.

## Quick Start

```bash
kubectl create namespace surgewave
kubectl apply -n surgewave \
  -f deployments/kubernetes/surgewave-configmap.yaml \
  -f deployments/kubernetes/surgewave-service.yaml \
  -f deployments/kubernetes/surgewave-statefulset.yaml
kubectl rollout status -n surgewave statefulset/surgewave
```

The manifests work in any namespace.

## What a broker cluster needs from Kubernetes

A broker is ready — `/health/ready` answers 200, clients are answered — only once it has caught up
with the metadata log. That takes a leader, and a leader takes a majority of the controller voters:
of three brokers, the first one alone is live and not ready; as soon as a second one runs, both are
ready; the third starts or rejoins at any time. Everything below follows from that.

### Parallel pod management

```yaml
spec:
  podManagementPolicy: Parallel
  updateStrategy:
    type: RollingUpdate
```

The default, `OrderedReady`, starts a pod only when the one before it is ready. The first broker never
becomes ready alone, so the second would never start. `Parallel` starts all pods together.
`podManagementPolicy` applies to scaling and to replacing deleted pods only: an update of the pod
template still replaces one pod at a time, highest ordinal first, each only after the one before is
ready again.

### A headless service that publishes not-ready addresses

```yaml
apiVersion: v1
kind: Service
metadata:
  name: surgewave-headless
spec:
  clusterIP: None
  publishNotReadyAddresses: true
```

The brokers find each other by their pod DNS names. Without `publishNotReadyAddresses` a pod's name
resolves only once the pod is ready — and the brokers only become ready once they have found each
other. The client service (`surgewave`, ClusterIP) does not set it: it routes to ready brokers only.

### Ports

| Port | Name | Serves |
|------|------|--------|
| 9092 | `kafka` | Kafka wire protocol and native protocol |
| 9093 | `grpc` | REST, `/health` and `/metrics` over HTTP/1.1; the probes and a ServiceMonitor use it |
| 9094 | `grpc-http2` | gRPC over HTTP/2 |
| 10092 | `replication` | Between the brokers; on the headless service only |

The broker has two HTTP endpoints, and the manifests serve them without TLS: a cleartext endpoint
serves HTTP/2, which gRPC needs, or HTTP/1.1, so REST and gRPC are two ports. All three services
carry both: a gRPC client in the cluster connects to `http://surgewave:9094`, a REST client to
`http://surgewave:9093`. With TLS both ports serve gRPC and REST over HTTPS, and the probes need
`scheme: HTTPS`; see [TLS on Kubernetes](tls.md#on-kubernetes).

### A stable identity per pod

```yaml
env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: POD_NAMESPACE
    valueFrom:
      fieldRef:
        fieldPath: metadata.namespace
  # The broker id is the pod's ordinal: 0, 1, 2.
  - name: Surgewave__BrokerId
    valueFrom:
      fieldRef:
        fieldPath: metadata.labels['apps.kubernetes.io/pod-index']
  # What clients and the other brokers are told: the pod's stable DNS name.
  - name: Surgewave__Host
    value: "$(POD_NAME).surgewave-headless.$(POD_NAMESPACE).svc"
  - name: Surgewave__ClusterNodes
    value: "0:surgewave-0.surgewave-headless.$(POD_NAMESPACE).svc:9092:10092,1:surgewave-1.surgewave-headless.$(POD_NAMESPACE).svc:9092:10092,2:surgewave-2.surgewave-headless.$(POD_NAMESPACE).svc:9092:10092"
  - name: Surgewave__ControllerQuorumVoters
    value: "0@surgewave-0.surgewave-headless.$(POD_NAMESPACE).svc:10092,1@surgewave-1.surgewave-headless.$(POD_NAMESPACE).svc:10092,2@surgewave-2.surgewave-headless.$(POD_NAMESPACE).svc:10092"
```

- `Surgewave__BrokerId` is a number. The StatefulSet controller labels every pod with its ordinal
  (`apps.kubernetes.io/pod-index`, Kubernetes 1.28+), and the downward API hands the label to the
  broker; no init container or shell is involved.
- `Surgewave__Host` is the name the broker tells clients and the other brokers. The client listener
  binds every interface (`"BindAddress": "0.0.0.0"` in the ConfigMap), because a name cannot be bound.
- `ClusterNodes` (`id:host:port:replicationPort`) and `ControllerQuorumVoters`
  (`id@host:replicationPort`) are the same on every broker and name all three. Changing `replicas`
  means changing both lists.

### Probes

```yaml
startupProbe:
  httpGet: { path: /health/live, port: grpc }
  periodSeconds: 5
  failureThreshold: 60
livenessProbe:
  httpGet: { path: /health/live, port: grpc }
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet: { path: /health/ready, port: grpc }
  periodSeconds: 5
  failureThreshold: 3
```

| Probe | Endpoint | Meaning |
|-------|----------|---------|
| startup | `/health/live` | The HTTP endpoint is bound. Holds the other two back until then. |
| liveness | `/health/live` | The process answers. Never fails for want of a quorum, so a broker that waits for its peers is not restarted. |
| readiness | `/health/ready` | The broker has caught up with the metadata log. A broker without a quorum is taken out of the client service. |

A broker binds the HTTP endpoint a few seconds after its process starts, whether or not another
broker runs: it does not wait for a peer first. The startup probe's `periodSeconds × failureThreshold`
has to cover the start itself on the CPU the pod is given.

### PodDisruptionBudget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: surgewave
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      app: surgewave
```

Two of three brokers keep the quorum; a node drain may take one.

### Configuration

The ConfigMap replaces the image's `appsettings.json`. It names the image's two HTTP endpoints and
sets `GrpcUseTls` to `false`, which binds REST, `/health` and `/metrics` on port 9093 and gRPC on
port 9094, both in cleartext (see [TLS](tls.md#on-kubernetes) to serve them over HTTPS), and leaves the
embedded Connect worker off. With it on, each broker runs a
worker that reads and writes its stores through its own broker, and only the one on the broker
that leads the stores' partitions has the stored connectors and pipelines.

### Storage

The volume claim names no storage class and gets the cluster's default one. Name one in
`volumeClaimTemplates` for production.

## Clients

Inside the cluster, clients bootstrap from the client service `surgewave:9092` and are then told the
brokers' pod names (`surgewave-0.surgewave-headless.<namespace>.svc:9092`, …).

The `surgewave-external` LoadBalancer service reaches a broker from outside, but the broker still
tells the client those in-cluster names. A client outside the cluster has to be able to resolve and
reach them; a broker has one advertised name.

## Operations

```bash
# Replace one broker; it rejoins and becomes ready again
kubectl delete pod -n surgewave surgewave-1
kubectl rollout status -n surgewave statefulset/surgewave

# Rolling restart, one broker at a time
kubectl rollout restart -n surgewave statefulset/surgewave
```

`kubectl scale` alone does not add a broker to the cluster: the new pod is not in `ClusterNodes` and
`ControllerQuorumVoters` of the others.

## Resource Requirements

| Size | CPU | Memory | Storage |
|------|-----|--------|---------|
| Small | 1 | 2Gi | 50Gi |
| Medium | 2 | 4Gi | 100Gi |
| Large | 4 | 8Gi | 500Gi |

## Monitoring

### ServiceMonitor (Prometheus)

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: surgewave
spec:
  selector:
    matchLabels:
      app: surgewave
  endpoints:
  - port: grpc
    path: /metrics
    interval: 30s
```

`/metrics` answers whether or not the broker is ready.

## Next Steps

- [Helm](helm.md) - Package management
- [Monitoring](../monitoring/index.md) - Observability
