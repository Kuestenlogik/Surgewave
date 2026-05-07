# Kubernetes Deployment

Deploy Surgewave on Kubernetes.

## Quick Start

```bash
kubectl apply -f https://raw.githubusercontent.com/Kuestenlogik/Surgewave/main/deploy/kubernetes/
```

## Components

### StatefulSet

3-replica Surgewave cluster:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: surgewave
spec:
  serviceName: surgewave-headless
  replicas: 3
  selector:
    matchLabels:
      app: surgewave
  template:
    spec:
      containers:
      - name: surgewave
        image: kuestenlogik/surgewave:latest
        ports:
        - containerPort: 9092
          name: kafka
        - containerPort: 9093
          name: grpc
        - containerPort: 10092
          name: replication
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: Surgewave__BrokerId
          value: "$(POD_NAME##*-)"
        - name: Surgewave__Host
          value: "$(POD_NAME).surgewave-headless"
        - name: Surgewave__ClusterNodes
          value: "surgewave-0.surgewave-headless:9092,surgewave-1.surgewave-headless:9092,surgewave-2.surgewave-headless:9092"
        - name: Surgewave__UseRaftConsensus
          value: "true"
        volumeMounts:
        - name: data
          mountPath: /data
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
        livenessProbe:
          tcpSocket:
            port: 9092
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          tcpSocket:
            port: 9092
          initialDelaySeconds: 5
          periodSeconds: 5
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: fast-ssd
      resources:
        requests:
          storage: 100Gi
```

### Services

```yaml
# Headless service for StatefulSet
apiVersion: v1
kind: Service
metadata:
  name: surgewave-headless
spec:
  clusterIP: None
  selector:
    app: surgewave
  ports:
  - name: kafka
    port: 9092
  - name: grpc
    port: 9093
  - name: replication
    port: 10092

---
# Client service
apiVersion: v1
kind: Service
metadata:
  name: surgewave
spec:
  type: ClusterIP
  selector:
    app: surgewave
  ports:
  - name: kafka
    port: 9092
  - name: grpc
    port: 9093
```

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: surgewave-config
data:
  appsettings.json: |
    {
      "Surgewave": {
        "StorageMode": "File",
        "DataDirectory": "/data",
        "LogSegmentBytes": 1073741824,
        "LogRetentionHours": 168,
        "AutoCreateTopics": true,
        "DefaultNumPartitions": 6,
        "DefaultReplicationFactor": 3,
        "MinInSyncReplicas": 2
      }
    }
```

## Scaling

```bash
# Scale to 5 brokers
kubectl scale statefulset surgewave --replicas=5

# Scale down (careful - move partitions first)
kubectl scale statefulset surgewave --replicas=3
```

## External Access

### LoadBalancer

```yaml
apiVersion: v1
kind: Service
metadata:
  name: surgewave-external
spec:
  type: LoadBalancer
  selector:
    app: surgewave
  ports:
  - name: kafka
    port: 9092
  - name: grpc
    port: 9093
```

### NodePort

```yaml
apiVersion: v1
kind: Service
metadata:
  name: surgewave-nodeport
spec:
  type: NodePort
  selector:
    app: surgewave
  ports:
  - name: kafka
    port: 9092
    nodePort: 30092
```

## Resource Requirements

| Size | CPU | Memory | Storage |
|------|-----|--------|---------|
| Small | 1 | 2Gi | 50Gi |
| Medium | 2 | 4Gi | 100Gi |
| Large | 4 | 8Gi | 500Gi |

## Pod Disruption Budget

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: surgewave-pdb
spec:
  minAvailable: 2
  selector:
    matchLabels:
      app: surgewave
```

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

## Next Steps

- [Helm](helm.md) - Package management
- [Monitoring](../monitoring/index.md) - Observability
