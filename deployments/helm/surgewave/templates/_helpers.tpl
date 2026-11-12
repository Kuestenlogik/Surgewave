{{/*
Expand the name of the chart.
*/}}
{{- define "surgewave.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "surgewave.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "surgewave.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "surgewave.labels" -}}
helm.sh/chart: {{ include "surgewave.chart" . }}
{{ include "surgewave.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels for broker
*/}}
{{- define "surgewave.selectorLabels" -}}
app.kubernetes.io/name: {{ include "surgewave.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: broker
{{- end }}

{{/*
Selector labels for Control UI
*/}}
{{- define "surgewave.control.selectorLabels" -}}
app.kubernetes.io/name: {{ include "surgewave.name" . }}-control
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: control-ui
{{- end }}

{{/*
Control UI labels
*/}}
{{- define "surgewave.control.labels" -}}
helm.sh/chart: {{ include "surgewave.chart" . }}
{{ include "surgewave.control.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "surgewave.serviceAccountName" -}}
{{- include "surgewave.fullname" . }}
{{- end }}

{{/*
The DNS suffix of the broker pods: every pod is <pod>.<this>, the name it tells clients and
the other brokers (Surgewave:Host).
*/}}
{{- define "surgewave.podDomain" -}}
{{- printf "%s-headless.%s.svc.%s" (include "surgewave.fullname" .) .Release.Namespace .Values.clusterDomain -}}
{{- end }}

{{/*
The brokers of the cluster, Surgewave:ClusterNodes.
Format: <broker-id>:<pod-fqdn>:<client-port>:<replication-port>
The broker id is the pod's ordinal.
*/}}
{{- define "surgewave.clusterNodes" -}}
{{- $fullname := include "surgewave.fullname" . -}}
{{- $domain := include "surgewave.podDomain" . -}}
{{- $clientPort := int .Values.broker.ports.kafka -}}
{{- $replicationPort := int .Values.broker.ports.replication -}}
{{- $nodes := list -}}
{{- range $i := until (int .Values.broker.replicaCount) -}}
{{- $nodes = append $nodes (printf "%d:%s-%d.%s:%d:%d" $i $fullname $i $domain $clientPort $replicationPort) -}}
{{- end -}}
{{- join "," $nodes -}}
{{- end }}

{{/*
The brokers that vote on metadata, Surgewave:ControllerQuorumVoters: all of them.
Format: <broker-id>@<pod-fqdn>:<replication-port>
*/}}
{{- define "surgewave.controllerQuorumVoters" -}}
{{- $fullname := include "surgewave.fullname" . -}}
{{- $domain := include "surgewave.podDomain" . -}}
{{- $replicationPort := int .Values.broker.ports.replication -}}
{{- $voters := list -}}
{{- range $i := until (int .Values.broker.replicaCount) -}}
{{- $voters = append $voters (printf "%d@%s-%d.%s:%d" $i $fullname $i $domain $replicationPort) -}}
{{- end -}}
{{- join "," $voters -}}
{{- end }}
