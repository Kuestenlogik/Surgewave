{{/*
Expand the name of the chart.
*/}}
{{- define "storm.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "storm.fullname" -}}
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
{{- define "storm.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "storm.labels" -}}
helm.sh/chart: {{ include "storm.chart" . }}
{{ include "storm.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels for broker
*/}}
{{- define "storm.selectorLabels" -}}
app.kubernetes.io/name: {{ include "storm.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: broker
{{- end }}

{{/*
Selector labels for Control UI
*/}}
{{- define "storm.control.selectorLabels" -}}
app.kubernetes.io/name: {{ include "storm.name" . }}-control
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: control-ui
{{- end }}

{{/*
Control UI labels
*/}}
{{- define "storm.control.labels" -}}
helm.sh/chart: {{ include "storm.chart" . }}
{{ include "storm.control.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "storm.serviceAccountName" -}}
{{- include "storm.fullname" . }}
{{- end }}

{{/*
Generate cluster nodes string for Raft consensus.
Format: <pod-name>:<pod-fqdn>:<replication-port>
*/}}
{{- define "storm.clusterNodes" -}}
{{- $fullname := include "storm.fullname" . -}}
{{- $replicaCount := int .Values.broker.replicaCount -}}
{{- $replicationPort := int .Values.broker.ports.replication -}}
{{- $nodes := list -}}
{{- range $i := until $replicaCount -}}
{{- $nodes = append $nodes (printf "%s-%d:%s-%d.%s-headless:%d" $fullname $i $fullname $i $fullname $replicationPort) -}}
{{- end -}}
{{- join "," $nodes -}}
{{- end }}

{{/*
Generate bootstrap servers string for client connections.
Format: <pod-fqdn>:<kafka-port>
*/}}
{{- define "storm.bootstrapServers" -}}
{{- $fullname := include "storm.fullname" . -}}
{{- $replicaCount := int .Values.broker.replicaCount -}}
{{- $kafkaPort := int .Values.broker.ports.kafka -}}
{{- $servers := list -}}
{{- range $i := until $replicaCount -}}
{{- $servers = append $servers (printf "%s-%d.%s-headless:%d" $fullname $i $fullname $kafkaPort) -}}
{{- end -}}
{{- join "," $servers -}}
{{- end }}
