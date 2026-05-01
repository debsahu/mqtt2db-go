{{/*
Common helpers for mqtt2db-go.
*/}}

{{- define "mqtt2db-go.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "mqtt2db-go.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "mqtt2db-go.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "mqtt2db-go.labels" -}}
app.kubernetes.io/name: {{ include "mqtt2db-go.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end }}

{{- define "mqtt2db-go.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mqtt2db-go.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "mqtt2db-go.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "mqtt2db-go.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "mqtt2db-go.image" -}}
{{ .Values.image.repository }}:{{ default .Chart.AppVersion .Values.image.tag }}
{{- end }}

{{- define "mqtt2db-go.secretName" -}}
{{- if .Values.secret.existingName }}{{ .Values.secret.existingName }}{{ else }}{{ include "mqtt2db-go.fullname" . }}-secret{{ end }}
{{- end }}
