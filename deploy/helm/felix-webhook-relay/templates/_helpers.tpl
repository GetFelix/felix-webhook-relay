{{- define "relay.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 45 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 45 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "relay.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* Selector labels for one component: (list . "intake") */}}
{{- define "relay.selector" -}}
{{- $root := index . 0 -}}
app.kubernetes.io/name: {{ $root.Chart.Name }}
app.kubernetes.io/instance: {{ $root.Release.Name }}
app.kubernetes.io/component: {{ index . 1 }}
{{- end -}}

{{- define "relay.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{- define "relay.brokerSecret" -}}
{{- default (printf "%s-broker-credential" (include "relay.fullname" .)) .Values.tokens.brokerCredentialSecret -}}
{{- end -}}

{{- define "relay.tokenSecret" -}}
{{- printf "%s-idp-token" (include "relay.fullname" .) -}}
{{- end -}}

{{- define "relay.securityContext" -}}
runAsNonRoot: true
runAsUser: 65532
runAsGroup: 65532
allowPrivilegeEscalation: false
readOnlyRootFilesystem: true
capabilities:
  drop: [ALL]
{{- end -}}

{{/*
The pod every relay role runs in: (list . "intake" .Values.intake extraEnvYaml).
Only the role and its own settings differ.
*/}}
{{- define "relay.pod" -}}
{{- $root := index . 0 -}}
{{- $role := index . 1 -}}
{{- $v := index . 2 -}}
{{- $values := $root.Values -}}
metadata:
  labels:
    {{- include "relay.selector" (list $root $role) | nindent 4 }}
spec:
  {{- with $values.image.pullSecrets }}
  imagePullSecrets:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  automountServiceAccountToken: false
  containers:
    - name: relay
      image: {{ include "relay.image" $root }}
      imagePullPolicy: {{ $values.image.pullPolicy }}
      securityContext:
        {{- include "relay.securityContext" $root | nindent 8 }}
      env:
        - name: RELAY_ROLES
          value: {{ $role }}
        - name: RELAY_SECRET_KEY
          valueFrom:
            secretKeyRef:
              name: {{ required "secretKey.existingSecret is required" $values.secretKey.existingSecret }}
              key: {{ $values.secretKey.key }}
        - name: RELAY_TENANTS
          value: {{ $values.tenants | quote }}
        - name: RELAY_FELIX_TENANT
          value: {{ $values.felix.tenant | quote }}
        - name: RELAY_FELIX_BROKERS
          value: {{ join "," (required "felix.brokers is required" $values.felix.brokers) | quote }}
        - name: RELAY_FELIX_SERVER_NAME
          value: {{ $values.felix.serverName | quote }}
        {{- if $values.felix.caSecret.name }}
        - name: RELAY_FELIX_CA_FILE
          value: /etc/felix-relay/ca/ca.crt
        {{- end }}
        - name: RELAY_FELIX_CONTROL_PLANE
          value: {{ required "felix.controlPlaneUrl is required" $values.felix.controlPlaneUrl | quote }}
        - name: RELAY_STREAM_REPLICAS
          value: {{ $values.felix.replicas | quote }}
        # The tokens Deployment rewrites this Secret, and the kubelet updates
        # the file, which the relay reads before each exchange.
        - name: RELAY_IDP_TOKEN_FILE
          value: /etc/felix-relay/token/token
        - name: RELAY_WORKER_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        {{- range $name, $value := $values.settings }}
        - name: {{ $name }}
          value: {{ $value | quote }}
        {{- end }}
        {{- with index . 3 }}
        {{- . | nindent 8 }}
        {{- end }}
        {{- with $v.extraEnv }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      ports:
        - name: http
          containerPort: 8090
      readinessProbe:
        httpGet:
          path: /healthz
          port: http
        periodSeconds: 5
      livenessProbe:
        httpGet:
          path: /healthz
          port: http
        periodSeconds: 10
      resources:
        {{- toYaml $v.resources | nindent 8 }}
      volumeMounts:
        - name: token
          mountPath: /etc/felix-relay/token
          readOnly: true
        {{- if $values.felix.caSecret.name }}
        - name: ca
          mountPath: /etc/felix-relay/ca
          readOnly: true
        {{- end }}
  volumes:
    # Written by the tokens Deployment. Pods wait for it on a first install.
    - name: token
      secret:
        secretName: {{ include "relay.tokenSecret" $root }}
    {{- if $values.felix.caSecret.name }}
    - name: ca
      secret:
        secretName: {{ $values.felix.caSecret.name }}
        items:
          - key: {{ $values.felix.caSecret.key }}
            path: ca.crt
    {{- end }}
{{- end -}}

{{/* A Service, and an Ingress when enabled: (list . "intake" .Values.intake "/in/") */}}
{{- define "relay.expose" -}}
{{- $root := index . 0 -}}
{{- $role := index . 1 -}}
{{- $v := index . 2 -}}
{{- $name := printf "%s-%s" (include "relay.fullname" $root) $role -}}
apiVersion: v1
kind: Service
metadata:
  name: {{ $name }}
  labels:
    {{- include "relay.labels" $root | nindent 4 }}
    app.kubernetes.io/component: {{ $role }}
spec:
  type: {{ $v.service.type }}
  ports:
    - name: http
      port: {{ $v.service.port }}
      targetPort: http
  selector:
    {{- include "relay.selector" (list $root $role) | nindent 4 }}
{{- if $v.ingress.enabled }}
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: {{ $name }}
  labels:
    {{- include "relay.labels" $root | nindent 4 }}
  {{- with $v.ingress.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  {{- with $v.ingress.className }}
  ingressClassName: {{ . }}
  {{- end }}
  {{- if $v.ingress.tlsSecret }}
  tls:
    - hosts: [{{ $v.ingress.host | quote }}]
      secretName: {{ $v.ingress.tlsSecret }}
  {{- end }}
  rules:
    - host: {{ required (printf "%s.ingress.host is required" $role) $v.ingress.host | quote }}
      http:
        paths:
          - path: {{ index . 3 }}
            pathType: Prefix
            backend:
              service:
                name: {{ $name }}
                port:
                  name: http
{{- end }}
{{- end -}}

{{- define "relay.deliverEnv" -}}
- name: RELAY_WORKER_COUNT
  value: {{ .Values.deliver.replicas | quote }}
- name: RELAY_WORKER_INDEX
  valueFrom:
    fieldRef:
      fieldPath: metadata.labels['apps.kubernetes.io/pod-index']
{{- end -}}

{{- define "relay.adminEnv" -}}
{{- $a := .Values.admin -}}
- name: RELAY_PUBLIC_URL
  value: {{ $a.publicUrl | default (printf "https://%s" $a.ingress.host) | quote }}
- name: RELAY_OIDC_ISSUER
  value: {{ required "oidc.issuer is required" .Values.oidc.issuer | quote }}
{{- with .Values.oidc.internalUrl }}
- name: RELAY_OIDC_INTERNAL_URL
  value: {{ . | quote }}
{{- end }}
- name: RELAY_OIDC_CLIENT_ID
  value: {{ .Values.oidc.clientId | quote }}
- name: RELAY_OIDC_CLIENT_SECRET
  valueFrom:
    secretKeyRef:
      name: {{ required "oidc.clientSecret.existingSecret is required" .Values.oidc.clientSecret.existingSecret }}
      key: {{ .Values.oidc.clientSecret.key }}
{{- end -}}
