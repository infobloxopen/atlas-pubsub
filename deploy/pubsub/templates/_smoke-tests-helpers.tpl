{{/*
Cluster identity of this chart: global.ib0.cluster.name (rendered from the DC env value, e.g. us-dev-5).
pubsub's chart has no cluster-name value of its own, so the DC overlay or the install command sets global.ib0.cluster.name.
Kept here, not in _helpers.tpl, so the smoke-test wiring stays in one file.
*/}}
{{- define "pubsub.env" -}}
{{- tpl (((.Values.global).ib0).cluster).name . -}}
{{- end -}}

{{/*
Cluster the smoke tests run against and tag metrics with. Defaults to the chart's own cluster identity
(pubsub.env); smokeTests.cluster is only an explicit override.
smokeTests.allowedClusters stays the hand-set guard list, so setup() fails fast when the suite is enabled
on a cluster that is not on it. Fails the render when no cluster can be resolved, so a Job never starts with an empty cluster.
*/}}
{{- define "pubsub.smokeCluster" -}}
{{- $cluster := .Values.smokeTests.cluster | default (include "pubsub.env" .) -}}
{{- if not $cluster -}}
{{- fail "smokeTests: no cluster name. Set smokeTests.cluster or global.ib0.cluster.name (e.g. --set global.ib0.cluster.name=us-dev-5)" -}}
{{- end -}}
{{- $cluster -}}
{{- end -}}
