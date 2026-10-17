// Service-specific config for the pubsub smoke test.
// pubsub is a Go service that serves gRPC on 8080 (exposed as port 80 by the Service `pubsub`) and an internal server on
// 5555 (pod IP only, not on the Service) serving /health (liveness) and /ready (readiness).
// The data plane is gRPC only and the suite never calls it: Publish/Subscribe and every other RPC reach SNS/SQS.
// The suite is READ-ONLY. Every request is a GET to the Kubernetes API or to a pod-internal health endpoint.
// There is no negative (unauthenticated or unknown-route) HTTP probe: the service exposes no HTTP surface on its Service.

/** Strip a trailing slash so URL builders can append paths safely. */
const trimSlash = (s) => String(s || '').replace(/\/$/, '');

// Kubernetes API base. For local runs point at `kubectl proxy` (http://127.0.0.1:8001).
export const K8S_API_URL = trimSlash(__ENV.K8S_API_URL || 'https://kubernetes.default.svc');

// Service-account token file. Set K8S_TOKEN_FILE='' (empty) to send NO Authorization header
// (used with `kubectl proxy`, which injects credentials itself).
export const K8S_TOKEN_FILE = __ENV.K8S_TOKEN_FILE !== undefined
  ? __ENV.K8S_TOKEN_FILE
  : '/var/run/secrets/kubernetes.io/serviceaccount/token';

/**
 * Read the service-account token at init. Fails fast when a token file is configured but cannot be
 * read, so the run never silently falls back to unauthenticated Kubernetes calls.
 * @param {string} path token file path, or '' for no token.
 * @returns {string} token, or '' when no path is configured.
 */
export function readToken(path) {
  if (!path) return '';
  try {
    return String(open(path)).trim();
  } catch (e) {
    throw new Error('K8S_TOKEN_FILE set but unreadable: ' + path);
  }
}

export const TOKEN = readToken(K8S_TOKEN_FILE);

// Namespace, Deployment and pod label selector the Kubernetes-side scenarios read.
export const SMOKE_NAMESPACE = __ENV.SMOKE_NAMESPACE || 'pubsub';
export const DEPLOYMENT_NAME = __ENV.DEPLOYMENT_NAME || 'pubsub';
export const POD_LABEL_SELECTOR = __ENV.POD_LABEL_SELECTOR
  || `app.kubernetes.io/name=${DEPLOYMENT_NAME},app.kubernetes.io/instance=${DEPLOYMENT_NAME},app.kubernetes.io/component notin (k6-runner,hr-watcher)`;

// Pod-internal server port (health, ready). Not exposed by the Service.
export const INTERNAL_PORT = __ENV.INTERNAL_PORT || '5555';

// URL builders for every request the suite makes.
export const u = {
  deployment: () =>
    `${K8S_API_URL}/apis/apps/v1/namespaces/${SMOKE_NAMESPACE}/deployments/${DEPLOYMENT_NAME}`,
  pods: () =>
    `${K8S_API_URL}/api/v1/namespaces/${SMOKE_NAMESPACE}/pods?labelSelector=${encodeURIComponent(POD_LABEL_SELECTOR)}`,
  // Pod-internal endpoint on a pod IP (sc02).
  // POD_INTERNAL_BASE (local runs only, e.g. a kubectl port-forward to one pod) replaces the pod IP.
  podInternal: (ip, path) => `${__ENV.POD_INTERNAL_BASE ? trimSlash(__ENV.POD_INTERNAL_BASE) : `http://${ip}:${INTERNAL_PORT}`}${path}`,
};
