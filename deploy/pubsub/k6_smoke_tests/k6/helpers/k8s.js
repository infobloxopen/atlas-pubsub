// Kubernetes API helper for the pubsub smoke test. Used by sc01 (GET the Deployment) and
// sc02 (LIST pods in the release namespace to find pod IPs). Every call is a read.
// Bearer auth uses the Job's ServiceAccount token (TOKEN, read at init in config/config.js).
import http from 'k6/http';
import { logRequest, authHeaders, noAuthHeaders, parseBody } from './utils.js';
import { TOKEN, u } from '../config/config.js';

/**
 * Kube API headers: Bearer when a token was read at init, otherwise none (kubectl proxy).
 * @returns {object} header map.
 */
export function kubeHeaders() {
  return TOKEN ? authHeaders(TOKEN) : noAuthHeaders();
}

/**
 * Single Kubernetes API GET. The status is always logged; the body only on errors (a success body is
 * never logged). Authorization is redacted by logRequest.
 * @param {string} url full URL.
 * @param {string} endpoint short label for logs.
 * @returns {object} k6 response.
 */
export function kubeGet(url, endpoint) {
  const headers = kubeHeaders();
  const r = http.get(url, { timeout: '30s', headers });
  logRequest('execution', endpoint, 'GET', url, null,
    { status: r.status, body: r.status >= 400 ? r.body : '' }, headers);
  return r;
}

/**
 * List the pods matching POD_LABEL_SELECTOR that are Running, Ready, have a pod IP and are not terminating.
 * @returns {{status: number, pods: Array<{name: string, ip: string}>}} list status and ready pods
 *   (pods is empty when the list call fails).
 */
export function listReadyPods() {
  const r = kubeGet(u.pods(), 'list_pubsub_pods');
  if (r.status !== 200) return { status: r.status, pods: [] };
  const items = parseBody(r).items || [];
  const pods = items
    .filter((p) => p.status && p.status.podIP && p.status.phase === 'Running'
      && !(p.metadata && p.metadata.deletionTimestamp)
      && (p.status.conditions || []).some((c) => c.type === 'Ready' && c.status === 'True'))
    .map((p) => ({ name: p.metadata.name, ip: p.status.podIP }));
  return { status: r.status, pods };
}
