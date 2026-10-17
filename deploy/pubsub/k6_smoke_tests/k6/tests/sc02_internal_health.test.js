// sc02 internal_health: every ready pod answers /health (liveness) and /ready (readiness) with 200.
// The internal server (5555) is not on the Service, so pod IPs come from a pod LIST through the Kubernetes API.
import { check } from 'k6';
import { httpGet } from '../helpers/utils.js';
import { listReadyPods } from '../helpers/k8s.js';
import { u } from '../config/config.js';

/**
 * sc02: at least one ready pod exists, and on every ready pod GET /health and GET /ready return 200.
 * @returns {boolean} overall pass/fail for the group.
 */
export function sc02InternalHealth() {
  const list = listReadyPods();
  const pods = list.pods;
  const perPod = pods.map((pod) => {
    const health = httpGet(u.podInternal(pod.ip, '/health'), { Accept: 'text/plain' }, 'pod_health');
    const ready = httpGet(u.podInternal(pod.ip, '/ready'), { Accept: 'text/plain' }, 'pod_ready');
    const ok = { health: health.status === 200, ready: ready.status === 200 };
    console.log(JSON.stringify({ phase: 'sc02', pod: pod.name, ip: pod.ip, ...ok,
      statuses: { health: health.status, ready: ready.status } }));
    return ok;
  });
  const every = (key) => pods.length > 0 && perPod.every((p) => p[key]);
  return check(list, {
    'sc02 pod list 200': (x) => x.status === 200,
    'sc02 at least one ready pod': () => pods.length > 0,
    'sc02 every ready pod /health 200': () => every('health'),
    'sc02 every ready pod /ready 200': () => every('ready'),
  });
}
