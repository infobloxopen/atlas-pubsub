// sc01 deployment_ready: the pubsub Deployment reports ready replicas via the Kubernetes API (GET only).
import { check, sleep } from 'k6';
import { parseBody } from '../helpers/utils.js';
import { kubeGet } from '../helpers/k8s.js';
import { u } from '../config/config.js';

/**
 * sc01: GET the Deployment returns 200, readyReplicas >= 1 and == spec.replicas, updatedReplicas >= spec.replicas
 * (so an old healthy pod cannot mask a crashing new one), observedGeneration == generation, and spec.paused is not set.
 * Re-reads while a rollout settles, bounded by a wall-clock deadline (SETTLE_MS from the start of the scenario).
 * No new read starts once the deadline has passed.
 * @returns {boolean} overall pass/fail for the group.
 */
export function sc01DeploymentReady() {
  const SETTLE_MS = 90000;
  const POLL_MS = 5000;
  const deadline = Date.now() + SETTLE_MS;
  let r;
  let body = {};
  let ready = 0;
  let updated = 0;
  let total = 0;
  let desired;
  let generation;
  let observed;
  let paused = false;
  for (;;) {
    r = kubeGet(u.deployment(), 'get_pubsub_deployment');
    body = parseBody(r);
    const status = body.status || {};
    ready = status.readyReplicas || 0;
    updated = status.updatedReplicas || 0;
    total = status.replicas || 0;
    desired = body.spec ? body.spec.replicas : undefined;
    generation = body.metadata ? body.metadata.generation : undefined;
    observed = status.observedGeneration;
    paused = !!(body.spec && body.spec.paused);
    const settled = r.status === 200 && desired > 0 && ready === desired && updated >= desired
      && total === desired
      && observed === generation && !paused;
    // Stop before the next sleep would carry us past the deadline, so no read starts after it.
    if (settled || r.status !== 200 || Date.now() + POLL_MS >= deadline) break;
    sleep(POLL_MS / 1000);
  }
  console.log(JSON.stringify({ phase: 'sc01', deployment: body.metadata ? body.metadata.name : null,
    readyReplicas: ready, updatedReplicas: updated, totalReplicas: total, desiredReplicas: desired, generation,
    observedGeneration: observed, paused }));
  return check(r, {
    'sc01 deployment GET 200': (x) => x.status === 200,
    'sc01 readyReplicas >= 1': () => ready >= 1,
    'sc01 readyReplicas == spec.replicas': () => desired > 0 && ready === desired,
    'sc01 updatedReplicas >= spec.replicas': () => desired > 0 && updated >= desired,
    'sc01 no old-ReplicaSet pods (status.replicas == spec.replicas)': () => desired > 0 && total === desired,
    'sc01 observedGeneration == generation': () => observed !== undefined && observed === generation,
    'sc01 deployment not paused': () => !paused,
  });
}
