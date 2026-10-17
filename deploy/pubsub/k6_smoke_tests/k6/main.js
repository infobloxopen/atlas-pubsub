// pubsub smoke test (Go service: gRPC on Service pubsub:80 -> 8080; internal HTTP on pod port 5555 serving /health and /ready).
//
// The suite is READ-ONLY. It never calls the gRPC API (Publish, Subscribe or any other RPC reaches SNS/SQS), and every
// request is a GET. The Kubernetes API is read with the Job's ServiceAccount (get on the Deployment, list on pods). The
// pod-internal endpoints take no credentials.
//
// Scenarios (one VU, one iteration). Each lives in tests/ and records one group result:
//   sc01  deployment_ready  GET apps/v1 Deployment -> 200, readyReplicas >= 1 == spec.replicas, updatedReplicas >= spec.replicas, observed, not paused
//   sc02  internal_health   every ready pod: GET :5555/health and GET :5555/ready -> 200 (pod IP)
// There is no negative HTTP probe: the service exposes no HTTP surface on its Service (gRPC only).
import { requireEnv, assertClusterAllowed } from './helpers/utils.js';
import { recordResult } from './helpers/metrics.js';
import { DEPLOYMENT_NAME, POD_LABEL_SELECTOR } from './config/config.js';
import { sc01DeploymentReady } from './tests/sc01_deployment_ready.test.js';
import { sc02InternalHealth } from './tests/sc02_internal_health.test.js';

export const options = {
  scenarios: {
    pubsub_smoke: {
      executor: 'per-vu-iterations',
      vus: 1,
      iterations: 1,
      maxDuration: '5m',
    },
  },
  thresholds: {
    checks: ['rate>=0.95'],
    smoke_overall_result: ['rate>=0.95'],
    // Fails the run if the single iteration never completed, so an interrupted run cannot pass.
    iterations: ['count>=1'],
  },
};

/**
 * Run one scenario and convert any thrown error into a recorded failure, so a crash can never
 * leave its group unrecorded (which would hide a failure from the thresholds).
 * @param {string} group recordResult group name.
 * @param {function(): boolean} fn scenario body; returns overall pass/fail.
 */
function runScenario(group, fn) {
  let ok = false;
  try {
    ok = fn();
  } catch (e) {
    console.error(JSON.stringify({ phase: 'scenario', group, error: String(e) }));
  }
  recordResult(group, ok);
}

/**
 * Guards. Fails fast if the target cluster is not allowlisted, so nothing runs elsewhere.
 * @returns {{cluster: string}} the validated cluster name.
 */
export function setup() {
  const cluster = requireEnv('TARGET_CLUSTER');
  assertClusterAllowed(cluster, requireEnv('ALLOWED_CLUSTERS'));
  console.log(JSON.stringify({ phase: 'setup', cluster, deployment: DEPLOYMENT_NAME, podSelector: POD_LABEL_SELECTOR }));
  return { cluster };
}

/** The one VU iteration. Each scenario records exactly one group result. */
export default function () {
  runScenario('sc01_deployment_ready', sc01DeploymentReady);
  runScenario('sc02_internal_health', sc02InternalHealth);
}
