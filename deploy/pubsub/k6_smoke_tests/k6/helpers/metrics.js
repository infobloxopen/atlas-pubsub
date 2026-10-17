// Shared custom Prometheus metrics for Atlas smoke tests.
// Owned by atlas.tap; assembled alongside utils.js/base-validators.js into every
// service's k6 workspace at runtime. These metric names are the fleet convention
// (athena-identity, customer.logs, notifications.mailbox all use the same) and feed
// the standard Grafana dashboards via Cortex remote-write.
import { Rate, Gauge } from 'k6/metrics';

export const smokeOverallResult  = new Rate('smoke_overall_result');
export const smokeGroupResult    = new Rate('smoke_group_result');   // tag with { test_group }
export const smokeEndpointStatus = new Gauge('smoke_endpoint_status');
export const smokeExecutionTime  = new Gauge('smoke_execution_time');

// Record one check outcome into both the overall rate and a per-group (tagged) rate.
// Call this wherever you would call k6 check() so the dashboards stay populated.
export function recordResult(group, passed) {
  const ok = !!passed;
  smokeOverallResult.add(ok);
  smokeGroupResult.add(ok, { test_group: group });
  return ok;
}
