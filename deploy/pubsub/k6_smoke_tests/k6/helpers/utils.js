// Shared K6 utilities for Atlas smoke tests.
// Owned by atlas.tap; assembled into every service's k6 workspace at runtime
// by the Job init container. Service-specific config/validators live in the service repo.
import http from 'k6/http';
import { fail } from 'k6';

export const parseBody = (r) => {
  try {
    // A failed request (status 0) has a null body: JSON.parse(null) is null, so fall back to {}.
    return JSON.parse(r.body) || {};
  } catch (e) {
    return {};
  }
};

export function logRequest(phase, endpoint, method, url, payload, response, actualHeaders) {
  let bodyOut = '';
  try {
    bodyOut = response && response.body ? String(response.body) : '';
    if (bodyOut.length > 2000) bodyOut = bodyOut.slice(0, 2000) + '...[truncated]';
  } catch (e) {
    bodyOut = '';
  }
  let reqHeaders;
  if (actualHeaders && typeof actualHeaders === 'object') {
    reqHeaders = {};
    for (const k of Object.keys(actualHeaders)) {
      reqHeaders[k] = k.toLowerCase() === 'authorization' ? 'Bearer ***redacted***' : actualHeaders[k];
    }
  } else {
    reqHeaders = {};
  }
  console.log(JSON.stringify({
    phase,
    endpoint,
    method,
    url,
    requestHeaders: reqHeaders,
    requestPayload: payload || null,
    status: response ? response.status : null,
    responseBody: bodyOut,
  }));
}

export function logSkip(scenarioId, endpoint, reason) {
  console.log(JSON.stringify({ phase: 'execution', scenario: scenarioId, endpoint, skipped: true, reason }));
}

// Generic authenticated GET — headers supplied by the service config
export function httpGet(url, headers, endpointName) {
  const r = http.get(url, { timeout: '30s', headers });
  logRequest('execution', endpointName, 'GET', url, null, r, headers);
  return r;
}

// Generic POST — used for auth flows (e.g. /v2/session/users/sign_in)
export function httpPost(url, payload, headers, endpointName) {
  const body = typeof payload === 'string' ? payload : JSON.stringify(payload);
  const r = http.post(url, body, { timeout: '30s', headers });
  logRequest('execution', endpointName, 'POST', url, payload, r, headers);
  return r;
}

// Returns ISO 8601 timestamp offset by `offsetMs` milliseconds from now
export function isoTimestamp(offsetMs = 0) {
  return new Date(Date.now() + offsetMs).toISOString();
}

// ---- Base64URL decode (for JWT payload inspection) ----
// Borrowed from the spire-oidc smoke tests — k6 has no native base64url.
export function b64urlDecode(input) {
  if (!input) return '';
  let s = String(input).replace(/-/g, '+').replace(/_/g, '/');
  while (s.length % 4 !== 0) s += '=';
  try {
    if (typeof atob === 'function') return atob(s);
  } catch (e) { /* fallthrough */ }
  // ASCII-only manual fallback (sufficient for JWT JSON payloads)
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
  let out = '';
  let buf = 0;
  let bits = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c === '=') break;
    const idx = chars.indexOf(c);
    if (idx < 0) continue;
    buf = (buf << 6) | idx;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out += String.fromCharCode((buf >> bits) & 0xff);
    }
  }
  return out;
}

export function decodeJwtPayload(jwt) {
  if (!jwt || typeof jwt !== 'string') return null;
  const parts = jwt.split('.');
  if (parts.length < 2) return null;
  try {
    return JSON.parse(b64urlDecode(parts[1]));
  } catch (e) {
    return null;
  }
}

// ---- Environment & cluster guards (fleet-universal) ----
// Fail-fast presence check for a required env var; returns the trimmed value.
// This is how the fleet guards credentials — plaintext env/Secret + presence check
// (NOT base64 encoding). base64 decoding of S2S tokens happens in the Helm init
// container shell (`... | base64 -d`), never in k6.
export function requireEnv(name) {
  const v = (__ENV[name] || '').trim();
  if (!v) fail(`Required env var ${name} is not set`);
  return v;
}

// Aborts the run if the target cluster is not in the allowlist — the blast-radius
// guardrail present in every service's setup()/validateConfig().
export function assertClusterAllowed(cluster, allowlist) {
  const allowed = Array.isArray(allowlist)
    ? allowlist
    : String(allowlist || '').split(',').map((s) => s.trim()).filter(Boolean);
  if (!allowed.includes((cluster || '').trim())) {
    fail(`Cluster guardrail: '${cluster}' is not in the allowlist [${allowed.join(', ')}]`);
  }
}

// ---- Run identity & resource safety (fleet-universal) ----
// Per-run unique id for prefixing/tracking created resources. Default prefix matches the
// AUTO_TEST_ convention enforced by assertAutoTestResource.
export function generateRunId(prefix = 'AUTO_TEST') {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
}

// Hard guardrail: throws before any UPDATE/DELETE unless the resource carries the
// AUTO_TEST_ prefix — prevents mutating real data. (See base-validators isAutoTestResource
// for the non-throwing predicate.)
export function assertAutoTestResource(value, operation = 'mutate') {
  if (typeof value !== 'string' || !value.startsWith('AUTO_TEST_')) {
    fail(`GUARDRAIL VIOLATION: refusing to ${operation} non-AUTO_TEST_ resource: ${value}`);
  }
}

// ---- Header builders (fleet-universal) ----
// Compose with any verb via httpGet/httpPost or direct http.*. This is how every service
// constructs auth — the fleet has NO per-verb GET wrappers. Services that use a non-Bearer
// scheme (e.g. KONK OBO headers, mTLS) define their own builders on top of these.
export const authHeaders        = (token) => ({ Authorization: `Bearer ${token}`, Accept: 'application/json' });
export const jsonHeaders        = (token) => ({ Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', Accept: 'application/json' });
export const noAuthHeaders      = () => ({ Accept: 'application/json' });
export const invalidAuthHeaders = () => ({ Authorization: 'Bearer INVALID_TOKEN_AUTO_TEST', Accept: 'application/json' });
