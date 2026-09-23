import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const manifestPath = __ENV.RUN_MANIFEST;
if (!manifestPath) throw new Error('RUN_MANIFEST is required; run tools.collector_load prepare first');
const manifest = JSON.parse(open(manifestPath));
if (manifest.schemaVersion !== 1 || manifest.ruleCode !== 'PROC_POWERSHELL_ENCODED' || manifest.ruleVersion !== 2) {
  throw new Error('unsupported run manifest or detection rule');
}
if (!Array.isArray(manifest.events) || manifest.events.length < 1 || manifest.events.length > 10000) {
  throw new Error('run manifest must contain 1-10000 events');
}
if (!/^[a-z0-9][a-z0-9._-]{0,63}$/.test(manifest.agentId)) throw new Error('invalid agent ID');

const collectorUrl = (__ENV.COLLECTOR_URL || 'https://127.0.0.1:8443/api/v1/collector').replace(/\/$/, '');
if (!/^https:\/\/(127\.0\.0\.1|localhost):\d{1,5}\/api\/v1\/collector$/.test(collectorUrl)) {
  throw new Error('Collector URL must be the local mTLS endpoint');
}

function integerOption(value, fallback, min, max, name) {
  const parsed = value === undefined ? fallback : Number(value);
  if (!Number.isInteger(parsed) || parsed < min || parsed > max) throw new Error(`invalid ${name}`);
  return parsed;
}

const vus = integerOption(__ENV.VUS, 1, 1, Math.min(50, manifest.events.length), 'VUS');
const duplicateEvery = integerOption(__ENV.DUPLICATE_EVERY, 0, 0, manifest.events.length, 'DUPLICATE_EVERY');
const certRoot = `../../runtime/compose/cert-authority/agents/${manifest.agentId}`;

export const options = {
  scenarios: {
    collector: {
      executor: 'shared-iterations',
      vus,
      iterations: manifest.events.length,
      maxDuration: '30m',
    },
  },
  tlsAuth: [{ cert: open(`${certRoot}/agent.crt`), key: open(`${certRoot}/agent.key`) }],
  // Only opt in after verifying the local server certificate with the Compose CA.
  insecureSkipTLSVerify: __ENV.LOCAL_SKIP_TLS_VERIFY === '1',
  thresholds: { checks: ['rate==1'], http_req_failed: ['rate==0'] },
};

const jsonHeaders = { 'Content-Type': 'application/json' };

export function setup() {
  const response = http.post(`${collectorUrl}/agents/register`, JSON.stringify({
    agentId: manifest.agentId,
    hostname: 'EDR-LOAD-LOCAL',
    osType: 'WINDOWS',
    osVersion: '11',
    agentVersion: '0.1.0',
    agentBuildId: 'local-k6',
    agentArch: 'X64',
    capabilityCodes: ['PROCESS_EXECUTION'],
  }), { headers: jsonHeaders, tags: { name: 'collector_register' }, timeout: '15s' });
  let registration;
  try { registration = response.json(); } catch (_) { registration = null; }
  if ((response.status !== 200 && response.status !== 201) || !registration?.data?.endpointId) {
    throw new Error(`Agent registration failed (HTTP ${response.status})`);
  }
  console.log(`runId=${manifest.runId} events=${manifest.events.length} vus=${vus} duplicateEvery=${duplicateEvery}`);
}

function send(body, eventId) {
  const response = http.post(`${collectorUrl}/telemetry/batches`, body, {
    headers: jsonHeaders,
    tags: { name: 'collector_telemetry' },
    timeout: '15s',
  });
  let data;
  try { data = response.json()?.data; } catch (_) { data = null; }
  const accepted = response.status === 200
    && Array.isArray(data?.acceptedEventIds)
    && data.acceptedEventIds.includes(eventId)
    && Array.isArray(data.rejectedEvents)
    && data.rejectedEvents.length === 0;
  check(response, { 'event accepted': () => accepted });
  if (!accepted) console.error(`collector did not accept eventId=${eventId} status=${response.status}`);
}

export default function () {
  const index = exec.scenario.iterationInTest;
  const ids = manifest.events[index];
  if (!ids) throw new Error(`missing event IDs for iteration ${index}`);
  const timestamp = new Date().toISOString();
  const body = JSON.stringify({
    schemaVersion: 1,
    batchId: ids.batchId,
    agentId: manifest.agentId,
    sentAt: timestamp,
    events: [{
      eventId: ids.eventId,
      eventType: 'PROCESS_EXECUTION',
      occurredAt: timestamp,
      payload: {
        processName: 'powershell.exe',
        pid: 1000 + (index % 30000),
        commandLine: 'powershell.exe -EncodedCommand ZQBjAGgAbwA=',
      },
    }],
  });
  send(body, ids.eventId);
  if (duplicateEvery > 0 && (index + 1) % duplicateEvery === 0) send(body, ids.eventId);
}
