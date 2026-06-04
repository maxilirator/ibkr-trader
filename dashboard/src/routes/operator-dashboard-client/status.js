import {
  brokerMonitor,
  executionRuntime,
  ibGateway,
  killSwitch,
  liveHealth,
  referenceNow,
  timestampFormatter
} from './view-state.js';

const fallbackTimestampFormatter = new Intl.DateTimeFormat('sv-SE', {
  timeZone: 'Europe/Stockholm',
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  timeZoneName: 'short'
});

function activeTimestampFormatter() {
  return timestampFormatter && typeof timestampFormatter.format === 'function'
    ? timestampFormatter
    : fallbackTimestampFormatter;
}

export function brokerConnected(role) {
  return liveHealth.broker_sessions[role].connected === true;
}

export function sessionStatus(role) {
  const session = liveHealth.broker_sessions[role] ?? {};
  const heartbeat = brokerMonitor?.heartbeat ?? {};
  if (heartbeat.is_stale) {
    return { label: 'Stale check', className: 'warn' };
  }
  if (heartbeat.ok === false) {
    return { label: 'Gateway failing', className: 'bad' };
  }
  if (session.connected === true) {
    return { label: 'Connected', className: 'ok' };
  }
  if (session.cooldown_seconds_remaining !== null && session.cooldown_seconds_remaining !== undefined) {
    return { label: 'Cooling down', className: 'warn' };
  }
  if (role === 'primary' && !session.last_error && Number(session.consecutive_failures ?? 0) === 0) {
    return { label: 'Idle', className: 'ok' };
  }
  return { label: 'Disconnected', className: 'bad' };
}

export function connectionLabel(role) {
  return sessionStatus(role).label;
}

export function classForConnection(role) {
  return sessionStatus(role).className;
}

export function runStatusClass(status) {
  if (status === 'CLEAN') return 'ok';
  if (status === 'WARNINGS') return 'warn';
  return 'bad';
}

export function killSwitchClass() {
  return killSwitch.enabled ? 'bad' : 'ok';
}

export function killSwitchLabel() {
  return killSwitch.enabled ? 'Enabled' : 'Disabled';
}

export function monitorLabel(status) {
  if (status?.is_stale) return 'Stale';
  if (status?.ok === true) return 'Healthy';
  if (status?.ok === false) return 'Failing';
  return 'Unknown';
}

export function monitorClass(status) {
  if (status?.is_stale) return 'warn';
  if (status?.ok === true) return 'ok';
  if (status?.ok === false) return 'bad';
  return 'warn';
}

export function ibGatewayLabel() {
  if (!ibGateway) return 'Unknown';
  const status = ibGateway.status;
  if (status === 'stuck_shutdown_after_existing_session') return 'Session Conflict';
  if (status === 'shutdown_in_progress') return 'Shutting Down';
  if (status === 'stuck_shutdown') return 'Stuck Shutdown';
  if (status === 'existing_session_detected') return 'Existing Session';
  if (status === 'restart_in_progress') return 'Restarting';
  if (status === 'second_factor') return '2FA Pending';
  if (status === 'deadlock_reported') return 'Deadlock';
  if (status === 'login_completed_after_restart_2fa') return 'Restart / 2FA OK';
  if (status === 'login_completed_with_config_warning') return 'Config Review';
  if (status === 'login_completed') return 'Login Complete';
  if (status === 'disabled') return 'Disabled';
  return status ?? 'Unknown';
}

export function ibGatewayClass() {
  if (ibGateway?.severity === 'bad') return 'bad';
  if (ibGateway?.severity === 'ok') return 'ok';
  return 'warn';
}

export function ibGatewayDetail() {
  if (!ibGateway) return 'No Gateway diagnostics have been collected.';
  const details = [ibGateway.summary].filter(Boolean);
  if (ibGateway.latest_dialog) details.push(`Dialog: ${ibGateway.latest_dialog}`);
  if (ibGateway.existing_session_detected_at) {
    details.push(`Existing session ${formatTimestamp(ibGateway.existing_session_detected_at)}`);
  }
  for (const warning of ibGateway.configuration_warnings ?? []) {
    details.push(warning);
  }
  if (ibGateway.latest_event_at) {
    details.push(`Latest ${formatTimestamp(ibGateway.latest_event_at)}`);
  }
  if (ibGateway.error) details.push(ibGateway.error);
  return details.join(' · ') || ibGateway.status || 'No recent Gateway UI state.';
}

export function executionRuntimeLabel() {
  return executionRuntime?.effective_status ?? executionRuntime?.status ?? 'Unknown';
}

export function executionRuntimeClass() {
  const status = executionRuntime?.effective_status ?? executionRuntime?.status;
  if (!status) return 'warn';
  if (status === 'RUNNING') return 'ok';
  if (status === 'DEGRADED') return 'warn';
  if (status === 'STALE') return 'bad';
  if (status === 'STOPPED' || status === 'DISABLED') return 'warn';
  return 'bad';
}

export function parseTimestamp(value) {
  if (!value) return null;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export function formatTimestamp(value) {
  const parsed = parseTimestamp(value);
  if (!parsed) {
    return value ?? 'n/a';
  }
  return activeTimestampFormatter().format(parsed);
}

export function formatTimestampOrNull(value) {
  const parsed = parseTimestamp(value);
  if (!parsed) {
    return null;
  }
  return activeTimestampFormatter().format(parsed);
}

export function ageSeconds(value) {
  const parsed = parseTimestamp(value);
  if (!parsed) return null;
  return Math.max(0, Math.round((referenceNow.getTime() - parsed.getTime()) / 1000));
}

export function formatAge(value) {
  const seconds = ageSeconds(value);
  if (seconds === null) return 'no timestamp';
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  const remainder = minutes % 60;
  return remainder > 0 ? `${hours}h ${remainder}m ago` : `${hours}h ago`;
}

export function latestTimestamp(rows, fields) {
  const fieldNames = Array.isArray(fields) ? fields : [fields];
  let latest = null;
  for (const row of rows ?? []) {
    for (const fieldName of fieldNames) {
      const parsed = parseTimestamp(row?.[fieldName]);
      if (!parsed) continue;
      if (!latest || parsed.getTime() > latest.getTime()) {
        latest = parsed;
      }
    }
  }
  return latest ? latest.toISOString() : null;
}

export function freshnessClass(value, maxAgeSeconds = 180) {
  const seconds = ageSeconds(value);
  if (seconds === null) return 'bad';
  if (seconds <= maxAgeSeconds) return 'ok';
  if (seconds <= maxAgeSeconds * 4) return 'warn';
  return 'bad';
}

export function compactCount(value) {
  return Number.isFinite(Number(value)) ? String(value) : 'n/a';
}

export function normalizedSymbol(value) {
  return String(value ?? '').trim().toUpperCase().replace(/\s+/g, '-').replace(/[._]/g, '-');
}

export function instrumentKeys(row) {
  const account = String(row?.account_key ?? '').trim().toUpperCase();
  const symbols = [
    normalizedSymbol(row?.symbol),
    normalizedSymbol(row?.local_symbol),
    normalizedSymbol(row?.primary_exchange ? `${row.primary_exchange}:${row.symbol}` : null)
  ].filter(Boolean);
  return symbols.map((symbol) => `${account}|${symbol}`);
}
