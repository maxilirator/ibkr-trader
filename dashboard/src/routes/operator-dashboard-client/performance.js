import { omxBenchmark, referenceNow } from './view-state.js';
import { parseTimestamp } from './status.js';
import {
  formatPlainNumber,
  formatSignedDecimal,
  parseFiniteNumber
} from './formatting.js';

export function normalizePerformancePoints(points, valueField = 'return_pct') {
  return (points ?? [])
    .map((point) => {
      const timestamp = parseTimestamp(point.timestamp ?? point.snapshot_at);
      const value = parseFiniteNumber(point[valueField]);
      if (!timestamp || value === null) {
        return null;
      }
      return {
        timestamp,
        value
      };
    })
    .filter(Boolean);
}

export function stockholmDstStartUtc(year) {
  const lastDay = new Date(Date.UTC(year, 2, 31));
  const lastSunday = 31 - lastDay.getUTCDay();
  return Date.UTC(year, 2, lastSunday, 1, 0, 0);
}

export function stockholmDstEndUtc(year) {
  const lastDay = new Date(Date.UTC(year, 9, 31));
  const lastSunday = 31 - lastDay.getUTCDay();
  return Date.UTC(year, 9, lastSunday, 1, 0, 0);
}

export function stockholmOffsetHours(year, month, day, hour, minute, second = 0) {
  const standardUtc = Date.UTC(year, month - 1, day, hour - 1, minute, second);
  return standardUtc >= stockholmDstStartUtc(year) && standardUtc < stockholmDstEndUtc(year)
    ? 2
    : 1;
}

export function stockholmDateKey(date) {
  const parts = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Europe/Stockholm',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(date);
  const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${byType.year}-${byType.month}-${byType.day}`;
}

export function stockholmLocalDate(dateKey, hour, minute) {
  const [year, month, day] = dateKey.split('-').map((part) => Number.parseInt(part, 10));
  const offsetHours = stockholmOffsetHours(year, month, day, hour, minute);
  return new Date(Date.UTC(year, month - 1, day, hour - offsetHours, minute, 0));
}

export function sessionWindowForPoints(points) {
  const latestPoint = points
    .map((point) => point.timestamp)
    .filter((timestamp) => timestamp instanceof Date && !Number.isNaN(timestamp.getTime()))
    .sort((left, right) => left.getTime() - right.getTime())
    .at(-1);
  const dateKey = stockholmDateKey(latestPoint ?? referenceNow);
  return {
    dateKey,
    open: stockholmLocalDate(dateKey, 9, 0),
    close: stockholmLocalDate(dateKey, 17, 30)
  };
}

export function tradingSessionPoints(points, session) {
  const openTime = session.open.getTime();
  const closeTime = session.close.getTime();
  return points
    .filter((point) => {
      const timestamp = point.timestamp.getTime();
      return timestamp >= openTime && timestamp <= closeTime;
    })
    .sort((left, right) => left.timestamp.getTime() - right.timestamp.getTime());
}

export function anchorSessionSeries(points, session, { extendToClose = false } = {}) {
  const sessionPoints = tradingSessionPoints(points, session);
  if (sessionPoints.length === 0) {
    return [];
  }
  const anchorValue = sessionPoints[0].value;
  const anchored = sessionPoints.map((point) => ({
    timestamp: point.timestamp,
    value: point.value - anchorValue
  }));
  if (anchored[0].timestamp.getTime() !== session.open.getTime()) {
    anchored.unshift({
      timestamp: session.open,
      value: 0
    });
  } else {
    anchored[0] = {
      timestamp: session.open,
      value: 0
    };
  }
  const latest = anchored.at(-1);
  if (extendToClose && latest && latest.timestamp.getTime() < session.close.getTime()) {
    anchored.push({
      timestamp: session.close,
      value: latest.value
    });
  }
  return anchored;
}

export function interpolateSeriesValue(points, timestamp) {
  if (points.length === 0) {
    return null;
  }
  const time = timestamp.getTime();
  if (time <= points[0].timestamp.getTime()) {
    return points[0].value;
  }
  for (let index = 1; index < points.length; index += 1) {
    const previous = points[index - 1];
    const next = points[index];
    const previousTime = previous.timestamp.getTime();
    const nextTime = next.timestamp.getTime();
    if (time <= nextTime) {
      if (nextTime === previousTime) {
        return next.value;
      }
      const fraction = (time - previousTime) / (nextTime - previousTime);
      return previous.value + (next.value - previous.value) * fraction;
    }
  }
  return points.at(-1).value;
}

export function accountDayChart(account) {
  const rawAccountPoints = normalizePerformancePoints(account.day_performance?.points);
  const rawBenchmarkPoints = normalizePerformancePoints(omxBenchmark?.points);
  const session = sessionWindowForPoints([...rawAccountPoints, ...rawBenchmarkPoints]);
  const extendToClose = referenceNow.getTime() >= session.close.getTime();
  const accountPoints = anchorSessionSeries(rawAccountPoints, session, { extendToClose });
  const benchmarkPoints = anchorSessionSeries(rawBenchmarkPoints, session, { extendToClose });
  if (accountPoints.length < 2) {
    return {
      ready: false,
      message: 'Waiting for at least two account snapshots from this trading session.'
    };
  }

  const width = 320;
  const height = 120;
  const left = 12;
  const right = 308;
  const top = 12;
  const bottom = 98;
  const openTime = session.open.getTime();
  const closeTime = session.close.getTime();
  const benchmarkAvailable = benchmarkPoints.length >= 2 && omxBenchmark?.status === 'ok';
  const relativePoints = accountPoints.map((point) => {
    const benchmarkValue = benchmarkAvailable
      ? interpolateSeriesValue(benchmarkPoints, point.timestamp)
      : 0;
    return {
      timestamp: point.timestamp,
      value: point.value - (benchmarkValue ?? 0)
    };
  });
  const yValues = accountPoints
    .map((point) => point.value)
    .concat(benchmarkAvailable ? benchmarkPoints.map((point) => point.value) : [])
    .concat(0);
  const maxAbsValue = Math.max(...yValues.map((value) => Math.abs(value)), 0.05) * 1.18;
  const minValue = -maxAbsValue;
  const maxValue = maxAbsValue;

  const xFor = (date) => {
    if (closeTime === openTime) return left;
    const clampedTime = Math.min(Math.max(date.getTime(), openTime), closeTime);
    return left + ((clampedTime - openTime) / (closeTime - openTime)) * (right - left);
  };
  const yFor = (value) => bottom - ((value - minValue) / (maxValue - minValue)) * (bottom - top);
  const pathFor = (points) =>
    points
      .map((point, index) => `${index === 0 ? 'M' : 'L'} ${xFor(point.timestamp).toFixed(2)} ${yFor(point.value).toFixed(2)}`)
      .join(' ');
  const latestAccountRaw = accountPoints.at(-1)?.value ?? null;
  const latestBenchmarkRaw = benchmarkAvailable
    ? interpolateSeriesValue(benchmarkPoints, accountPoints.at(-1).timestamp)
    : null;
  const latestRelative = relativePoints.at(-1)?.value ?? null;
  const zeroPath = `M ${left} ${yFor(0).toFixed(2)} L ${right} ${yFor(0).toFixed(2)}`;

  return {
    ready: true,
    accountPath: pathFor(accountPoints),
    benchmarkPath: benchmarkAvailable ? pathFor(benchmarkPoints) : null,
    zeroPath,
    yMin: minValue,
    yMax: maxValue,
    latestAccount: latestAccountRaw,
    latestBenchmark: latestBenchmarkRaw,
    latestRelative,
    benchmarkAvailable,
    benchmarkLabel: omxBenchmark?.symbol ?? 'OMX',
    openLabel: '09:00',
    closeLabel: '17:30'
  };
}

