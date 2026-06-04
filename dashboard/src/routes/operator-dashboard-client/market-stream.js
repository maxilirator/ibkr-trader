import { marketStreamMarks } from './view-state.js';
import { parseTimestamp } from './status.js';
import {
  firstFinite,
  formatPlainNumber,
  formatSignedDecimal,
  parseFiniteNumber
} from './formatting.js';

export function streamPayload(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') {
    return {};
  }
  return snapshot.stream && typeof snapshot.stream === 'object' ? snapshot.stream : snapshot;
}

export function streamSymbolKeys(symbol) {
  const normalized = String(symbol ?? '').trim().toUpperCase();
  if (!normalized) {
    return [];
  }
  const keys = new Set([normalized]);
  if (normalized.includes('-')) {
    keys.add(normalized.replaceAll('-', ' '));
  }
  if (normalized.includes(' ')) {
    keys.add(normalized.replaceAll(' ', '-'));
  }
  return [...keys];
}

export function streamRowSymbol(row) {
  return row?.symbol ?? row?.local_symbol ?? '';
}

export function streamQuotePrice(quote) {
  const bid = parseFiniteNumber(quote?.bid_price);
  const ask = parseFiniteNumber(quote?.ask_price);
  const bidAskMidpoint = bid !== null && ask !== null ? (bid + ask) / 2 : null;
  return firstFinite([
    quote?.last_price,
    bidAskMidpoint,
    quote?.midpoint_price,
    quote?.close_price,
    bid,
    ask
  ]);
}

export function latestStreamBar(bars) {
  if (!Array.isArray(bars)) {
    return null;
  }
  for (let index = bars.length - 1; index >= 0; index -= 1) {
    if (bars[index] && typeof bars[index] === 'object') {
      return bars[index];
    }
  }
  return null;
}

export function previousStreamBar(bars) {
  if (!Array.isArray(bars)) {
    return null;
  }
  let seenLatest = false;
  for (let index = bars.length - 1; index >= 0; index -= 1) {
    if (!bars[index] || typeof bars[index] !== 'object') {
      continue;
    }
    if (!seenLatest) {
      seenLatest = true;
      continue;
    }
    return bars[index];
  }
  return null;
}

export function streamTimestamp(value) {
  const parsed = parseTimestamp(value);
  return parsed ? parsed.toISOString() : null;
}

export function streamQuoteForSymbol(snapshot, symbol) {
  const stream = streamPayload(snapshot);
  const quotes = Array.isArray(stream.quotes) ? stream.quotes : [];
  const keys = new Set(streamSymbolKeys(symbol));
  return quotes.find((quote) => keys.has(String(quote?.symbol ?? '').trim().toUpperCase())) ?? null;
}

export function stockholmDateKeyForTimestamp(timestamp) {
  const parsed = parseTimestamp(timestamp);
  if (!parsed) {
    return null;
  }
  const parts = new Intl.DateTimeFormat('sv-SE', {
    timeZone: 'Europe/Stockholm',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).formatToParts(parsed);
  const byType = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${byType.year}-${byType.month}-${byType.day}`;
}

export function stockholmOffsetHours(year, month, day, hour, minute, second = 0) {
  const standardUtc = Date.UTC(year, month - 1, day, hour - 1, minute, second);
  const dstStart = (() => {
    const lastDay = new Date(Date.UTC(year, 2, 31));
    const lastSunday = 31 - lastDay.getUTCDay();
    return Date.UTC(year, 2, lastSunday, 1, 0, 0);
  })();
  const dstEnd = (() => {
    const lastDay = new Date(Date.UTC(year, 9, 31));
    const lastSunday = 31 - lastDay.getUTCDay();
    return Date.UTC(year, 9, lastSunday, 1, 0, 0);
  })();
  return standardUtc >= dstStart && standardUtc < dstEnd ? 2 : 1;
}

export function stockholmSessionOpenForTimestamp(timestamp) {
  const dateKey = stockholmDateKeyForTimestamp(timestamp);
  if (!dateKey) {
    return null;
  }
  const [year, month, day] = dateKey.split('-').map((part) => Number.parseInt(part, 10));
  const offsetHours = stockholmOffsetHours(year, month, day, 9, 0);
  return new Date(Date.UTC(year, month - 1, day, 9 - offsetHours, 0, 0)).toISOString();
}

export function latestBenchmarkPointFromQuote(quote) {
  const latest = parseFiniteNumber(quote?.last_price);
  const timestamp = streamTimestamp(quote?.last_trade_at ?? quote?.updated_at);
  if (latest === null || !timestamp) {
    return null;
  }
  return { timestamp, value: latest };
}

export function buildMarketStreamMarks(snapshot) {
  const stream = streamPayload(snapshot);
  const quoteBySymbol = new Map();
  const barsBySymbol = new Map();
  const quotes = Array.isArray(stream.quotes) ? stream.quotes : [];
  const rawBarsBySymbol =
    stream.bars_by_symbol && typeof stream.bars_by_symbol === 'object'
      ? stream.bars_by_symbol
      : {};

  for (const quote of quotes) {
    if (!quote || typeof quote !== 'object') {
      continue;
    }
    for (const key of streamSymbolKeys(quote.symbol)) {
      quoteBySymbol.set(key, quote);
    }
  }

  for (const [symbol, bars] of Object.entries(rawBarsBySymbol)) {
    for (const key of streamSymbolKeys(symbol)) {
      barsBySymbol.set(key, Array.isArray(bars) ? bars : []);
    }
  }

  const marks = new Map();
  const keys = new Set([...quoteBySymbol.keys(), ...barsBySymbol.keys()]);
  for (const key of keys) {
    const quote = quoteBySymbol.get(key);
    const bars = barsBySymbol.get(key) ?? [];
    const latestBar = latestStreamBar(bars);
    const previousBar = previousStreamBar(bars);
    let price = quote ? streamQuotePrice(quote) : null;
    let source = 'quote';
    let observedAt = quote
      ? streamTimestamp(quote.last_trade_at ?? quote.updated_at)
      : null;

    if (price === null && latestBar) {
      price = parseFiniteNumber(latestBar.close);
      observedAt = streamTimestamp(latestBar.timestamp);
      source = 'bar';
    } else if (!observedAt && latestBar) {
      observedAt = streamTimestamp(latestBar.timestamp);
    }
    if (price === null) {
      continue;
    }

    let previousPrice = previousBar ? parseFiniteNumber(previousBar.close) : null;
    if (previousPrice === null && quote) {
      previousPrice = parseFiniteNumber(quote.close_price);
    }
    let direction = null;
    if (previousPrice !== null) {
      direction = price > previousPrice ? 'UP' : price < previousPrice ? 'DOWN' : 'UNCHANGED';
    }
    const canonicalSymbol = String(quote?.symbol ?? key).trim().toUpperCase();
    const mark = {
      symbol: canonicalSymbol,
      price,
      previous_price: previousPrice,
      observed_at: observedAt,
      source,
      direction
    };
    for (const candidate of [...streamSymbolKeys(key), ...streamSymbolKeys(canonicalSymbol)]) {
      marks.set(candidate, mark);
    }
  }
  return marks;
}

export function marketMarkForRow(row, marks = marketStreamMarks) {
  for (const key of streamSymbolKeys(streamRowSymbol(row))) {
    const mark = marks.get(key);
    if (mark) {
      return mark;
    }
  }
  return null;
}

export function applyMarketStreamToPositions(basePositions, marks) {
  if (!marks || marks.size === 0) {
    return basePositions ?? [];
  }
  let changed = false;
  const rows = (basePositions ?? []).map((position) => {
    const quantity = parseFiniteNumber(position.quantity);
    const mark = marketMarkForRow(position, marks);
    if (quantity === null || mark === null) {
      return position;
    }
    const averageCost = parseFiniteNumber(position.average_cost);
    const marketValue = quantity * mark.price;
    const unrealizedPnl =
      averageCost !== null ? quantity * (mark.price - averageCost) : null;
    changed = true;
    return {
      ...position,
      market_price: formatPlainNumber(mark.price),
      market_value: formatPlainNumber(marketValue),
      unrealized_pnl: formatPlainNumber(unrealizedPnl),
      market_price_at: mark.observed_at,
      market_data_source: 'market_stream'
    };
  });
  return changed ? rows : (basePositions ?? []);
}

export function latestIsoTimestamp(left, right) {
  const leftDate = parseTimestamp(left);
  const rightDate = parseTimestamp(right);
  if (!leftDate) {
    return rightDate ? rightDate.toISOString() : null;
  }
  if (!rightDate) {
    return leftDate.toISOString();
  }
  return leftDate.getTime() >= rightDate.getTime()
    ? leftDate.toISOString()
    : rightDate.toISOString();
}

export function enrichAccountDayPerformance(account, netLiquidation, markedAt) {
  const markedDate = parseTimestamp(markedAt);
  const dayPerformance = account.day_performance;
  if (!markedDate || !dayPerformance || typeof dayPerformance !== 'object') {
    return account;
  }
  const points = Array.isArray(dayPerformance.points) ? [...dayPerformance.points] : [];
  let startValue = parseFiniteNumber(dayPerformance.start_net_liquidation);
  if (startValue === null && points.length > 0) {
    startValue = parseFiniteNumber(points[0]?.net_liquidation);
  }
  if (startValue === null || startValue === 0) {
    return account;
  }

  const latestReturn = ((netLiquidation - startValue) / startValue) * 100;
  const point = {
    snapshot_at: markedDate.toISOString(),
    net_liquidation: formatPlainNumber(netLiquidation),
    return_pct: formatSignedDecimal(latestReturn) ?? '0.00'
  };
  const latestPointAt = parseTimestamp(points.at(-1)?.snapshot_at ?? points.at(-1)?.timestamp);
  if (!latestPointAt || markedDate.getTime() > latestPointAt.getTime()) {
    points.push(point);
  } else if (markedDate.getTime() === latestPointAt.getTime()) {
    points[points.length - 1] = point;
  }

  return {
    ...account,
    day_performance: {
      ...dayPerformance,
      latest_at: markedDate.toISOString(),
      latest_net_liquidation: formatPlainNumber(netLiquidation),
      latest_return_pct: formatSignedDecimal(latestReturn),
      points
    }
  };
}

export function incrementMap(map, key, amount = 1) {
  map.set(key, (map.get(key) ?? 0) + amount);
}

export function applyMarketStreamToAccounts(baseAccounts, basePositions, marks) {
  const accountsToMark = baseAccounts ?? [];
  if (!marks || marks.size === 0) {
    return accountsToMark;
  }

  const virtualAccounts = new Set(
    accountsToMark
      .filter((account) => account?.is_virtual)
      .map((account) => String(account.account_key ?? ''))
  );
  const accountPositionCounts = new Map();
  const accountMarkedPositionCounts = new Map();
  const accountStreamMarketValues = new Map();
  const accountDeltas = new Map();
  const accountLatestAt = new Map();

  for (const position of basePositions ?? []) {
    const accountKey = String(position?.account_key ?? '');
    const quantity = parseFiniteNumber(position?.quantity);
    if (!accountKey || quantity === null || quantity === 0) {
      continue;
    }
    incrementMap(accountPositionCounts, accountKey);
    const mark = marketMarkForRow(position, marks);
    if (!mark) {
      continue;
    }

    const marketValue = quantity * mark.price;
    let oldMarketValue = parseFiniteNumber(position.market_value);
    const oldMarketValueWasAvailable = oldMarketValue !== null;
    if (oldMarketValue === null) {
      const oldMarketPrice = parseFiniteNumber(position.market_price);
      oldMarketValue = oldMarketPrice !== null ? quantity * oldMarketPrice : 0;
    }
    incrementMap(accountMarkedPositionCounts, accountKey);
    incrementMap(accountStreamMarketValues, accountKey, marketValue);
    if (mark.observed_at) {
      accountLatestAt.set(
        accountKey,
        latestIsoTimestamp(accountLatestAt.get(accountKey), mark.observed_at)
      );
    }
    if (virtualAccounts.has(accountKey) || (oldMarketValueWasAvailable && oldMarketValue !== 0)) {
      incrementMap(accountDeltas, accountKey, marketValue - oldMarketValue);
    }
  }

  let changed = false;
  const rows = accountsToMark.map((account) => {
    const accountKey = String(account?.account_key ?? '');
    const currentNet = parseFiniteNumber(account?.net_liquidation);
    if (!accountKey || currentNet === null) {
      return account;
    }

    let valuationMethod = 'mark_delta';
    let delta = accountDeltas.get(accountKey);
    let streamNet = null;
    if (
      !virtualAccounts.has(accountKey) &&
      (accountPositionCounts.get(accountKey) ?? 0) > 0 &&
      accountMarkedPositionCounts.get(accountKey) === accountPositionCounts.get(accountKey)
    ) {
      const cashValue = parseFiniteNumber(account.total_cash_value);
      if (cashValue !== null) {
        streamNet = cashValue + (accountStreamMarketValues.get(accountKey) ?? 0);
        delta = streamNet - currentNet;
        valuationMethod = 'cash_plus_stream_positions';
      }
    }
    if (streamNet === null && delta !== undefined) {
      streamNet = currentNet + delta;
    }
    if (streamNet === null) {
      return account;
    }

    changed = true;
    const markedAt = accountLatestAt.get(accountKey) ?? null;
    const nextAccount = {
      ...account,
      net_liquidation: formatPlainNumber(streamNet),
      stream_valuation: {
        source: 'market_stream',
        method: valuationMethod,
        base_net_liquidation: formatPlainNumber(currentNet),
        mark_delta: formatPlainNumber(delta),
        stream_position_market_value: formatPlainNumber(accountStreamMarketValues.get(accountKey)),
        marked_at: markedAt
      }
    };
    return enrichAccountDayPerformance(nextAccount, streamNet, markedAt);
  });
  return changed ? rows : accountsToMark;
}

export function workingOrderPrice(order) {
  return firstFinite([order?.working_price, order?.limit_price, order?.stop_price]);
}

export function applyMarketStreamToOpenOrders(baseOpenOrders, marks) {
  if (!marks || marks.size === 0) {
    return baseOpenOrders ?? [];
  }
  let changed = false;
  const rows = (baseOpenOrders ?? []).map((order) => {
    const mark = marketMarkForRow(order, marks);
    if (!mark) {
      return order;
    }
    const workingPrice = workingOrderPrice(order);
    const nextOrder = {
      ...order,
      reference_market_price: formatPlainNumber(mark.price),
      reference_market_price_at: mark.observed_at,
      last_market_price_direction: mark.direction,
      market_data_source: 'market_stream'
    };
    if (workingPrice !== null) {
      const spread = workingPrice - mark.price;
      nextOrder.price_spread = formatSignedDecimal(spread);
      nextOrder.price_spread_pct =
        mark.price !== 0 ? formatSignedDecimal((spread / mark.price) * 100) : null;
      nextOrder.spread_reference =
        order.working_price_reference ?? (order.limit_price ? 'LIMIT' : 'STOP');
    }
    changed = true;
    return nextOrder;
  });
  return changed ? rows : (baseOpenOrders ?? []);
}

export function streamBarsForSymbol(snapshot, symbol) {
  const stream = streamPayload(snapshot);
  const barsBySymbol =
    stream.bars_by_symbol && typeof stream.bars_by_symbol === 'object'
      ? stream.bars_by_symbol
      : {};
  for (const key of streamSymbolKeys(symbol)) {
    const bars = barsBySymbol[key];
    if (Array.isArray(bars)) {
      return bars;
    }
  }
  return [];
}

export function buildLiveOmxBenchmark(fallbackBenchmark, snapshot) {
  const bars = streamBarsForSymbol(snapshot, 'OMXS30');
  const quote = streamQuoteForSymbol(snapshot, 'OMXS30');
  const validBars = bars
    .map((bar) => ({
      timestamp: streamTimestamp(bar?.timestamp),
      value: parseFiniteNumber(bar?.close)
    }))
    .filter((bar) => bar.timestamp && bar.value !== null);
  const fallbackPoints = Array.isArray(fallbackBenchmark?.points)
    ? fallbackBenchmark.points
        .map((point) => ({
          timestamp: streamTimestamp(point?.timestamp),
          value: parseFiniteNumber(point?.value)
        }))
        .filter((point) => point.timestamp && point.value !== null)
    : [];
  const quotePoint = latestBenchmarkPointFromQuote(quote);
  const mergedByTimestamp = new Map();
  for (const point of [...fallbackPoints, ...validBars, quotePoint].filter(Boolean)) {
    mergedByTimestamp.set(point.timestamp, point);
  }
  const allMergedPoints = [...mergedByTimestamp.values()].sort(
    (left, right) => new Date(left.timestamp).getTime() - new Date(right.timestamp).getTime()
  );
  const latestDateKey = stockholmDateKeyForTimestamp(allMergedPoints.at(-1)?.timestamp);
  const mergedPoints = allMergedPoints
    .filter(
      (point) => !latestDateKey || stockholmDateKeyForTimestamp(point.timestamp) === latestDateKey
    )
    .filter((point) => point.value !== 0);
  const previousClose = parseFiniteNumber(quote?.close_price);
  const baseline = previousClose ?? mergedPoints[0]?.value ?? null;
  if (baseline === null || baseline === 0) {
    return fallbackBenchmark;
  }

  const anchorTimestamp =
    previousClose !== null
      ? stockholmSessionOpenForTimestamp(mergedPoints.at(-1)?.timestamp ?? quotePoint?.timestamp)
      : null;
  const anchorPoint =
    anchorTimestamp !== null
      ? [{ timestamp: anchorTimestamp, value: baseline, return_pct: 0 }]
      : [];
  const points = mergedPoints.map((bar) => ({
    timestamp: bar.timestamp,
    value: bar.value,
    return_pct: ((bar.value - baseline) / baseline) * 100
  }));
  const allPoints = [...anchorPoint, ...points];
  const latest = allPoints.at(-1);
  return {
    ...(fallbackBenchmark ?? {}),
    label: 'OMX',
    symbol: 'OMXS30',
    status: allPoints.length > 1 ? 'ok' : 'insufficient_data',
    error: null,
    latest_return_pct: latest?.return_pct ?? null,
    points: allPoints,
    source: 'market_stream'
  };
}
