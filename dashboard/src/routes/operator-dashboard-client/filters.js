export function defaultDashboardFilters() {
  return {
    positions: {
      account: '',
      symbol: '',
      exchange: '',
      currency: '',
      quantity: '',
      averageCost: '',
      marketPrice: '',
      marketValue: '',
      unrealizedPnl: '',
      exitPlan: ''
    },
    openOrders: {
      account: '',
      symbol: '',
      role: '',
      purpose: '',
      side: '',
      quantity: '',
      type: '',
      limit: '',
      stop: '',
      vsFill: '',
      market: '',
      vsMkt: '',
      status: '',
      warning: ''
    },
    recentFills: {
      time: '',
      account: '',
      symbol: '',
      side: '',
      strat: '',
      quantity: '',
      price: '',
      fee: '',
      pnl: ''
    },
    instructions: {
      instruction: '',
      symbol: '',
      state: '',
      lifecycle: '',
      guidance: '',
      entryOrder: '',
      exitOrder: '',
      updated: ''
    },
    brokerAttention: {},
    reconciliation: {}
  };
}

export function parseStoredFilters(rawValue) {
  const defaults = defaultDashboardFilters();
  if (!rawValue) {
    return defaults;
  }

  try {
    const parsed = JSON.parse(rawValue);
    for (const [sectionName, sectionDefaults] of Object.entries(defaults)) {
      const parsedSection =
        parsed && typeof parsed === 'object' && parsed[sectionName] && typeof parsed[sectionName] === 'object'
          ? parsed[sectionName]
          : {};
      defaults[sectionName] = Object.fromEntries(
        Object.keys(sectionDefaults).map((key) => [key, String(parsedSection[key] ?? '')])
      );
    }
    return defaults;
  } catch {
    return defaults;
  }
}

export function normalizeSearchText(value) {
  if (value === null || value === undefined) return '';
  if (Array.isArray(value)) return value.map((item) => normalizeSearchText(item)).join(' ');
  return String(value).toLowerCase();
}

export function matchesFilterValue(value, filterValue) {
  const normalizedFilter = String(filterValue ?? '').trim().toLowerCase();
  if (!normalizedFilter) {
    return true;
  }
  return normalizeSearchText(value).includes(normalizedFilter);
}

export function uniqueIds(values) {
  return [...new Set(values.filter((value) => Number.isInteger(value) && value > 0))];
}

export function summarizeRefs(values) {
  const uniqueValues = [...new Set(values.filter(Boolean))];
  if (uniqueValues.length === 0) {
    return null;
  }
  if (uniqueValues.length <= 2) {
    return uniqueValues.join(', ');
  }
  return `${uniqueValues.slice(0, 2).join(', ')} +${uniqueValues.length - 2} more`;
}
