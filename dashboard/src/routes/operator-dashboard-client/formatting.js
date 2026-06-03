const quantityFormatter = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 0,
  maximumFractionDigits: 4
});
const priceFormatter = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2
});
const moneyFormatter = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2
});

export function parseFiniteNumber(value) {
  const parsed = Number.parseFloat(String(value ?? ''));
  if (!Number.isFinite(parsed)) return null;
  return Math.abs(parsed) < 1e12 ? parsed : null;
}

export function formatNumericValue(value, formatter, { zeroAsUnavailable = false } = {}) {
  if (value === null || value === undefined || value === '') {
    return 'n/a';
  }

  const parsed = parseFiniteNumber(value);
  if (parsed === null) {
    return String(value);
  }
  if (zeroAsUnavailable && parsed === 0) {
    return 'n/a';
  }
  return formatter.format(parsed);
}

export function formatQuantity(value) {
  return formatNumericValue(value, quantityFormatter);
}

export function formatPrice(value, options = {}) {
  return formatNumericValue(value, priceFormatter, options);
}

export function formatMoney(value) {
  return formatNumericValue(value, moneyFormatter);
}

export function formatSignedMoney(value) {
  const parsed = parseFiniteNumber(value);
  if (parsed === null) return 'n/a';
  const prefix = parsed > 0 ? '+' : '';
  return `${prefix}${moneyFormatter.format(parsed)}`;
}

export function moneyTone(value) {
  const parsed = parseFiniteNumber(value);
  if (parsed === null || parsed === 0) return 'subtle';
  return parsed > 0 ? 'ok' : 'bad';
}

export function formatReturnPct(value) {
  const parsed = parseFiniteNumber(value);
  if (parsed === null) return 'n/a';
  const prefix = parsed > 0 ? '+' : '';
  return `${prefix}${parsed.toFixed(2)}%`;
}

export function formatSignedNumber(value, digits = 2) {
  const parsed = parseFiniteNumber(value);
  if (parsed === null) return 'n/a';
  const prefix = parsed > 0 ? '+' : '';
  return `${prefix}${parsed.toFixed(digits)}`;
}

export function formatAbsoluteNumber(value, digits = 2) {
  const parsed = parseFiniteNumber(value);
  if (parsed === null) return 'n/a';
  return Math.abs(parsed).toFixed(digits);
}

export function formatPlainNumber(value, digits = 8) {
  const parsed = parseFiniteNumber(value);
  if (parsed === null) {
    return null;
  }
  const rounded = Number.parseFloat(parsed.toFixed(digits));
  return String(rounded);
}

export function formatSignedDecimal(value, digits = 2) {
  const parsed = parseFiniteNumber(value);
  if (parsed === null) {
    return null;
  }
  const text = parsed.toFixed(digits);
  return parsed > 0 ? `+${text}` : text;
}

export function firstFinite(values) {
  for (const value of values) {
    const parsed = parseFiniteNumber(value);
    if (parsed !== null) {
      return parsed;
    }
  }
  return null;
}

export function displayOrderPrice(value) {
  return formatPrice(value, { zeroAsUnavailable: true });
}
