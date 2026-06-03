import { formatTimestampOrNull } from './status.js';
import {
  formatAbsoluteNumber,
  formatSignedMoney,
  formatSignedNumber,
  parseFiniteNumber
} from './formatting.js';

export function operatorReviewClass(review) {
  const status = review.status;
  if (status !== 'OPEN') return 'neutral';
  return 'warn';
}

export function operatorReviewLabel(review) {
  const status = review.status;
  return status === 'OPEN' ? 'OPEN' : 'ARCHIVED';
}

export function operatorReviewActions(review) {
  const status = review.status;
  if (status !== 'OPEN') {
    return [];
  }
  return [{ operation: 'ARCHIVE', label: 'Archive', className: 'inline-button neutral' }];
}

export function operatorReviewDetail(review) {
  if (!review?.latest_action_type) {
    return 'Not archived yet.';
  }

  const reviewedAt = formatTimestampOrNull(review.latest_action_at) ?? 'unknown time';
  const reviewedBy = review.latest_action_by ?? 'unknown operator';
  return `Archived by ${reviewedBy} at ${reviewedAt}`;
}

export function marketDirectionArrow(direction) {
  if (direction === 'UP') return '↑';
  if (direction === 'DOWN') return '↓';
  if (direction === 'UNCHANGED') return '→';
  return '';
}

export function marketDirectionClass(direction) {
  if (direction === 'UP') return 'ok';
  if (direction === 'DOWN') return 'bad';
  return 'subtle';
}

export function orderSpreadLabel(order) {
  const spread = parseFiniteNumber(order.price_spread);
  const spreadPct = parseFiniteNumber(order.price_spread_pct);
  if (spread === null) {
    return 'n/a';
  }

  const direction = spread > 0 ? 'above mkt' : spread < 0 ? 'below mkt' : 'at mkt';
  const pctSuffix = spreadPct !== null ? ` (${formatSignedNumber(spreadPct)}%)` : '';
  return `${formatAbsoluteNumber(spread)} ${direction}${pctSuffix}`;
}

export function orderTriggerDetail(order) {
  if (!order.working_price) {
    return null;
  }
  const reference = order.working_price_reference ?? order.spread_reference ?? 'trigger';
  return `${reference} ${order.working_price}`;
}

export function orderFillSpreadLabel(order) {
  if (!order.fill_price_spread) {
    return 'n/a';
  }

  const pctSuffix = order.fill_price_spread_pct ? ` (${order.fill_price_spread_pct}%)` : '';
  return `${order.fill_price_spread}${pctSuffix}`;
}

export function fillExitPnlLabel(fill) {
  if (!fill.realized_pnl) {
    return fill.order_role === 'EXIT' ? 'pending' : 'n/a';
  }
  return `${formatSignedMoney(fill.realized_pnl)} ${fill.realized_pnl_currency ?? fill.currency}`;
}

export function fillStrategyLabel(fill) {
  const side = String(fill.position_side ?? '').trim().toUpperCase();
  if (side === 'LONG') {
    return 'Long';
  }
  if (side === 'SHORT') {
    return 'Short';
  }
  return 'n/a';
}

export function fillExitPnlSearchText(fill) {
  return [
    fill.order_role,
    fillExitPnlLabel(fill),
    fill.realized_pnl_gross ? `gross ${fill.realized_pnl_gross}` : null,
    fill.realized_pnl_basis_price ? `basis ${fill.realized_pnl_basis_price}` : null
  ].filter(Boolean).join(' ');
}

