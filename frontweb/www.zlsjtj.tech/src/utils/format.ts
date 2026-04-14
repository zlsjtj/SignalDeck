import dayjs from 'dayjs';

export function formatTs(iso?: string, fmt = 'YYYY-MM-DD HH:mm:ss') {
  if (!iso) return '-';
  const parsed = new Date(iso);
  if (Number.isNaN(parsed.getTime())) return '-';
  // Convert through Date object to avoid timezone ambiguity and keep local timezone explicit.
  const d = dayjs(parsed);
  return d.isValid() ? d.format(fmt) : '-';
}

export function formatNumber(n?: number, digits = 2) {
  if (n === null || n === undefined || Number.isNaN(n)) return '-';
  return n.toLocaleString(undefined, {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

export function formatPercent(ratio?: number, digits = 2) {
  if (ratio === null || ratio === undefined || Number.isNaN(ratio)) return '-';
  return `${(ratio * 100).toFixed(digits)}%`;
}

export function priceDigitsBySymbol(symbol?: string) {
  const s = String(symbol ?? '').toUpperCase();
  if (s.includes('DOGE') || s.includes('TRX') || s.includes('XRP')) return 5;
  return 2;
}

export function formatPriceBySymbol(symbol: string | undefined, price?: number) {
  return formatNumber(price, priceDigitsBySymbol(symbol));
}
