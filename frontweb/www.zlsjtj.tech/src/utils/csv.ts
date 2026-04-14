type CsvValue = string | number | boolean | null | undefined;

function escapeCsvValue(v: CsvValue): string {
  if (v === null || v === undefined) return '';
  const s = String(v);
  if (/[",\n]/.test(s)) return `"${s.replaceAll('"', '""')}"`;
  return s;
}

export function toCsv(rows: Array<Record<string, CsvValue>>, headers?: string[]) {
  const cols = headers ?? Array.from(new Set(rows.flatMap((r) => Object.keys(r))));
  const lines = [
    cols.map((c) => escapeCsvValue(c)).join(','),
    ...rows.map((r) => cols.map((c) => escapeCsvValue(r[c])).join(',')),
  ];
  return lines.join('\n');
}

export function downloadTextFile(filename: string, content: string, mime = 'text/plain') {
  const blob = new Blob([content], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export function downloadCsv(filename: string, rows: Array<Record<string, CsvValue>>, headers?: string[]) {
  const csv = toCsv(rows, headers);
  downloadTextFile(filename, csv, 'text/csv;charset=utf-8');
}

