// 统一时间显示：显式指定 Asia/Shanghai（北京时间），不依赖浏览器所在地。
// zh-CN 只决定语言，不能替代时区；夏令时也不由浏览器当地规则污染。
//
// 解析约定（与后端 `core/timebase.py` 对齐，两侧都不"猜"单位）：
// - 数字/纯数字字符串：绝对值 < 1e12 视为**秒**，否则视为**毫秒**；NaN/Infinity/越界 → null；
// - 带 Z 或 ±offset 的 ISO：按原 offset 解析（`new Date` 语义明确，可移植）；
// - 无时区的 `YYYY-MM-DDTHH:mm(:ss)`：按**北京时间**解释（历史数据是本地写入的
//   日历时间），用 `Date.UTC(...) - 8h` 构造，不受浏览器时区影响；
// - 纯日期 `YYYY-MM-DD`：保持日历日期，北京时间当天 00:00；
// - 其它无法识别：返回 null，由调用方显示原值或占位，不抛错。

export const BEIJING_TZ = 'Asia/Shanghai';

const BEIJING_OFFSET_MS = 8 * 60 * 60 * 1000;
//: `Date` 可表示的最大绝对毫秒（ECMA-262）。
const MAX_MS = 8.64e15;

const PARTS = new Intl.DateTimeFormat('zh-CN', {
  timeZone: BEIJING_TZ,
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  hour: '2-digit',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
});

function beijingInstant(
  year: number, month: number, day: number,
  hour = 0, minute = 0, second = 0, milli = 0,
): Date | null {
  const utc = Date.UTC(year, month - 1, day, hour, minute, second, milli);
  if (!Number.isFinite(utc)) return null;
  const date = new Date(utc - BEIJING_OFFSET_MS);
  // 回读校验：2026-02-31 这类会被 Date 顺延，必须拒绝而不是悄悄改成 3 月 3 日。
  const check = new Date(utc);
  if (
    check.getUTCFullYear() !== year || check.getUTCMonth() !== month - 1
    || check.getUTCDate() !== day || check.getUTCHours() !== hour
    || check.getUTCMinutes() !== minute || check.getUTCSeconds() !== second
  ) {
    return null;
  }
  return date;
}

function numeric(value: number): Date | null {
  if (!Number.isFinite(value)) return null;
  const milli = Math.abs(value) < 1e12 ? value * 1000 : value;
  if (Math.abs(milli) > MAX_MS) return null;
  const date = new Date(milli);
  return Number.isNaN(date.valueOf()) ? null : date;
}

const DATE_ONLY = /^(\d{4})-(\d{2})-(\d{2})$/;
const NAIVE = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?$/;
const HAS_OFFSET = /(?:Z|[+-]\d{2}:?\d{2})$/i;
const NUMERIC_TEXT = /^[+-]?\d+(?:\.\d+)?$/;

/** 解析成绝对时刻；无法确定时返回 null（不猜时区、不猜单位）。 */
export function parseAbsolute(value: any): Date | null {
  if (value === null || value === undefined || value === '') return null;
  if (value instanceof Date) {
    return Number.isNaN(value.valueOf()) ? null : value;
  }
  if (typeof value === 'number') return numeric(value);
  if (typeof value === 'boolean') return null;
  const text = String(value).trim();
  if (!text) return null;
  if (NUMERIC_TEXT.test(text)) return numeric(Number(text));

  const dateOnly = DATE_ONLY.exec(text);
  if (dateOnly) {
    return beijingInstant(Number(dateOnly[1]), Number(dateOnly[2]), Number(dateOnly[3]));
  }
  const naive = NAIVE.exec(text);
  if (naive && !HAS_OFFSET.test(text)) {
    return beijingInstant(
      Number(naive[1]), Number(naive[2]), Number(naive[3]),
      Number(naive[4]), Number(naive[5]),
      naive[6] ? Number(naive[6]) : 0,
      naive[7] ? Number(naive[7].padEnd(3, '0')) : 0,
    );
  }
  const parsed = new Date(text);
  return Number.isNaN(parsed.valueOf()) ? null : parsed;
}

/** 绝对毫秒（用于排序/耗时/去重）；无法解析返回 null，不返回 NaN。 */
export function tsMs(value: any): number | null {
  const date = parseAbsolute(value);
  return date ? date.valueOf() : null;
}

/** 后端/接口里 date 字段的日历日期（`YYYY-MM-DD`），解析失败返回空串。 */
export function calendarDate(value: any): string {
  if (typeof value === 'string') {
    const matched = DATE_ONLY.exec(value.trim());
    if (matched) return matched[0];
  }
  const date = parseAbsolute(value);
  if (!date) return '';
  const p = pieces(date);
  return `${p.year}-${p.month}-${p.day}`;
}

function pieces(date: Date): Record<string, string> {
  const out: Record<string, string> = {};
  for (const part of PARTS.formatToParts(date)) {
    if (part.type !== 'literal') out[part.type] = part.value;
  }
  // 部分引擎在 hour12:false 下把午夜给成 24，这里统一成 00。
  if (out.hour === '24') out.hour = '00';
  return out;
}

/** 北京时间 `YYYY-MM-DD HH:mm:ss`；无法解析时返回占位（默认 `—`）。 */
export function fmtTime(value: any, fallback = '—'): string {
  const date = parseAbsolute(value);
  if (!date) return fallback;
  const p = pieces(date);
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}:${p.second}`;
}

/** 北京时间 `YYYY-MM-DD`；无法解析返回占位。 */
export function fmtDate(value: any, fallback = '—'): string {
  const date = parseAbsolute(value);
  if (!date) return fallback;
  const p = pieces(date);
  return `${p.year}-${p.month}-${p.day}`;
}

/** 北京时间 `HH:mm`（紧凑列表用）；无法解析返回占位。 */
export function fmtClock(value: any, fallback = '—'): string {
  const date = parseAbsolute(value);
  if (!date) return fallback;
  const p = pieces(date);
  return `${p.hour}:${p.minute}`;
}

/** 带时区标注的完整写法（设置页/详情页用）。 */
export function fmtTimeWithZone(value: any, fallback = '—'): string {
  const text = fmtTime(value, fallback);
  return text === fallback ? text : `${text}（北京时间）`;
}
