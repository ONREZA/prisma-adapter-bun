import { type ArgType, type ColumnType, ColumnTypeEnum } from "@prisma/driver-adapter-utils";

// Top-level regex constants (biome: useTopLevelRegex)
const RE_TIMESTAMPTZ_OFFSET = /([+-]\d{2})$/;
const RE_TIMETZ_STRIP = /[+-]\d{2}(:\d{2})?$/;
const RE_MONEY_SYMBOL = /^\$/;
const RE_PG_ESCAPE_BACKSLASH = /\\/g;
const RE_PG_ESCAPE_QUOTE = /"/g;

// PostgreSQL OIDs (from pg_type system catalog)
export const PgOid = {
  BIT: 1560,
  BIT_ARRAY: 1561,
  BOOL: 16,

  // Array types
  BOOL_ARRAY: 1000,
  BPCHAR: 1042,
  BPCHAR_ARRAY: 1014,
  BYTEA: 17,
  BYTEA_ARRAY: 1001,
  CHAR: 18,
  CHAR_ARRAY: 1002,
  CIDR: 650,
  CIDR_ARRAY: 651,
  DATE: 1082,
  DATE_ARRAY: 1182,
  FLOAT4: 700,
  FLOAT4_ARRAY: 1021,
  FLOAT8: 701,
  FLOAT8_ARRAY: 1022,
  INET: 869,
  INET_ARRAY: 1041,
  INT2: 21,
  INT2_ARRAY: 1005,
  INT4: 23,
  INT4_ARRAY: 1007,
  INT8: 20,
  INT8_ARRAY: 1016,
  JSON: 114,
  JSON_ARRAY: 199,
  JSONB: 3802,
  JSONB_ARRAY: 3807,
  MONEY: 790,
  MONEY_ARRAY: 791,
  NAME: 19,
  NAME_ARRAY: 1003,
  NUMERIC: 1700,
  NUMERIC_ARRAY: 1231,
  OID: 26,
  OID_ARRAY: 1028,
  TEXT: 25,
  TEXT_ARRAY: 1009,
  TIME: 1083,
  TIME_ARRAY: 1183,
  TIMESTAMP: 1114,
  TIMESTAMP_ARRAY: 1115,
  TIMESTAMPTZ: 1184,
  TIMESTAMPTZ_ARRAY: 1185,
  TIMETZ: 1266,
  TIMETZ_ARRAY: 1270,
  UUID: 2950,
  UUID_ARRAY: 2951,
  VARBIT: 1562,
  VARBIT_ARRAY: 1563,
  VARCHAR: 1043,
  VARCHAR_ARRAY: 1015,
  XML: 142,
  XML_ARRAY: 143,
} as const;

// OID threshold: types >= this are user-defined (enums, composites, etc.)
const FIRST_NORMAL_OBJECT_ID = 16384;

export class UnsupportedNativeDataType extends Error {
  type: string;
  constructor(oid: number) {
    const message = `Unsupported column type with OID ${oid}`;
    super(message);
    this.type = `${oid}`;
  }
}

const scalarMapping: Record<number, ColumnType> = {
  [PgOid.INT2]: ColumnTypeEnum.Int32,
  [PgOid.INT4]: ColumnTypeEnum.Int32,
  [PgOid.INT8]: ColumnTypeEnum.Int64,
  [PgOid.FLOAT4]: ColumnTypeEnum.Float,
  [PgOid.FLOAT8]: ColumnTypeEnum.Double,
  [PgOid.BOOL]: ColumnTypeEnum.Boolean,
  [PgOid.DATE]: ColumnTypeEnum.Date,
  [PgOid.TIME]: ColumnTypeEnum.Time,
  [PgOid.TIMETZ]: ColumnTypeEnum.Time,
  [PgOid.TIMESTAMP]: ColumnTypeEnum.DateTime,
  [PgOid.TIMESTAMPTZ]: ColumnTypeEnum.DateTime,
  [PgOid.NUMERIC]: ColumnTypeEnum.Numeric,
  [PgOid.MONEY]: ColumnTypeEnum.Numeric,
  [PgOid.JSON]: ColumnTypeEnum.Json,
  [PgOid.JSONB]: ColumnTypeEnum.Json,
  [PgOid.UUID]: ColumnTypeEnum.Uuid,
  [PgOid.OID]: ColumnTypeEnum.Int64,
  [PgOid.BPCHAR]: ColumnTypeEnum.Text,
  [PgOid.TEXT]: ColumnTypeEnum.Text,
  [PgOid.VARCHAR]: ColumnTypeEnum.Text,
  [PgOid.BIT]: ColumnTypeEnum.Text,
  [PgOid.VARBIT]: ColumnTypeEnum.Text,
  [PgOid.INET]: ColumnTypeEnum.Text,
  [PgOid.CIDR]: ColumnTypeEnum.Text,
  [PgOid.XML]: ColumnTypeEnum.Text,
  [PgOid.NAME]: ColumnTypeEnum.Text,
  [PgOid.CHAR]: ColumnTypeEnum.Character,
  [PgOid.BYTEA]: ColumnTypeEnum.Bytes,
};

const arrayMapping: Record<number, ColumnType> = {
  [PgOid.INT2_ARRAY]: ColumnTypeEnum.Int32Array,
  [PgOid.INT4_ARRAY]: ColumnTypeEnum.Int32Array,
  [PgOid.INT8_ARRAY]: ColumnTypeEnum.Int64Array,
  [PgOid.FLOAT4_ARRAY]: ColumnTypeEnum.FloatArray,
  [PgOid.FLOAT8_ARRAY]: ColumnTypeEnum.DoubleArray,
  [PgOid.BOOL_ARRAY]: ColumnTypeEnum.BooleanArray,
  [PgOid.DATE_ARRAY]: ColumnTypeEnum.DateArray,
  [PgOid.TIME_ARRAY]: ColumnTypeEnum.TimeArray,
  [PgOid.TIMETZ_ARRAY]: ColumnTypeEnum.TimeArray,
  [PgOid.TIMESTAMP_ARRAY]: ColumnTypeEnum.DateTimeArray,
  [PgOid.TIMESTAMPTZ_ARRAY]: ColumnTypeEnum.DateTimeArray,
  [PgOid.NUMERIC_ARRAY]: ColumnTypeEnum.NumericArray,
  [PgOid.MONEY_ARRAY]: ColumnTypeEnum.NumericArray,
  [PgOid.JSON_ARRAY]: ColumnTypeEnum.JsonArray,
  [PgOid.JSONB_ARRAY]: ColumnTypeEnum.JsonArray,
  [PgOid.UUID_ARRAY]: ColumnTypeEnum.UuidArray,
  [PgOid.OID_ARRAY]: ColumnTypeEnum.Int64Array,
  [PgOid.BPCHAR_ARRAY]: ColumnTypeEnum.TextArray,
  [PgOid.TEXT_ARRAY]: ColumnTypeEnum.TextArray,
  [PgOid.VARCHAR_ARRAY]: ColumnTypeEnum.TextArray,
  [PgOid.BIT_ARRAY]: ColumnTypeEnum.TextArray,
  [PgOid.VARBIT_ARRAY]: ColumnTypeEnum.TextArray,
  [PgOid.INET_ARRAY]: ColumnTypeEnum.TextArray,
  [PgOid.CIDR_ARRAY]: ColumnTypeEnum.TextArray,
  [PgOid.XML_ARRAY]: ColumnTypeEnum.TextArray,
  [PgOid.NAME_ARRAY]: ColumnTypeEnum.TextArray,
  [PgOid.CHAR_ARRAY]: ColumnTypeEnum.CharacterArray,
  [PgOid.BYTEA_ARRAY]: ColumnTypeEnum.BytesArray,
};

export function fieldToColumnType(oid: number): ColumnType {
  const scalar = scalarMapping[oid];
  if (scalar !== undefined) return scalar;

  const array = arrayMapping[oid];
  if (array !== undefined) return array;

  // User-defined types (enums, composites) are treated as Text
  if (oid >= FIRST_NORMAL_OBJECT_ID) return ColumnTypeEnum.Text;

  throw new UnsupportedNativeDataType(oid);
}

// --- Value normalizers (post-processing Bun.sql results) ---

function pad2(n: number): string {
  return n < 10 ? `0${n}` : `${n}`;
}

function pad3(n: number): string {
  if (n < 10) return `00${n}`;
  if (n < 100) return `0${n}`;
  return `${n}`;
}

function formatDateTime(date: Date): string {
  return (
    `${date.getUTCFullYear()}-${pad2(date.getUTCMonth() + 1)}-${pad2(date.getUTCDate())} ` +
    `${pad2(date.getUTCHours())}:${pad2(date.getUTCMinutes())}:${pad2(date.getUTCSeconds())}.${pad3(date.getUTCMilliseconds())}`
  );
}

function formatDate(date: Date): string {
  return `${date.getUTCFullYear()}-${pad2(date.getUTCMonth() + 1)}-${pad2(date.getUTCDate())}`;
}

function formatTime(date: Date): string {
  return `${pad2(date.getUTCHours())}:${pad2(date.getUTCMinutes())}:${pad2(date.getUTCSeconds())}.${pad3(date.getUTCMilliseconds())}`;
}

// Normalize a timestamp string to ISO-like format: "2024-01-01 12:00:00" -> "2024-01-01T12:00:00.000+00:00"
function normalizeTimestamp(value: unknown): unknown {
  if (value instanceof Date) {
    return `${formatDateTime(value)}+00:00`;
  }
  if (typeof value === "string") {
    return `${value.replace(" ", "T")}+00:00`;
  }
  return value;
}

function normalizeTimestamptz(value: unknown): unknown {
  if (value instanceof Date) {
    return `${formatDateTime(value)}+00:00`;
  }
  if (typeof value === "string") {
    // Normalize various timezone formats to +00:00
    return value.replace(" ", "T").replace(RE_TIMESTAMPTZ_OFFSET, "$1:00");
  }
  return value;
}

function normalizeTimetz(value: unknown): unknown {
  if (typeof value === "string") {
    // Remove timezone offset from time value
    return value.replace(RE_TIMETZ_STRIP, "");
  }
  return value;
}

function normalizeDate(value: unknown): unknown {
  if (value instanceof Date) {
    return formatDate(value);
  }
  return value;
}

function normalizeTime(value: unknown): unknown {
  if (value instanceof Date) {
    return formatTime(value);
  }
  return value;
}

function normalizeNumeric(value: unknown): unknown {
  return String(value);
}

function normalizeMoney(value: unknown): unknown {
  const s = String(value);
  return s.replace(RE_MONEY_SYMBOL, "");
}

function normalizeJson(value: unknown): unknown {
  if (typeof value === "object" && value !== null) {
    return JSON.stringify(value);
  }
  return value;
}

function normalizeBytes(value: unknown): unknown {
  if (value instanceof Uint8Array || value instanceof Buffer) {
    return Buffer.from(value);
  }
  return value;
}

function normalizeArray(value: unknown, elementNormalizer: (v: unknown) => unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((el) => (el === null ? null : elementNormalizer(el)));
  }
  return value;
}

type Normalizer = (value: unknown) => unknown;

export const resultNormalizers: Record<number, Normalizer> = {
  [PgOid.NUMERIC]: normalizeNumeric,
  [PgOid.MONEY]: normalizeMoney,
  [PgOid.TIME]: normalizeTime,
  [PgOid.TIMETZ]: normalizeTimetz,
  [PgOid.DATE]: normalizeDate,
  [PgOid.TIMESTAMP]: normalizeTimestamp,
  [PgOid.TIMESTAMPTZ]: normalizeTimestamptz,
  [PgOid.JSON]: normalizeJson,
  [PgOid.JSONB]: normalizeJson,
  [PgOid.BYTEA]: normalizeBytes,

  // Array normalizers
  [PgOid.NUMERIC_ARRAY]: (v) => normalizeArray(v, normalizeNumeric),
  [PgOid.MONEY_ARRAY]: (v) => normalizeArray(v, normalizeMoney),
  [PgOid.TIME_ARRAY]: (v) => normalizeArray(v, normalizeTime),
  [PgOid.TIMETZ_ARRAY]: (v) => normalizeArray(v, normalizeTimetz),
  [PgOid.DATE_ARRAY]: (v) => normalizeArray(v, normalizeDate),
  [PgOid.TIMESTAMP_ARRAY]: (v) => normalizeArray(v, normalizeTimestamp),
  [PgOid.TIMESTAMPTZ_ARRAY]: (v) => normalizeArray(v, normalizeTimestamptz),
  [PgOid.JSON_ARRAY]: (v) => normalizeArray(v, normalizeJson),
  [PgOid.JSONB_ARRAY]: (v) => normalizeArray(v, normalizeJson),
  [PgOid.BYTEA_ARRAY]: (v) => normalizeArray(v, normalizeBytes),
  [PgOid.BIT_ARRAY]: (v) => normalizeArray(v, String),
  [PgOid.VARBIT_ARRAY]: (v) => normalizeArray(v, String),
  [PgOid.XML_ARRAY]: (v) => normalizeArray(v, String),
};

// --- Type inference (fallback when .columns metadata is unavailable) ---

function inferArrayOid(arr: unknown[]): number {
  if (arr.length === 0) return PgOid.TEXT_ARRAY;
  const first = arr.find((v) => v !== null && v !== undefined);
  if (first === undefined) return PgOid.TEXT_ARRAY;
  if (typeof first === "number") {
    return Number.isInteger(first) ? PgOid.INT8_ARRAY : PgOid.FLOAT8_ARRAY;
  }
  if (typeof first === "boolean") return PgOid.BOOL_ARRAY;
  if (first instanceof Date) return PgOid.TIMESTAMPTZ_ARRAY;
  if (first instanceof Uint8Array || first instanceof Buffer) return PgOid.BYTEA_ARRAY;
  // Objects/arrays → treat as a single JSONB value (not JSONB[]).
  // json_agg() returns type jsonb (OID 3802), but Bun.sql auto-parses it
  // into a JS array. Prisma PostgreSQL doesn't support Json[] columns,
  // and relation joins use json_agg which must be typed as Json, not JsonArray.
  if (typeof first === "object") return PgOid.JSONB;
  return PgOid.TEXT_ARRAY;
}

/**
 * Detect whether a string value is a JSON object or array.
 * Used to distinguish JSON columns (returned as strings by Bun.sql
 * in relation joins) from plain TEXT columns.
 */
function isJsonString(value: string): boolean {
  const ch = value.charAt(0);
  if (ch !== "{" && ch !== "[") return false;
  try {
    const parsed = JSON.parse(value);
    return typeof parsed === "object" && parsed !== null;
  } catch {
    return false;
  }
}

/**
 * Infer a PostgreSQL OID from a JavaScript value.
 * Used as a fallback when Bun.sql doesn't expose column metadata.
 * The inferred OID is then mapped to ColumnType via fieldToColumnType().
 */
export function inferOidFromValue(value: unknown): number {
  if (value === null || value === undefined) return PgOid.TEXT;
  if (typeof value === "boolean") return PgOid.BOOL;
  if (typeof value === "bigint") return PgOid.INT8;
  if (typeof value === "number") {
    return Number.isInteger(value) ? PgOid.INT8 : PgOid.FLOAT8;
  }
  if (value instanceof Date) return PgOid.TIMESTAMPTZ;
  if (value instanceof Uint8Array || value instanceof Buffer) return PgOid.BYTEA;
  if (Array.isArray(value)) return inferArrayOid(value);
  if (typeof value === "object") return PgOid.JSONB;
  if (typeof value === "string" && isJsonString(value)) return PgOid.JSON;
  return PgOid.TEXT;
}

/**
 * Find the first non-null value for a given column across all rows.
 */
export function findFirstNonNullInColumn(rows: unknown[][], colIndex: number): unknown {
  for (const row of rows) {
    const val = row[colIndex];
    if (val !== null && val !== undefined) return val;
  }
  return null;
}

// --- Array literal formatting (Bun.sql doesn't handle JS arrays in unsafe()) ---

function escapePgArrayElement(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (typeof value === "boolean") return value ? "t" : "f";
  if (typeof value === "number" || typeof value === "bigint") return String(value);
  // Strings: escape backslashes and double-quotes, then wrap in quotes
  const s = String(value);
  return `"${s.replace(RE_PG_ESCAPE_BACKSLASH, "\\\\").replace(RE_PG_ESCAPE_QUOTE, '\\"')}"`;
}

/**
 * Convert a JS array to a PostgreSQL array literal string.
 * Bun.sql's `unsafe()` does not serialize JS arrays as PostgreSQL arrays,
 * so we must pass them in `{elem1,elem2,...}` format.
 */
export function toPgArrayLiteral(arr: unknown[]): string {
  return `{${arr.map(escapePgArrayElement).join(",")}}`;
}

// --- Argument mapping (input parameters) ---

export function mapArg(arg: unknown, argType: ArgType): unknown {
  if (arg === null || arg === undefined) return null;

  if (Array.isArray(arg) && argType.arity === "list") {
    const mapped = arg.map((v) => mapArg(v, { ...argType, arity: "scalar" }));
    return toPgArrayLiteral(mapped);
  }

  const value = typeof arg === "string" && argType.scalarType === "datetime" ? new Date(arg) : arg;

  if (value instanceof Date) {
    switch (argType.dbType) {
      case "TIME":
      case "TIMETZ":
        return formatTime(value);
      case "DATE":
        return formatDate(value);
      default:
        return formatDateTime(value);
    }
  }

  if (typeof value === "string" && argType.scalarType === "bytes") {
    return Buffer.from(value, "base64");
  }

  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }

  return value;
}
