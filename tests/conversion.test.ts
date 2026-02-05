import { describe, expect, test } from "bun:test";
import { ColumnTypeEnum } from "@prisma/driver-adapter-utils";
import {
  fieldToColumnType,
  findFirstNonNullInColumn,
  inferOidFromValue,
  mapArg,
  PgOid,
  resultNormalizers,
  toPgArrayLiteral,
  UnsupportedNativeDataType,
} from "../src/conversion.ts";

describe("fieldToColumnType", () => {
  test("maps integer types", () => {
    expect(fieldToColumnType(PgOid.INT2)).toBe(ColumnTypeEnum.Int32);
    expect(fieldToColumnType(PgOid.INT4)).toBe(ColumnTypeEnum.Int32);
    expect(fieldToColumnType(PgOid.INT8)).toBe(ColumnTypeEnum.Int64);
  });

  test("maps float types", () => {
    expect(fieldToColumnType(PgOid.FLOAT4)).toBe(ColumnTypeEnum.Float);
    expect(fieldToColumnType(PgOid.FLOAT8)).toBe(ColumnTypeEnum.Double);
  });

  test("maps numeric types", () => {
    expect(fieldToColumnType(PgOid.NUMERIC)).toBe(ColumnTypeEnum.Numeric);
    expect(fieldToColumnType(PgOid.MONEY)).toBe(ColumnTypeEnum.Numeric);
  });

  test("maps boolean", () => {
    expect(fieldToColumnType(PgOid.BOOL)).toBe(ColumnTypeEnum.Boolean);
  });

  test("maps text types", () => {
    expect(fieldToColumnType(PgOid.TEXT)).toBe(ColumnTypeEnum.Text);
    expect(fieldToColumnType(PgOid.VARCHAR)).toBe(ColumnTypeEnum.Text);
    expect(fieldToColumnType(PgOid.BPCHAR)).toBe(ColumnTypeEnum.Text);
    expect(fieldToColumnType(PgOid.NAME)).toBe(ColumnTypeEnum.Text);
    expect(fieldToColumnType(PgOid.INET)).toBe(ColumnTypeEnum.Text);
    expect(fieldToColumnType(PgOid.CIDR)).toBe(ColumnTypeEnum.Text);
    expect(fieldToColumnType(PgOid.XML)).toBe(ColumnTypeEnum.Text);
    expect(fieldToColumnType(PgOid.BIT)).toBe(ColumnTypeEnum.Text);
    expect(fieldToColumnType(PgOid.VARBIT)).toBe(ColumnTypeEnum.Text);
  });

  test("maps date/time types", () => {
    expect(fieldToColumnType(PgOid.DATE)).toBe(ColumnTypeEnum.Date);
    expect(fieldToColumnType(PgOid.TIME)).toBe(ColumnTypeEnum.Time);
    expect(fieldToColumnType(PgOid.TIMETZ)).toBe(ColumnTypeEnum.Time);
    expect(fieldToColumnType(PgOid.TIMESTAMP)).toBe(ColumnTypeEnum.DateTime);
    expect(fieldToColumnType(PgOid.TIMESTAMPTZ)).toBe(ColumnTypeEnum.DateTime);
  });

  test("maps json types", () => {
    expect(fieldToColumnType(PgOid.JSON)).toBe(ColumnTypeEnum.Json);
    expect(fieldToColumnType(PgOid.JSONB)).toBe(ColumnTypeEnum.Json);
  });

  test("maps uuid", () => {
    expect(fieldToColumnType(PgOid.UUID)).toBe(ColumnTypeEnum.Uuid);
  });

  test("maps bytes", () => {
    expect(fieldToColumnType(PgOid.BYTEA)).toBe(ColumnTypeEnum.Bytes);
  });

  test("maps char", () => {
    expect(fieldToColumnType(PgOid.CHAR)).toBe(ColumnTypeEnum.Character);
  });

  test("maps array types", () => {
    expect(fieldToColumnType(PgOid.INT4_ARRAY)).toBe(ColumnTypeEnum.Int32Array);
    expect(fieldToColumnType(PgOid.INT8_ARRAY)).toBe(ColumnTypeEnum.Int64Array);
    expect(fieldToColumnType(PgOid.TEXT_ARRAY)).toBe(ColumnTypeEnum.TextArray);
    expect(fieldToColumnType(PgOid.BOOL_ARRAY)).toBe(ColumnTypeEnum.BooleanArray);
    expect(fieldToColumnType(PgOid.FLOAT4_ARRAY)).toBe(ColumnTypeEnum.FloatArray);
    expect(fieldToColumnType(PgOid.FLOAT8_ARRAY)).toBe(ColumnTypeEnum.DoubleArray);
    expect(fieldToColumnType(PgOid.NUMERIC_ARRAY)).toBe(ColumnTypeEnum.NumericArray);
    expect(fieldToColumnType(PgOid.UUID_ARRAY)).toBe(ColumnTypeEnum.UuidArray);
    expect(fieldToColumnType(PgOid.JSON_ARRAY)).toBe(ColumnTypeEnum.JsonArray);
    expect(fieldToColumnType(PgOid.JSONB_ARRAY)).toBe(ColumnTypeEnum.JsonArray);
    expect(fieldToColumnType(PgOid.BYTEA_ARRAY)).toBe(ColumnTypeEnum.BytesArray);
    expect(fieldToColumnType(PgOid.TIMESTAMP_ARRAY)).toBe(ColumnTypeEnum.DateTimeArray);
    expect(fieldToColumnType(PgOid.DATE_ARRAY)).toBe(ColumnTypeEnum.DateArray);
    expect(fieldToColumnType(PgOid.TIME_ARRAY)).toBe(ColumnTypeEnum.TimeArray);
  });

  test("treats user-defined types (OID >= 16384) as Text", () => {
    expect(fieldToColumnType(16384)).toBe(ColumnTypeEnum.Text);
    expect(fieldToColumnType(20000)).toBe(ColumnTypeEnum.Text);
    expect(fieldToColumnType(99999)).toBe(ColumnTypeEnum.Text);
  });

  test("throws UnsupportedNativeDataType for unknown OIDs < 16384", () => {
    expect(() => fieldToColumnType(9999)).toThrow(UnsupportedNativeDataType);
  });
});

describe("mapArg", () => {
  test("passes null through", () => {
    expect(mapArg(null, { arity: "scalar", scalarType: "string" })).toBeNull();
    expect(mapArg(undefined, { arity: "scalar", scalarType: "string" })).toBeNull();
  });

  test("passes strings through", () => {
    expect(mapArg("hello", { arity: "scalar", scalarType: "string" })).toBe("hello");
  });

  test("passes numbers through", () => {
    expect(mapArg(42, { arity: "scalar", scalarType: "int" })).toBe(42);
  });

  test("passes booleans through", () => {
    expect(mapArg(true, { arity: "scalar", scalarType: "boolean" })).toBe(true);
  });

  test("converts datetime strings to formatted date strings", () => {
    const result = mapArg("2024-06-15T12:30:45.123Z", { arity: "scalar", scalarType: "datetime" });
    expect(result).toBe("2024-06-15 12:30:45.123");
  });

  test("converts datetime strings with dbType DATE", () => {
    const result = mapArg("2024-06-15T12:30:45.123Z", { arity: "scalar", dbType: "DATE", scalarType: "datetime" });
    expect(result).toBe("2024-06-15");
  });

  test("converts datetime strings with dbType TIME", () => {
    const result = mapArg("2024-06-15T12:30:45.123Z", { arity: "scalar", dbType: "TIME", scalarType: "datetime" });
    expect(result).toBe("12:30:45.123");
  });

  test("converts bytes from base64 to Buffer", () => {
    const b64 = Buffer.from("hello").toString("base64");
    const result = mapArg(b64, { arity: "scalar", scalarType: "bytes" });
    expect(result).toBeInstanceOf(Buffer);
    expect((result as Buffer).toString()).toBe("hello");
  });

  test("maps list arguments to PG array literal", () => {
    const result = mapArg([1, 2, 3], { arity: "list", scalarType: "int" });
    expect(result).toBe("{1,2,3}");
  });

  test("maps list of strings to PG array literal", () => {
    const result = mapArg(["hello", "world"], { arity: "list", scalarType: "string" });
    expect(result).toBe('{"hello","world"}');
  });

  test("maps list of datetime strings to PG array literal", () => {
    const result = mapArg(["2024-01-01T00:00:00.000Z", "2024-06-15T12:30:00.000Z"], {
      arity: "list",
      scalarType: "datetime",
    });
    expect(result).toBe('{"2024-01-01 00:00:00.000","2024-06-15 12:30:00.000"}');
  });

  test("maps list with nulls to PG array literal", () => {
    const result = mapArg([1, null, 3], { arity: "list", scalarType: "int" });
    expect(result).toBe("{1,NULL,3}");
  });
});

describe("resultNormalizers", () => {
  test("normalizes JSON objects to strings", () => {
    const normalizer = resultNormalizers[PgOid.JSON]!;
    expect(normalizer({ key: "value" })).toBe('{"key":"value"}');
    expect(normalizer([1, 2, 3])).toBe("[1,2,3]");
    // Already a string: passthrough
    expect(normalizer('{"key":"value"}')).toBe('{"key":"value"}');
  });

  test("normalizes JSONB objects to strings", () => {
    const normalizer = resultNormalizers[PgOid.JSONB]!;
    expect(normalizer({ nested: { a: 1 } })).toBe('{"nested":{"a":1}}');
  });

  test("normalizes JSONB array value to single JSON string (json_agg scenario)", () => {
    // json_agg() returns a single JSONB value containing a JSON array.
    // Bun.sql auto-parses it into a JS array. The normalizer must stringify
    // the entire array as one JSON string, not per-element.
    const normalizer = resultNormalizers[PgOid.JSONB]!;
    expect(normalizer([{ id: 1, role: "OWNER" }])).toBe('[{"id":1,"role":"OWNER"}]');
    expect(normalizer([{ a: 1 }, { b: 2 }])).toBe('[{"a":1},{"b":2}]');
  });

  test("normalizes NUMERIC to string", () => {
    const normalizer = resultNormalizers[PgOid.NUMERIC]!;
    expect(normalizer(123.456)).toBe("123.456");
    expect(normalizer("99.99")).toBe("99.99");
  });

  test("normalizes MONEY by stripping $", () => {
    const normalizer = resultNormalizers[PgOid.MONEY]!;
    expect(normalizer("$100.50")).toBe("100.50");
    expect(normalizer("50.00")).toBe("50.00");
  });

  test("normalizes TIMESTAMP Date to ISO", () => {
    const normalizer = resultNormalizers[PgOid.TIMESTAMP]!;
    const d = new Date("2024-06-15T12:30:45.123Z");
    const result = normalizer(d) as string;
    expect(result).toContain("2024-06-15");
    expect(result).toContain("12:30:45.123");
    expect(result).toContain("+00:00");
  });

  test("normalizes TIMESTAMPTZ Date to ISO", () => {
    const normalizer = resultNormalizers[PgOid.TIMESTAMPTZ]!;
    const d = new Date("2024-06-15T12:30:45.123Z");
    const result = normalizer(d) as string;
    expect(result).toContain("2024-06-15");
    expect(result).toContain("+00:00");
  });

  test("normalizes DATE to string", () => {
    const normalizer = resultNormalizers[PgOid.DATE]!;
    const d = new Date("2024-06-15T00:00:00.000Z");
    expect(normalizer(d)).toBe("2024-06-15");
  });

  test("normalizes TIME to string", () => {
    const normalizer = resultNormalizers[PgOid.TIME]!;
    const d = new Date("2024-06-15T14:30:00.000Z");
    expect(normalizer(d)).toBe("14:30:00.000");
  });

  test("normalizes TIMETZ by stripping offset", () => {
    const normalizer = resultNormalizers[PgOid.TIMETZ]!;
    expect(normalizer("14:30:00+03")).toBe("14:30:00");
    expect(normalizer("14:30:00+03:00")).toBe("14:30:00");
  });

  test("normalizes BYTEA Uint8Array to Buffer", () => {
    const normalizer = resultNormalizers[PgOid.BYTEA]!;
    const input = new Uint8Array([104, 101, 108, 108, 111]);
    const result = normalizer(input);
    expect(result).toBeInstanceOf(Buffer);
  });

  test("normalizes JSON arrays", () => {
    const normalizer = resultNormalizers[PgOid.JSON_ARRAY]!;
    const result = normalizer([{ a: 1 }, { b: 2 }]);
    expect(result).toEqual(['{"a":1}', '{"b":2}']);
  });

  test("normalizes NUMERIC arrays", () => {
    const normalizer = resultNormalizers[PgOid.NUMERIC_ARRAY]!;
    const result = normalizer([1.5, 2.5, 3.5]);
    expect(result).toEqual(["1.5", "2.5", "3.5"]);
  });
});

describe("inferOidFromValue", () => {
  test("null -> TEXT", () => {
    expect(inferOidFromValue(null)).toBe(PgOid.TEXT);
    expect(inferOidFromValue(undefined)).toBe(PgOid.TEXT);
  });

  test("boolean -> BOOL", () => {
    expect(inferOidFromValue(true)).toBe(PgOid.BOOL);
    expect(inferOidFromValue(false)).toBe(PgOid.BOOL);
  });

  test("integer -> INT8", () => {
    expect(inferOidFromValue(42)).toBe(PgOid.INT8);
    expect(inferOidFromValue(0)).toBe(PgOid.INT8);
    expect(inferOidFromValue(-100)).toBe(PgOid.INT8);
  });

  test("float -> FLOAT8", () => {
    expect(inferOidFromValue(3.14)).toBe(PgOid.FLOAT8);
    expect(inferOidFromValue(-0.5)).toBe(PgOid.FLOAT8);
  });

  test("bigint -> INT8", () => {
    expect(inferOidFromValue(BigInt(9999999999999))).toBe(PgOid.INT8);
  });

  test("string -> TEXT", () => {
    expect(inferOidFromValue("hello")).toBe(PgOid.TEXT);
    expect(inferOidFromValue("")).toBe(PgOid.TEXT);
    expect(inferOidFromValue("123")).toBe(PgOid.TEXT);
    expect(inferOidFromValue("true")).toBe(PgOid.TEXT);
  });

  test("JSON object string -> JSON", () => {
    expect(inferOidFromValue('{"role":"admin"}')).toBe(PgOid.JSON);
    expect(inferOidFromValue('{"nested":{"a":1}}')).toBe(PgOid.JSON);
    expect(inferOidFromValue("{}")).toBe(PgOid.JSON);
  });

  test("JSON array string -> JSON", () => {
    expect(inferOidFromValue("[1,2,3]")).toBe(PgOid.JSON);
    expect(inferOidFromValue('[{"id":1},{"id":2}]')).toBe(PgOid.JSON);
    expect(inferOidFromValue("[]")).toBe(PgOid.JSON);
  });

  test("invalid JSON starting with { or [ -> TEXT", () => {
    expect(inferOidFromValue("{not json}")).toBe(PgOid.TEXT);
    expect(inferOidFromValue("[not json")).toBe(PgOid.TEXT);
    expect(inferOidFromValue("{a,b,c}")).toBe(PgOid.TEXT);
  });

  test("Date -> TIMESTAMPTZ", () => {
    expect(inferOidFromValue(new Date())).toBe(PgOid.TIMESTAMPTZ);
  });

  test("Buffer -> BYTEA", () => {
    expect(inferOidFromValue(Buffer.from("hello"))).toBe(PgOid.BYTEA);
    expect(inferOidFromValue(new Uint8Array([1, 2, 3]))).toBe(PgOid.BYTEA);
  });

  test("object -> JSONB", () => {
    expect(inferOidFromValue({ key: "value" })).toBe(PgOid.JSONB);
  });

  test("array of numbers -> INT8_ARRAY or FLOAT8_ARRAY", () => {
    expect(inferOidFromValue([1, 2, 3])).toBe(PgOid.INT8_ARRAY);
    expect(inferOidFromValue([1.5, 2.5])).toBe(PgOid.FLOAT8_ARRAY);
  });

  test("array of booleans -> BOOL_ARRAY", () => {
    expect(inferOidFromValue([true, false])).toBe(PgOid.BOOL_ARRAY);
  });

  test("array of strings -> TEXT_ARRAY", () => {
    expect(inferOidFromValue(["a", "b"])).toBe(PgOid.TEXT_ARRAY);
  });

  test("array of objects -> JSONB (not JSONB_ARRAY)", () => {
    // json_agg() returns type jsonb, not jsonb[]. Bun.sql auto-parses it
    // into a JS array. Must be typed as JSONB so Prisma gets columnType Json.
    expect(inferOidFromValue([{ id: 1 }, { id: 2 }])).toBe(PgOid.JSONB);
    expect(inferOidFromValue([{ role: "OWNER" }])).toBe(PgOid.JSONB);
  });

  test("array of Dates -> TIMESTAMPTZ_ARRAY", () => {
    expect(inferOidFromValue([new Date(), new Date()])).toBe(PgOid.TIMESTAMPTZ_ARRAY);
  });

  test("array of Buffers -> BYTEA_ARRAY", () => {
    expect(inferOidFromValue([Buffer.from("a"), Buffer.from("b")])).toBe(PgOid.BYTEA_ARRAY);
    expect(inferOidFromValue([new Uint8Array([1]), new Uint8Array([2])])).toBe(PgOid.BYTEA_ARRAY);
  });

  test("empty array -> TEXT_ARRAY", () => {
    expect(inferOidFromValue([])).toBe(PgOid.TEXT_ARRAY);
  });

  test("array of all nulls -> TEXT_ARRAY", () => {
    expect(inferOidFromValue([null, null])).toBe(PgOid.TEXT_ARRAY);
  });
});

describe("toPgArrayLiteral", () => {
  test("empty array", () => {
    expect(toPgArrayLiteral([])).toBe("{}");
  });

  test("numbers", () => {
    expect(toPgArrayLiteral([1, 2, 3])).toBe("{1,2,3}");
  });

  test("strings with quoting", () => {
    expect(toPgArrayLiteral(["hello", "world"])).toBe('{"hello","world"}');
  });

  test("strings with special characters", () => {
    expect(toPgArrayLiteral(['hello "world"', "back\\slash"])).toBe('{"hello \\"world\\"","back\\\\slash"}');
  });

  test("mixed with nulls", () => {
    expect(toPgArrayLiteral([1, null, 3])).toBe("{1,NULL,3}");
  });

  test("booleans", () => {
    expect(toPgArrayLiteral([true, false])).toBe("{t,f}");
  });

  test("bigints", () => {
    expect(toPgArrayLiteral([BigInt(1), BigInt(999)])).toBe("{1,999}");
  });
});

describe("findFirstNonNullInColumn", () => {
  test("finds first non-null value", () => {
    const rows = [
      [null, "a"],
      [42, "b"],
      [99, "c"],
    ];
    expect(findFirstNonNullInColumn(rows, 0)).toBe(42);
    expect(findFirstNonNullInColumn(rows, 1)).toBe("a");
  });

  test("returns null for all-null column", () => {
    const rows = [
      [null, "a"],
      [null, "b"],
    ];
    expect(findFirstNonNullInColumn(rows, 0)).toBeNull();
  });

  test("returns null for empty rows", () => {
    expect(findFirstNonNullInColumn([], 0)).toBeNull();
  });
});
