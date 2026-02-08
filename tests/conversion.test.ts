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

  test("parses JSON string arguments to objects (avoid double-stringify)", () => {
    // Prisma sends JSON fields as pre-stringified strings. Bun.sql's unsafe()
    // would double-stringify them. mapArg must parse them back to objects.
    const result = mapArg('{"role":"admin"}', { arity: "scalar", scalarType: "json" });
    expect(result).toEqual({ role: "admin" });
  });

  test("parses JSON array string arguments", () => {
    const result = mapArg("[1,2,3]", { arity: "scalar", scalarType: "json" });
    expect(result).toEqual([1, 2, 3]);
  });

  test("parses JSON scalar string arguments", () => {
    expect(mapArg('"hello"', { arity: "scalar", scalarType: "json" })).toBe("hello");
    expect(mapArg("42", { arity: "scalar", scalarType: "json" })).toBe(42);
    expect(mapArg("true", { arity: "scalar", scalarType: "json" })).toBe(true);
    expect(mapArg("null", { arity: "scalar", scalarType: "json" })).toBeNull();
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
  test("normalizes INT8 string to BigInt", () => {
    const normalizer = resultNormalizers[PgOid.INT8]!;
    // Bun.sql returns BIGINT as string
    expect(normalizer("3000000000")).toBe(BigInt(3000000000));
    expect(normalizer("9999999999999")).toBe(BigInt(9999999999999));
    expect(normalizer("-2147483649")).toBe(BigInt(-2147483649));
    // Small values
    expect(normalizer("42")).toBe(BigInt(42));
    // Already BigInt: passthrough
    expect(normalizer(BigInt(42))).toBe(BigInt(42));
    // Null: passthrough
    expect(normalizer(null)).toBe(null);
  });

  test("normalizes INT8 array strings to BigInt array", () => {
    const normalizer = resultNormalizers[PgOid.INT8_ARRAY]!;
    const result = normalizer(["3000000000", "9999999999999", null]);
    expect(result).toEqual([BigInt(3000000000), BigInt(9999999999999), null]);
  });

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
    // Negative values
    expect(normalizer("-$100.50")).toBe("-100.50");
    expect(normalizer("-50.00")).toBe("-50.00");
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

  test("normalizes TIMESTAMPTZ string with various timezone formats", () => {
    const normalizer = resultNormalizers[PgOid.TIMESTAMPTZ]!;
    // Short format should be preserved
    expect(normalizer("2024-06-15 12:30:00+03")).toBe("2024-06-15T12:30:00+03");
    // Full format should be preserved (not double-appended)
    expect(normalizer("2024-06-15 12:30:00+05:30")).toBe("2024-06-15T12:30:00+05:30");
    expect(normalizer("2024-06-15 12:30:00-03:00")).toBe("2024-06-15T12:30:00-03:00");
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

  test("normalizes BIT strings", () => {
    const normalizer = resultNormalizers[PgOid.BIT]!;
    expect(normalizer("10101010")).toBe("10101010");
    expect(normalizer("0")).toBe("0");
    expect(normalizer("1")).toBe("1");
    // Numbers should be converted to strings
    expect(normalizer(10101010)).toBe("10101010");
  });

  test("normalizes VARBIT strings", () => {
    const normalizer = resultNormalizers[PgOid.VARBIT]!;
    expect(normalizer("101010101010")).toBe("101010101010");
    expect(normalizer("")).toBe("");
  });

  test("normalizes BIT arrays", () => {
    const normalizer = resultNormalizers[PgOid.BIT_ARRAY]!;
    const result = normalizer(["10101010", "11110000"]);
    expect(result).toEqual(["10101010", "11110000"]);
  });

  test("normalizes VARBIT arrays", () => {
    const normalizer = resultNormalizers[PgOid.VARBIT_ARRAY]!;
    const result = normalizer(["1010", "111100001111"]);
    expect(result).toEqual(["1010", "111100001111"]);
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

  test("integer in INT32 range -> INT4", () => {
    expect(inferOidFromValue(42)).toBe(PgOid.INT4);
    expect(inferOidFromValue(0)).toBe(PgOid.INT4);
    expect(inferOidFromValue(-100)).toBe(PgOid.INT4);
    expect(inferOidFromValue(2147483647)).toBe(PgOid.INT4); // INT32_MAX
    expect(inferOidFromValue(-2147483648)).toBe(PgOid.INT4); // INT32_MIN
  });

  test("integer outside INT32 range -> INT8", () => {
    expect(inferOidFromValue(2147483648)).toBe(PgOid.INT8); // INT32_MAX + 1
    expect(inferOidFromValue(-2147483649)).toBe(PgOid.INT8); // INT32_MIN - 1
    expect(inferOidFromValue(9007199254740991)).toBe(PgOid.INT8); // MAX_SAFE_INTEGER
  });

  test("INT32 boundary strings are correctly identified", () => {
    // INT32_MIN as string should be recognized as INT4 (number), not INT8
    // Note: "-2147483648".length === 11, but it's valid INT32
    expect(inferOidFromValue("-2147483648")).toBe(PgOid.TEXT); // Small numeric string -> TEXT
    expect(inferOidFromValue("-2147483649")).toBe(PgOid.INT8); // INT32_MIN - 1 -> INT8
    expect(inferOidFromValue("2147483647")).toBe(PgOid.TEXT); // INT32_MAX as string -> TEXT
    expect(inferOidFromValue("2147483648")).toBe(PgOid.INT8); // INT32_MAX + 1 -> INT8
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
    expect(inferOidFromValue("true")).toBe(PgOid.TEXT);
    expect(inferOidFromValue("abc123")).toBe(PgOid.TEXT);
  });

  test("numeric string in INT32 range -> TEXT (treated as text, not bigint)", () => {
    // Small numeric strings are treated as TEXT unless they look like BIGINT
    expect(inferOidFromValue("123")).toBe(PgOid.TEXT);
    expect(inferOidFromValue("-456")).toBe(PgOid.TEXT);
    expect(inferOidFromValue("2147483647")).toBe(PgOid.TEXT); // INT32_MAX as string
  });

  test("numeric string outside INT32 range -> INT8 (bigint string)", () => {
    // Bun.sql returns BIGINT columns as strings
    expect(inferOidFromValue("2147483648")).toBe(PgOid.INT8); // INT32_MAX + 1
    expect(inferOidFromValue("-2147483649")).toBe(PgOid.INT8); // INT32_MIN - 1
    expect(inferOidFromValue("3000000000")).toBe(PgOid.INT8);
    expect(inferOidFromValue("9999999999999")).toBe(PgOid.INT8);
  });

  test("numeric string with decimal point -> NUMERIC", () => {
    // Bun.sql returns NUMERIC/DECIMAL as strings
    expect(inferOidFromValue("99.99")).toBe(PgOid.NUMERIC);
    expect(inferOidFromValue("123.456")).toBe(PgOid.NUMERIC);
    expect(inferOidFromValue("-0.001")).toBe(PgOid.NUMERIC);
    expect(inferOidFromValue("999999999999999999.999999")).toBe(PgOid.NUMERIC);
  });

  test("UUID string -> UUID", () => {
    expect(inferOidFromValue("550e8400-e29b-41d4-a716-446655440000")).toBe(PgOid.UUID);
    expect(inferOidFromValue("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")).toBe(PgOid.UUID);
    // Lowercase
    expect(inferOidFromValue("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11".toLowerCase())).toBe(PgOid.UUID);
  });

  test("TIME string -> TIME", () => {
    expect(inferOidFromValue("14:30:00")).toBe(PgOid.TIME);
    expect(inferOidFromValue("00:00:00")).toBe(PgOid.TIME);
    expect(inferOidFromValue("23:59:59")).toBe(PgOid.TIME);
  });

  test("TIMETZ string -> TIMETZ", () => {
    // With timezone offset
    expect(inferOidFromValue("14:30:00+03")).toBe(PgOid.TIMETZ);
    expect(inferOidFromValue("14:30:00-05:00")).toBe(PgOid.TIMETZ);
    expect(inferOidFromValue("00:00:00+00")).toBe(PgOid.TIMETZ);
    // Full timezone offset with minutes
    expect(inferOidFromValue("14:30:00+05:30")).toBe(PgOid.TIMETZ);
    expect(inferOidFromValue("14:30:00-03:30")).toBe(PgOid.TIMETZ);
  });

  test("MONEY string -> MONEY", () => {
    expect(inferOidFromValue("$100.50")).toBe(PgOid.MONEY);
    expect(inferOidFromValue("$1,000.00")).toBe(PgOid.MONEY);
    expect(inferOidFromValue("$0.99")).toBe(PgOid.MONEY);
  });

  test("BIT string -> BIT", () => {
    // Bit strings (only 0s and 1s)
    expect(inferOidFromValue("10101010")).toBe(PgOid.BIT);
    expect(inferOidFromValue("0")).toBe(PgOid.BIT);
    expect(inferOidFromValue("1")).toBe(PgOid.BIT);
    expect(inferOidFromValue("1111000011110000")).toBe(PgOid.BIT);
  });

  test("regular numeric string is not BIT", () => {
    // Regular integers should not be detected as BIT
    expect(inferOidFromValue("123")).toBe(PgOid.TEXT); // Small number -> TEXT
    expect(inferOidFromValue("1234567890")).toBe(PgOid.TEXT); // 10-digit number -> TEXT
    expect(inferOidFromValue("3000000000")).toBe(PgOid.INT8); // Big number -> INT8
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

  test("array of numbers -> INT4_ARRAY, INT8_ARRAY or FLOAT8_ARRAY", () => {
    expect(inferOidFromValue([1, 2, 3])).toBe(PgOid.INT4_ARRAY);
    expect(inferOidFromValue([2147483647])).toBe(PgOid.INT4_ARRAY); // INT32_MAX
    expect(inferOidFromValue([2147483648])).toBe(PgOid.INT8_ARRAY); // INT32_MAX + 1
    expect(inferOidFromValue([1.5, 2.5])).toBe(PgOid.FLOAT8_ARRAY);
  });

  test("array of numeric strings -> NUMERIC_ARRAY", () => {
    expect(inferOidFromValue(["99.99", "100.50"])).toBe(PgOid.NUMERIC_ARRAY);
    expect(inferOidFromValue(["0.001"])).toBe(PgOid.NUMERIC_ARRAY);
  });

  test("array of bigint strings -> INT8_ARRAY", () => {
    expect(inferOidFromValue(["3000000000", "4000000000"])).toBe(PgOid.INT8_ARRAY);
    expect(inferOidFromValue(["2147483648"])).toBe(PgOid.INT8_ARRAY);
  });

  test("array of UUID strings -> UUID_ARRAY", () => {
    expect(inferOidFromValue(["550e8400-e29b-41d4-a716-446655440000"])).toBe(PgOid.UUID_ARRAY);
    expect(inferOidFromValue(["550e8400-e29b-41d4-a716-446655440000", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"])).toBe(
      PgOid.UUID_ARRAY,
    );
  });

  test("array of TIME strings -> TIME_ARRAY", () => {
    expect(inferOidFromValue(["14:30:00"])).toBe(PgOid.TIME_ARRAY);
    expect(inferOidFromValue(["00:00:00", "23:59:59"])).toBe(PgOid.TIME_ARRAY);
  });

  test("array of TIMETZ strings -> TIMETZ_ARRAY", () => {
    expect(inferOidFromValue(["14:30:00+03"])).toBe(PgOid.TIMETZ_ARRAY);
    expect(inferOidFromValue(["14:30:00+03", "10:00:00-05:00"])).toBe(PgOid.TIMETZ_ARRAY);
  });

  test("array of MONEY strings -> MONEY_ARRAY", () => {
    expect(inferOidFromValue(["$100.50"])).toBe(PgOid.MONEY_ARRAY);
    expect(inferOidFromValue(["$100.50", "$200.00"])).toBe(PgOid.MONEY_ARRAY);
  });

  test("array of BIT strings -> BIT_ARRAY", () => {
    expect(inferOidFromValue(["10101010"])).toBe(PgOid.BIT_ARRAY);
    expect(inferOidFromValue(["0", "1"])).toBe(PgOid.BIT_ARRAY);
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
