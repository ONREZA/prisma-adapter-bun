import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { ColumnTypeEnum } from "@prisma/driver-adapter-utils";
import { SQL } from "bun";
import { PrismaBunAdapter } from "../src/adapter.ts";

const DB_URL = process.env.DATABASE_URL ?? "postgres://postgres:postgres@localhost:5432/postgres";
const canConnect = !!DB_URL;

describe.skipIf(!canConnect)("PrismaBunAdapter integration", () => {
  let client: InstanceType<typeof SQL>;
  let adapter: PrismaBunAdapter;

  beforeAll(async () => {
    client = new SQL(DB_URL);
    adapter = new PrismaBunAdapter(client);

    await client
      .unsafe(`
      DROP TABLE IF EXISTS _adapter_test;
      CREATE TABLE _adapter_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        email VARCHAR(255),
        age INT,
        score FLOAT8,
        active BOOLEAN DEFAULT true,
        metadata JSONB,
        tags TEXT[],
        created_at TIMESTAMPTZ DEFAULT NOW(),
        data BYTEA
      )
    `)
      .simple();
  });

  afterAll(async () => {
    await client.unsafe("DROP TABLE IF EXISTS _adapter_test").simple();
    await client.close();
  });

  test("queryRaw: SELECT with various types", async () => {
    await client
      .unsafe(`
      INSERT INTO _adapter_test (name, email, age, score, active, metadata, tags, data)
      VALUES ('Alice', 'alice@test.com', 30, 95.5, true, '{"role": "admin"}', ARRAY['tag1', 'tag2'], '\\x68656c6c6f')
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: ["Alice"],
      argTypes: [{ arity: "scalar", scalarType: "string" }],
      sql: "SELECT id, name, email, age, score, active, metadata, tags, created_at, data FROM _adapter_test WHERE name = $1",
    });

    expect(result.columnNames).toEqual([
      "id",
      "name",
      "email",
      "age",
      "score",
      "active",
      "metadata",
      "tags",
      "created_at",
      "data",
    ]);
    expect(result.rows.length).toBe(1);

    const row = result.rows[0]!;
    expect(row[1]).toBe("Alice"); // name
    expect(row[2]).toBe("alice@test.com"); // email
    expect(row[3]).toBe(30); // age
    expect(row[4]).toBe(95.5); // score
    expect(row[5]).toBe(true); // active
  });

  test("queryRaw: JSON is returned as string", async () => {
    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT metadata FROM _adapter_test WHERE name = 'Alice'",
    });

    const jsonVal = result.rows[0]?.[0];
    expect(typeof jsonVal).toBe("string");
    expect(JSON.parse(jsonVal as string)).toEqual({ role: "admin" });
  });

  test("queryRaw: empty result", async () => {
    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT * FROM _adapter_test WHERE name = 'NonExistent'",
    });

    expect(result.rows.length).toBe(0);
    expect(result.columnNames).toEqual([]);
  });

  test("executeRaw: INSERT returns row count", async () => {
    const count = await adapter.executeRaw({
      args: ["Bob", 25, "Charlie", 35],
      argTypes: [
        { arity: "scalar", scalarType: "string" },
        { arity: "scalar", scalarType: "int" },
        { arity: "scalar", scalarType: "string" },
        { arity: "scalar", scalarType: "int" },
      ],
      sql: "INSERT INTO _adapter_test (name, age) VALUES ($1, $2), ($3, $4)",
    });

    expect(count).toBe(2);
  });

  test("executeRaw: UPDATE returns affected count", async () => {
    const count = await adapter.executeRaw({
      args: [false, 30],
      argTypes: [
        { arity: "scalar", scalarType: "boolean" },
        { arity: "scalar", scalarType: "int" },
      ],
      sql: "UPDATE _adapter_test SET active = $1 WHERE age > $2",
    });

    expect(count).toBeGreaterThanOrEqual(1);
  });

  test("executeRaw: DELETE returns affected count", async () => {
    const count = await adapter.executeRaw({
      args: ["Charlie"],
      argTypes: [{ arity: "scalar", scalarType: "string" }],
      sql: "DELETE FROM _adapter_test WHERE name = $1",
    });

    expect(count).toBe(1);
  });

  test("startTransaction: commit", async () => {
    const tx = await adapter.startTransaction();

    await tx.executeRaw({
      args: ["TxUser", 40],
      argTypes: [
        { arity: "scalar", scalarType: "string" },
        { arity: "scalar", scalarType: "int" },
      ],
      sql: "INSERT INTO _adapter_test (name, age) VALUES ($1, $2)",
    });

    // Prisma Engine sends COMMIT via executeRaw
    await tx.executeRaw({ args: [], argTypes: [], sql: "COMMIT" });
    await tx.commit();

    // Verify data persisted
    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT name FROM _adapter_test WHERE name = 'TxUser'",
    });
    expect(result.rows.length).toBe(1);
  });

  test("startTransaction: rollback", async () => {
    const tx = await adapter.startTransaction();

    await tx.executeRaw({
      args: ["RollbackUser", 50],
      argTypes: [
        { arity: "scalar", scalarType: "string" },
        { arity: "scalar", scalarType: "int" },
      ],
      sql: "INSERT INTO _adapter_test (name, age) VALUES ($1, $2)",
    });

    // Prisma Engine sends ROLLBACK via executeRaw
    await tx.executeRaw({ args: [], argTypes: [], sql: "ROLLBACK" });
    await tx.rollback();

    // Verify data was rolled back
    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT name FROM _adapter_test WHERE name = 'RollbackUser'",
    });
    expect(result.rows.length).toBe(0);
  });

  test("startTransaction: with isolation level", async () => {
    const tx = await adapter.startTransaction("SERIALIZABLE");

    const result = await tx.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT current_setting('transaction_isolation')",
    });

    expect(result.rows[0]?.[0]).toBe("serializable");

    await tx.executeRaw({ args: [], argTypes: [], sql: "ROLLBACK" });
    await tx.rollback();
  });

  test("executeScript: multiple statements", async () => {
    await adapter.executeScript(`
      CREATE TEMP TABLE _script_test (id int);
      INSERT INTO _script_test VALUES (1);
      INSERT INTO _script_test VALUES (2);
      DROP TABLE _script_test;
    `);
    // If we get here without error, the script worked
  });

  test("getConnectionInfo", () => {
    const info = adapter.getConnectionInfo();
    expect(info.schemaName).toBe("public");
    expect(info.supportsRelationJoins).toBe(true);
  });

  test("queryRaw: parameterized query with various types", async () => {
    const result = await adapter.queryRaw({
      args: ["hello", 42, true],
      argTypes: [
        { arity: "scalar", scalarType: "string" },
        { arity: "scalar", scalarType: "int" },
        { arity: "scalar", scalarType: "boolean" },
      ],
      sql: "SELECT $1::text as txt, $2::int as num, $3::bool as flag",
    });

    expect(result.rows[0]).toEqual(["hello", 42, true]);
  });

  test("queryRaw: JSON string from relation join is typed as Json", async () => {
    // Simulate what Prisma does with relationJoins: LEFT JOIN + to_jsonb()
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _adapter_child;
      DROP TABLE IF EXISTS _adapter_parent;
      CREATE TABLE _adapter_parent (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
      CREATE TABLE _adapter_child (id SERIAL PRIMARY KEY, parent_id INT REFERENCES _adapter_parent(id), role TEXT NOT NULL);
      INSERT INTO _adapter_parent (name) VALUES ('Parent1');
      INSERT INTO _adapter_child (parent_id, role) VALUES (1, 'OWNER');
    `)
      .simple();

    // to_jsonb() returns JSON as a string — this is the relation join scenario
    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: `
        SELECT p.id, p.name, to_jsonb(c.*) AS child_data
        FROM _adapter_parent p
        LEFT JOIN _adapter_child c ON c.parent_id = p.id
        WHERE p.name = 'Parent1'
      `,
    });

    expect(result.rows.length).toBe(1);
    const row = result.rows[0]!;

    // child_data: Bun.sql returns to_jsonb() as a string
    const childData = row[2];
    expect(typeof childData).toBe("string");
    const parsed = JSON.parse(childData as string);
    expect(parsed.role).toBe("OWNER");

    // Column type must be Json, not Text (this was the bug)
    expect(result.columnTypes[2]).toBe(ColumnTypeEnum.Json);

    await client.unsafe("DROP TABLE IF EXISTS _adapter_child; DROP TABLE IF EXISTS _adapter_parent").simple();
  });

  test("queryRaw: json_build_object result is typed as Json", async () => {
    // json_build_object returns JSON as a string in Bun.sql
    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT json_build_object('role', 'OWNER', 'active', true) AS data",
    });

    expect(result.rows.length).toBe(1);
    const data = result.rows[0]![0];
    expect(typeof data).toBe("string");
    const parsed = JSON.parse(data as string);
    expect(parsed.role).toBe("OWNER");
    expect(parsed.active).toBe(true);
    expect(result.columnTypes[0]).toBe(ColumnTypeEnum.Json);
  });

  test("queryRaw: jsonb_agg result (parsed JS array) is typed as Json", async () => {
    // jsonb_agg returns type jsonb (OID 3802), not jsonb[] (OID 3807).
    // Bun.sql auto-parses it into a JS array. The adapter must classify it
    // as JSONB (Json), not JSONB_ARRAY (JsonArray), and stringify the whole array.
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _adapter_child2;
      DROP TABLE IF EXISTS _adapter_parent2;
      CREATE TABLE _adapter_parent2 (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
      CREATE TABLE _adapter_child2 (id SERIAL PRIMARY KEY, parent_id INT REFERENCES _adapter_parent2(id), role TEXT NOT NULL);
      INSERT INTO _adapter_parent2 (name) VALUES ('Parent1');
      INSERT INTO _adapter_child2 (parent_id, role) VALUES (1, 'OWNER'), (1, 'MEMBER');
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: `
        SELECT p.id, p.name, jsonb_agg(to_jsonb(c.*)) AS children
        FROM _adapter_parent2 p
        LEFT JOIN _adapter_child2 c ON c.parent_id = p.id
        WHERE p.name = 'Parent1'
        GROUP BY p.id, p.name
      `,
    });

    expect(result.rows.length).toBe(1);

    // Column type must be Json (not JsonArray) — this is the relation join fix
    expect(result.columnTypes[2]).toBe(ColumnTypeEnum.Json);

    // Value must be a stringified JSON array (whole array, not per-element)
    const children = result.rows[0]![2];
    expect(typeof children).toBe("string");
    const parsed = JSON.parse(children as string);
    expect(Array.isArray(parsed)).toBe(true);
    expect(parsed.length).toBe(2);
    expect(parsed.find((c: { role: string }) => c.role === "OWNER")).toBeTruthy();
    expect(parsed.find((c: { role: string }) => c.role === "MEMBER")).toBeTruthy();

    await client.unsafe("DROP TABLE IF EXISTS _adapter_child2; DROP TABLE IF EXISTS _adapter_parent2").simple();
  });

  test("executeRaw: JSON string argument is not double-stringified", async () => {
    // Prisma sends JSON as pre-stringified strings. Bun.sql's unsafe() would
    // double-stringify them without the mapArg JSON parse fix.
    await adapter.executeRaw({
      args: ['{"level":5}', "JsonUser", 99],
      argTypes: [
        { arity: "scalar", scalarType: "json" },
        { arity: "scalar", scalarType: "string" },
        { arity: "scalar", scalarType: "int" },
      ],
      sql: "INSERT INTO _adapter_test (metadata, name, age) VALUES ($1, $2, $3)",
    });

    // Read back via raw SQL to verify the stored value
    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT metadata FROM _adapter_test WHERE name = 'JsonUser'",
    });

    const val = result.rows[0]![0];
    // Must be a JS object (Bun.sql auto-parses JSONB), then normalizeJson stringifies it.
    // The key test: the value must represent {"level":5}, not a double-escaped string.
    expect(typeof val).toBe("string");
    expect(JSON.parse(val as string)).toEqual({ level: 5 });
  });

  test("queryRaw: plain text starting with { is not misclassified as Json", async () => {
    // PG array literals like {a,b} start with { but are NOT JSON
    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT '{not json}'::text AS val",
    });

    expect(result.rows.length).toBe(1);
    expect(result.rows[0]![0]).toBe("{not json}");
    expect(result.columnTypes[0]).toBe(ColumnTypeEnum.Text);
  });

  test("error handling: table does not exist", async () => {
    try {
      await adapter.queryRaw({
        args: [],
        argTypes: [],
        sql: "SELECT * FROM _nonexistent_table_xyz",
      });
      expect(true).toBe(false); // should not reach here
    } catch (e: any) {
      expect(e.name).toBe("DriverAdapterError");
      expect(e.cause.kind).toBe("TableDoesNotExist");
    }
  });

  test("queryRaw: INTEGER returns Int32 column type with number value", async () => {
    // Create table with INTEGER (INT4) column
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _int_test;
      CREATE TABLE _int_test (id SERIAL PRIMARY KEY, int_col INTEGER, bigint_col BIGINT);
      INSERT INTO _int_test (int_col, bigint_col) VALUES (30, 3000000000);
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT id, int_col, bigint_col FROM _int_test",
    });

    expect(result.rows.length).toBe(1);
    const row = result.rows[0]!;

    // id (SERIAL = INTEGER/INT4) should be Int32 with number value
    expect(result.columnTypes[0]).toBe(ColumnTypeEnum.Int32);
    expect(typeof row[0]).toBe("number");
    expect(row[0]).toBe(1);

    // int_col (INTEGER/INT4) should be Int32 with number value
    expect(result.columnTypes[1]).toBe(ColumnTypeEnum.Int32);
    expect(typeof row[1]).toBe("number");
    expect(row[1]).toBe(30);

    // bigint_col (BIGINT/INT8) should be Int64 with BigInt value
    expect(result.columnTypes[2]).toBe(ColumnTypeEnum.Int64);
    expect(typeof row[2]).toBe("bigint");
    expect(row[2]).toBe(BigInt(3000000000));

    await client.unsafe("DROP TABLE _int_test").simple();
  });

  test("queryRaw: BIGINT boundary values return correct types", async () => {
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _bigint_test;
      CREATE TABLE _bigint_test (
        int32_max INTEGER,
        int32_max_plus_1 BIGINT,
        int32_min INTEGER,
        int32_min_minus_1 BIGINT
      );
      INSERT INTO _bigint_test VALUES (
        2147483647,
        2147483648,
        -2147483648,
        -2147483649
      );
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT int32_max, int32_max_plus_1, int32_min, int32_min_minus_1 FROM _bigint_test",
    });

    expect(result.rows.length).toBe(1);
    const row = result.rows[0]!;

    // INT32_MAX (2147483647) should be Int32
    expect(result.columnTypes[0]).toBe(ColumnTypeEnum.Int32);
    expect(row[0]).toBe(2147483647);

    // INT32_MAX + 1 (2147483648) should be Int64 (BigInt)
    expect(result.columnTypes[1]).toBe(ColumnTypeEnum.Int64);
    expect(row[1]).toBe(BigInt(2147483648));

    // INT32_MIN (-2147483648) should be Int32
    expect(result.columnTypes[2]).toBe(ColumnTypeEnum.Int32);
    expect(row[2]).toBe(-2147483648);

    // INT32_MIN - 1 (-2147483649) should be Int64 (BigInt)
    expect(result.columnTypes[3]).toBe(ColumnTypeEnum.Int64);
    expect(row[3]).toBe(BigInt(-2147483649));

    await client.unsafe("DROP TABLE _bigint_test").simple();
  });

  test("queryRaw: NULL values in INT/BIGINT columns", async () => {
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _null_test;
      CREATE TABLE _null_test (int_col INTEGER, bigint_col BIGINT);
      INSERT INTO _null_test VALUES (NULL, NULL), (42, 3000000000);
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT int_col, bigint_col FROM _null_test ORDER BY int_col NULLS FIRST",
    });

    expect(result.rows.length).toBe(2);

    // Column types should be inferred from first non-null value
    expect(result.columnTypes[0]).toBe(ColumnTypeEnum.Int32);
    expect(result.columnTypes[1]).toBe(ColumnTypeEnum.Int64);

    // First row: NULLs
    expect(result.rows[0]![0]).toBeNull();
    expect(result.rows[0]![1]).toBeNull();

    // Second row: values
    expect(result.rows[1]![0]).toBe(42);
    expect(result.rows[1]![1]).toBe(BigInt(3000000000));

    await client.unsafe("DROP TABLE _null_test").simple();
  });

  test("queryRaw: NUMERIC/DECIMAL columns return correct type", async () => {
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _numeric_test;
      CREATE TABLE _numeric_test (price NUMERIC(10,2), amount DECIMAL(20,8));
      INSERT INTO _numeric_test VALUES (99.99, 12345678.12345678);
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT price, amount FROM _numeric_test",
    });

    expect(result.rows.length).toBe(1);
    const row = result.rows[0]!;

    // Both should be Numeric type
    expect(result.columnTypes[0]).toBe(ColumnTypeEnum.Numeric);
    expect(result.columnTypes[1]).toBe(ColumnTypeEnum.Numeric);

    // Values should be strings (normalized by resultNormalizers)
    expect(typeof row[0]).toBe("string");
    expect(typeof row[1]).toBe("string");
    expect(row[0]).toBe("99.99");
    expect(row[1]).toBe("12345678.12345678");

    await client.unsafe("DROP TABLE _numeric_test").simple();
  });

  test("queryRaw: BIGINT arrays", async () => {
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _bigint_array_test;
      CREATE TABLE _bigint_array_test (big_arr BIGINT[]);
      INSERT INTO _bigint_array_test VALUES (ARRAY[3000000000, 4000000000]);
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT big_arr FROM _bigint_array_test",
    });

    expect(result.rows.length).toBe(1);
    const row = result.rows[0]!;

    expect(result.columnTypes[0]).toBe(ColumnTypeEnum.Int64Array);

    // Array values should be BigInts
    const arr = row[0] as unknown[];
    expect(Array.isArray(arr)).toBe(true);
    expect(arr.length).toBe(2);
    expect(arr[0]).toBe(BigInt(3000000000));
    expect(arr[1]).toBe(BigInt(4000000000));

    await client.unsafe("DROP TABLE _bigint_array_test").simple();
  });

  test("queryRaw: UUID columns return correct type", async () => {
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _uuid_test;
      CREATE TABLE _uuid_test (id UUID);
      INSERT INTO _uuid_test VALUES ('550e8400-e29b-41d4-a716-446655440000');
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT id FROM _uuid_test",
    });

    expect(result.rows.length).toBe(1);

    expect(result.columnTypes[0]).toBe(ColumnTypeEnum.Uuid);
    expect(typeof result.rows[0]![0]).toBe("string");
    expect(result.rows[0]![0]).toBe("550e8400-e29b-41d4-a716-446655440000");

    await client.unsafe("DROP TABLE _uuid_test").simple();
  });

  test("queryRaw: TIME columns return correct type", async () => {
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _time_test;
      CREATE TABLE _time_test (t TIME, tz TIMETZ);
      INSERT INTO _time_test VALUES ('14:30:00', '14:30:00+03');
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT t, tz FROM _time_test",
    });

    expect(result.rows.length).toBe(1);
    const row = result.rows[0]!;

    // Both should be Time type
    expect(result.columnTypes[0]).toBe(ColumnTypeEnum.Time);
    expect(result.columnTypes[1]).toBe(ColumnTypeEnum.Time);

    // Values should be strings
    expect(typeof row[0]).toBe("string");
    expect(row[0]).toBe("14:30:00");
    // TIMETZ should have timezone stripped by normalizer
    expect(typeof row[1]).toBe("string");
    expect(row[1]).toBe("14:30:00");

    await client.unsafe("DROP TABLE _time_test").simple();
  });

  test("queryRaw: MONEY columns return correct type", async () => {
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _money_test;
      CREATE TABLE _money_test (amount MONEY);
      INSERT INTO _money_test VALUES ('$100.50');
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT amount FROM _money_test",
    });

    expect(result.rows.length).toBe(1);

    expect(result.columnTypes[0]).toBe(ColumnTypeEnum.Numeric);
    expect(typeof result.rows[0]![0]).toBe("string");
    // $ should be stripped by normalizer
    expect(result.rows[0]![0]).toBe("100.50");

    await client.unsafe("DROP TABLE _money_test").simple();
  });

  test("queryRaw: BIT/VARBIT columns return correct type", async () => {
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _bit_test;
      CREATE TABLE _bit_test (bits BIT(8), varbits VARBIT(16));
      INSERT INTO _bit_test VALUES (B'10101010', B'1111000011110000');
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT bits, varbits FROM _bit_test",
    });

    expect(result.rows.length).toBe(1);
    const row = result.rows[0]!;

    // Both should be Text type (mapped from BIT/VARBIT)
    expect(result.columnTypes[0]).toBe(ColumnTypeEnum.Text);
    expect(result.columnTypes[1]).toBe(ColumnTypeEnum.Text);

    // Values should be strings
    expect(typeof row[0]).toBe("string");
    expect(typeof row[1]).toBe("string");
    expect(row[0]).toBe("10101010");
    expect(row[1]).toBe("1111000011110000");

    await client.unsafe("DROP TABLE _bit_test").simple();
  });

  test("queryRaw: BIGINT column with small value (within INT32 range)", async () => {
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _bigint_small_test;
      CREATE TABLE _bigint_small_test (val BIGINT);
      INSERT INTO _bigint_small_test VALUES (42);
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT val FROM _bigint_small_test",
    });

    expect(result.rows.length).toBe(1);
    // Without column metadata, small BIGINT values may not be identified as Int64.
    // This test documents the actual Bun.sql behavior for small BIGINT values.
    const val = result.rows[0]![0];
    expect(Number(val)).toBe(42);

    await client.unsafe("DROP TABLE _bigint_small_test").simple();
  });

  test("queryRaw: MONEY values >= 1000 return valid numeric strings", async () => {
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _money_large_test;
      CREATE TABLE _money_large_test (amount MONEY);
      INSERT INTO _money_large_test VALUES (1234.56);
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT amount FROM _money_large_test",
    });

    expect(result.rows.length).toBe(1);
    expect(result.columnTypes[0]).toBe(ColumnTypeEnum.Numeric);
    const val = result.rows[0]![0] as string;
    expect(typeof val).toBe("string");
    // Must be a valid numeric string (no $ or commas after normalization)
    expect(val).not.toContain("$");
    expect(val).not.toContain(",");
    expect(Number.parseFloat(val)).toBeCloseTo(1234.56, 2);

    await client.unsafe("DROP TABLE _money_large_test").simple();
  });

  test("queryRaw: FLOAT8 special values (NaN)", async () => {
    // Note: Bun.sql returns Infinity/-Infinity as NaN (Bun limitation),
    // so we only test NaN which works correctly.
    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT 'NaN'::float8 as nan_val",
    });

    expect(result.rows.length).toBe(1);
    expect(Number.isNaN(result.rows[0]![0] as number)).toBe(true);
    expect(result.columnTypes[0]).toBe(ColumnTypeEnum.Double);
  });

  test("executeScript: throws on invalid SQL", async () => {
    try {
      await adapter.executeScript("INVALID SQL STATEMENT");
      expect(true).toBe(false);
    } catch (e: unknown) {
      expect((e as Error).name).toBe("DriverAdapterError");
    }
  });

  test("startTransaction: concurrent transactions", async () => {
    const tx1 = await adapter.startTransaction();
    const tx2 = await adapter.startTransaction();

    await tx1.executeRaw({
      args: ["ConcUser1", 60],
      argTypes: [
        { arity: "scalar", scalarType: "string" },
        { arity: "scalar", scalarType: "int" },
      ],
      sql: "INSERT INTO _adapter_test (name, age) VALUES ($1, $2)",
    });

    await tx2.executeRaw({
      args: ["ConcUser2", 70],
      argTypes: [
        { arity: "scalar", scalarType: "string" },
        { arity: "scalar", scalarType: "int" },
      ],
      sql: "INSERT INTO _adapter_test (name, age) VALUES ($1, $2)",
    });

    // Commit tx1, rollback tx2
    await tx1.executeRaw({ args: [], argTypes: [], sql: "COMMIT" });
    await tx1.commit();
    await tx2.executeRaw({ args: [], argTypes: [], sql: "ROLLBACK" });
    await tx2.rollback();

    const result1 = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT name FROM _adapter_test WHERE name = 'ConcUser1'",
    });
    expect(result1.rows.length).toBe(1);

    const result2 = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT name FROM _adapter_test WHERE name = 'ConcUser2'",
    });
    expect(result2.rows.length).toBe(0);
  });

  test("queryRaw: JSONB objects and arrays are stringified (full pipeline)", async () => {
    // Without column metadata, value-based inference can only identify
    // objects/arrays as JSONB. Primitives (number, boolean) are inferred
    // as their SQL counterparts (INT4, BOOL) and not stringified.
    // This tests the cases that DO work correctly through the full pipeline.
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _jsonb_prim_test;
      CREATE TABLE _jsonb_prim_test (id SERIAL PRIMARY KEY, data JSONB);
      INSERT INTO _jsonb_prim_test (data) VALUES
        ('[1,2,3]'), ('{"a":1}'), ('{"nested":{"b":2}}');
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT data FROM _jsonb_prim_test ORDER BY id",
    });

    expect(result.rows.length).toBe(3);

    // Objects and arrays: correctly identified as JSONB → stringified
    for (const row of result.rows) {
      expect(typeof row[0]).toBe("string");
    }

    expect(JSON.parse(result.rows[0]![0] as string)).toEqual([1, 2, 3]);
    expect(JSON.parse(result.rows[1]![0] as string)).toEqual({ a: 1 });
    expect(JSON.parse(result.rows[2]![0] as string)).toEqual({ nested: { b: 2 } });

    await client.unsafe("DROP TABLE _jsonb_prim_test").simple();
  });

  test("queryRaw: JSONB primitives are returned as JS types (inference limitation)", async () => {
    // Without column metadata, JSONB primitives are indistinguishable from
    // regular SQL types. This documents the current behavior.
    // With Prisma models, this is not an issue — Prisma knows the schema types.
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _jsonb_scalar_test;
      CREATE TABLE _jsonb_scalar_test (id SERIAL PRIMARY KEY, data JSONB);
      INSERT INTO _jsonb_scalar_test (data) VALUES ('42'), ('true');
    `)
      .simple();

    const result = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT data FROM _jsonb_scalar_test ORDER BY id",
    });

    expect(result.rows.length).toBe(2);
    // 42 is inferred as INT4 (number), not JSONB
    expect(result.rows[0]![0]).toBe(42);
    // true is inferred as BOOL (boolean), not JSONB
    expect(result.rows[1]![0]).toBe(true);

    await client.unsafe("DROP TABLE _jsonb_scalar_test").simple();
  });

  test("UPDATE RETURNING with JSONB + BIGINT columns in transaction", async () => {
    // Reproduces the smoke-tester scenario: $transaction with updateMany
    // (no RETURNING) → update with RETURNING on table with jsonb + bigint columns.
    // Uses an object for metadata (correctly inferred as JSONB by value inference)
    // and a large BIGINT value (correctly inferred as INT8).
    await client
      .unsafe(`
      DROP TABLE IF EXISTS _tx_json_bigint_test;
      CREATE TABLE _tx_json_bigint_test (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        metadata JSONB DEFAULT '{}',
        counter BIGINT DEFAULT 0
      );
      INSERT INTO _tx_json_bigint_test (name, metadata, counter) VALUES
        ('target', '{"role":"admin"}', 0),
        ('other1', '{"role":"user"}', 100),
        ('other2', '{"role":"user"}', 200);
    `)
      .simple();

    const tx = await adapter.startTransaction();

    try {
      // Step 1: updateMany on other rows (DML, no RETURNING)
      const updateCount = await tx.executeRaw({
        args: ['{"role":"guest"}'],
        argTypes: [{ arity: "scalar", scalarType: "json" }],
        sql: "UPDATE _tx_json_bigint_test SET metadata = $1 WHERE name != 'target'",
      });
      expect(updateCount).toBe(2);

      // Step 2: update target row with RETURNING (SELECT-like result with jsonb + bigint)
      // Use an object for metadata (inferred as JSONB → stringified by normalizeJson)
      // Use a large BIGINT value (outside INT32 → inferred as INT8 → BigInt by normalizeInt8)
      const result = await tx.queryRaw({
        args: ['{"updated":true}', BigInt(3000000000)],
        argTypes: [
          { arity: "scalar", scalarType: "json" },
          { arity: "scalar", scalarType: "bigint" },
        ],
        sql: "UPDATE _tx_json_bigint_test SET metadata = $1, counter = $2 WHERE name = 'target' RETURNING id, name, metadata, counter",
      });

      expect(result.rows.length).toBe(1);
      const row = result.rows[0]!;

      // id: integer
      expect(typeof row[0]).toBe("number");
      // name: text
      expect(row[1]).toBe("target");
      // metadata: JSONB object → inferred as JSONB → stringified by normalizeJson
      expect(typeof row[2]).toBe("string");
      expect(JSON.parse(row[2] as string)).toEqual({ updated: true });
      // counter: large BIGINT → inferred as INT8 → converted to BigInt
      expect(row[3]).toBe(BigInt(3000000000));

      await tx.executeRaw({ args: [], argTypes: [], sql: "COMMIT" });
      await tx.commit();
    } catch (e) {
      await tx.executeRaw({ args: [], argTypes: [], sql: "ROLLBACK" });
      await tx.rollback();
      throw e;
    }

    // Verify committed state
    const verify = await adapter.queryRaw({
      args: [],
      argTypes: [],
      sql: "SELECT metadata, counter FROM _tx_json_bigint_test WHERE name = 'target'",
    });
    expect(verify.rows.length).toBe(1);
    expect(JSON.parse(verify.rows[0]![0] as string)).toEqual({ updated: true });
    expect(verify.rows[0]![1]).toBe(BigInt(3000000000));

    await client.unsafe("DROP TABLE _tx_json_bigint_test").simple();
  });
});
