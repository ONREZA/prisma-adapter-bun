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
});
