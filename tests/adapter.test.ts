import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { SQL } from 'bun'
import { PrismaBunAdapter } from '../src/adapter.ts'
import { ColumnTypeEnum } from '@prisma/driver-adapter-utils'

const DB_URL = process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/postgres'
const canConnect = !!DB_URL

describe.skipIf(!canConnect)('PrismaBunAdapter integration', () => {
  let client: InstanceType<typeof SQL>
  let adapter: PrismaBunAdapter

  beforeAll(async () => {
    client = new SQL(DB_URL)
    adapter = new PrismaBunAdapter(client)

    await client.unsafe(`
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
    `).simple()
  })

  afterAll(async () => {
    await client.unsafe('DROP TABLE IF EXISTS _adapter_test').simple()
    await client.close()
  })

  test('queryRaw: SELECT with various types', async () => {
    await client.unsafe(`
      INSERT INTO _adapter_test (name, email, age, score, active, metadata, tags, data)
      VALUES ('Alice', 'alice@test.com', 30, 95.5, true, '{"role": "admin"}', ARRAY['tag1', 'tag2'], '\\x68656c6c6f')
    `).simple()

    const result = await adapter.queryRaw({
      sql: 'SELECT id, name, email, age, score, active, metadata, tags, created_at, data FROM _adapter_test WHERE name = $1',
      args: ['Alice'],
      argTypes: [{ scalarType: 'string', arity: 'scalar' }],
    })

    expect(result.columnNames).toEqual(['id', 'name', 'email', 'age', 'score', 'active', 'metadata', 'tags', 'created_at', 'data'])
    expect(result.rows.length).toBe(1)

    const row = result.rows[0]!
    expect(row[1]).toBe('Alice')           // name
    expect(row[2]).toBe('alice@test.com')   // email
    expect(row[3]).toBe(30)                 // age
    expect(row[4]).toBe(95.5)               // score
    expect(row[5]).toBe(true)               // active
  })

  test('queryRaw: JSON is returned as string', async () => {
    const result = await adapter.queryRaw({
      sql: "SELECT metadata FROM _adapter_test WHERE name = 'Alice'",
      args: [],
      argTypes: [],
    })

    const jsonVal = result.rows[0]![0]
    expect(typeof jsonVal).toBe('string')
    expect(JSON.parse(jsonVal as string)).toEqual({ role: 'admin' })
  })

  test('queryRaw: empty result', async () => {
    const result = await adapter.queryRaw({
      sql: "SELECT * FROM _adapter_test WHERE name = 'NonExistent'",
      args: [],
      argTypes: [],
    })

    expect(result.rows.length).toBe(0)
    expect(result.columnNames).toEqual([])
  })

  test('executeRaw: INSERT returns row count', async () => {
    const count = await adapter.executeRaw({
      sql: "INSERT INTO _adapter_test (name, age) VALUES ($1, $2), ($3, $4)",
      args: ['Bob', 25, 'Charlie', 35],
      argTypes: [
        { scalarType: 'string', arity: 'scalar' },
        { scalarType: 'int', arity: 'scalar' },
        { scalarType: 'string', arity: 'scalar' },
        { scalarType: 'int', arity: 'scalar' },
      ],
    })

    expect(count).toBe(2)
  })

  test('executeRaw: UPDATE returns affected count', async () => {
    const count = await adapter.executeRaw({
      sql: "UPDATE _adapter_test SET active = $1 WHERE age > $2",
      args: [false, 30],
      argTypes: [
        { scalarType: 'boolean', arity: 'scalar' },
        { scalarType: 'int', arity: 'scalar' },
      ],
    })

    expect(count).toBeGreaterThanOrEqual(1)
  })

  test('executeRaw: DELETE returns affected count', async () => {
    const count = await adapter.executeRaw({
      sql: "DELETE FROM _adapter_test WHERE name = $1",
      args: ['Charlie'],
      argTypes: [{ scalarType: 'string', arity: 'scalar' }],
    })

    expect(count).toBe(1)
  })

  test('startTransaction: commit', async () => {
    const tx = await adapter.startTransaction()

    await tx.executeRaw({
      sql: "INSERT INTO _adapter_test (name, age) VALUES ($1, $2)",
      args: ['TxUser', 40],
      argTypes: [
        { scalarType: 'string', arity: 'scalar' },
        { scalarType: 'int', arity: 'scalar' },
      ],
    })

    // Prisma Engine sends COMMIT via executeRaw
    await tx.executeRaw({ sql: 'COMMIT', args: [], argTypes: [] })
    await tx.commit()

    // Verify data persisted
    const result = await adapter.queryRaw({
      sql: "SELECT name FROM _adapter_test WHERE name = 'TxUser'",
      args: [],
      argTypes: [],
    })
    expect(result.rows.length).toBe(1)
  })

  test('startTransaction: rollback', async () => {
    const tx = await adapter.startTransaction()

    await tx.executeRaw({
      sql: "INSERT INTO _adapter_test (name, age) VALUES ($1, $2)",
      args: ['RollbackUser', 50],
      argTypes: [
        { scalarType: 'string', arity: 'scalar' },
        { scalarType: 'int', arity: 'scalar' },
      ],
    })

    // Prisma Engine sends ROLLBACK via executeRaw
    await tx.executeRaw({ sql: 'ROLLBACK', args: [], argTypes: [] })
    await tx.rollback()

    // Verify data was rolled back
    const result = await adapter.queryRaw({
      sql: "SELECT name FROM _adapter_test WHERE name = 'RollbackUser'",
      args: [],
      argTypes: [],
    })
    expect(result.rows.length).toBe(0)
  })

  test('startTransaction: with isolation level', async () => {
    const tx = await adapter.startTransaction('SERIALIZABLE')

    const result = await tx.queryRaw({
      sql: "SELECT current_setting('transaction_isolation')",
      args: [],
      argTypes: [],
    })

    expect(result.rows[0]![0]).toBe('serializable')

    await tx.executeRaw({ sql: 'ROLLBACK', args: [], argTypes: [] })
    await tx.rollback()
  })

  test('executeScript: multiple statements', async () => {
    await adapter.executeScript(`
      CREATE TEMP TABLE _script_test (id int);
      INSERT INTO _script_test VALUES (1);
      INSERT INTO _script_test VALUES (2);
      DROP TABLE _script_test;
    `)
    // If we get here without error, the script worked
  })

  test('getConnectionInfo', () => {
    const info = adapter.getConnectionInfo()
    expect(info.schemaName).toBe('public')
    expect(info.supportsRelationJoins).toBe(true)
  })

  test('queryRaw: parameterized query with various types', async () => {
    const result = await adapter.queryRaw({
      sql: 'SELECT $1::text as txt, $2::int as num, $3::bool as flag',
      args: ['hello', 42, true],
      argTypes: [
        { scalarType: 'string', arity: 'scalar' },
        { scalarType: 'int', arity: 'scalar' },
        { scalarType: 'boolean', arity: 'scalar' },
      ],
    })

    expect(result.rows[0]).toEqual(['hello', 42, true])
  })

  test('error handling: table does not exist', async () => {
    try {
      await adapter.queryRaw({
        sql: 'SELECT * FROM _nonexistent_table_xyz',
        args: [],
        argTypes: [],
      })
      expect(true).toBe(false) // should not reach here
    } catch (e: any) {
      expect(e.name).toBe('DriverAdapterError')
      expect(e.cause.kind).toBe('TableDoesNotExist')
    }
  })
})
