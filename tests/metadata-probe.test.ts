import { test, expect, describe } from 'bun:test'
import { SQL } from 'bun'

// Diagnostic test to detect when Bun.sql starts exposing column metadata.
// Currently (Bun v1.3.8) .columns is NOT available — tests verify this.
// When Bun adds .columns support, these tests will fail, signaling
// that the adapter can switch from type inference to native OID metadata.

const canConnect = !!process.env.DATABASE_URL

describe.skipIf(!canConnect)('Bun.sql column metadata probe', () => {
  let db: InstanceType<typeof SQL>

  test('connect to database', async () => {
    db = new SQL(process.env.DATABASE_URL!)
    await db.connect()
  })

  test('result.columns is NOT available in current Bun version', async () => {
    const result = await db.unsafe('SELECT 1::int4 as num, \'hello\'::text as txt').values()
    const columns = (result as any).columns

    // When this starts passing, the adapter can use native metadata!
    expect(columns).toBeUndefined()
  })

  test('result.count IS available for DML', async () => {
    await db.unsafe('CREATE TEMP TABLE _probe_test (id int)').simple()
    await db.unsafe('INSERT INTO _probe_test VALUES (1), (2), (3)').simple()

    const updateResult = await db.unsafe('UPDATE _probe_test SET id = id + 10 WHERE id > 1')
    expect((updateResult as any).count).toBe(2)

    await db.unsafe('DROP TABLE _probe_test').simple()
  })

  test('close connection', async () => {
    await db.close()
  })
})
