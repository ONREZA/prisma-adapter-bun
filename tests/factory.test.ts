import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { SQL } from 'bun'
import { startPostgres, type PgContainer } from './helpers/pg-container.ts'
import { PrismaBunFactory } from '../src/factory.ts'
import { PrismaBunAdapter } from '../src/adapter.ts'

const TIMEOUT = 60_000

let pg: PgContainer

beforeAll(async () => {
  pg = await startPostgres()
}, TIMEOUT)

afterAll(async () => {
  await pg?.stop()
})

describe('PrismaBunFactory', () => {
  describe('constructor', () => {
    test('accepts connection string', () => {
      const factory = new PrismaBunFactory(pg.connectionUri)
      expect(factory.provider).toBe('postgres')
      expect(factory.adapterName).toBe('@onreza/prisma-adapter-bun')
    })

    test('accepts URL object', () => {
      const factory = new PrismaBunFactory(new URL(pg.connectionUri))
      expect(factory.provider).toBe('postgres')
    })

    test('accepts existing SQL instance', async () => {
      const client = new SQL(pg.connectionUri)
      const factory = new PrismaBunFactory(client)
      expect(factory.provider).toBe('postgres')
      await client.close()
    })

    test('accepts options', () => {
      const factory = new PrismaBunFactory(pg.connectionUri, { schema: 'custom' })
      expect(factory.provider).toBe('postgres')
    })
  })

  describe('connect()', () => {
    test('returns a working SqlDriverAdapter', async () => {
      const factory = new PrismaBunFactory(pg.connectionUri)
      const adapter = await factory.connect()

      expect(adapter.provider).toBe('postgres')
      expect(adapter.adapterName).toBe('@onreza/prisma-adapter-bun')

      const result = await adapter.queryRaw({
        sql: 'SELECT 1 as num',
        args: [],
        argTypes: [],
      })

      expect(result.rows[0]![0]).toBe(1)
      await adapter.dispose()
    })

    test('with external client does not close on dispose', async () => {
      const client = new SQL(pg.connectionUri)
      const factory = new PrismaBunFactory(client)
      const adapter = await factory.connect()

      const result = await adapter.queryRaw({
        sql: 'SELECT 42 as num',
        args: [],
        argTypes: [],
      })
      expect(result.rows[0]![0]).toBe(42)

      await adapter.dispose()

      // External client should still work after adapter dispose
      const check = await client.unsafe('SELECT 1 as alive')
      expect((check[0] as any).alive).toBe(1)

      await client.close()
    })

    test('getConnectionInfo returns schema', async () => {
      const factory = new PrismaBunFactory(pg.connectionUri, { schema: 'myschema' })
      const adapter = await factory.connect()

      const info = (adapter as PrismaBunAdapter).getConnectionInfo()
      expect(info.schemaName).toBe('myschema')
      expect(info.supportsRelationJoins).toBe(true)

      await adapter.dispose()
    })

    test('getConnectionInfo defaults to public', async () => {
      const factory = new PrismaBunFactory(pg.connectionUri)
      const adapter = await factory.connect()

      const info = (adapter as PrismaBunAdapter).getConnectionInfo()
      expect(info.schemaName).toBe('public')

      await adapter.dispose()
    })
  })

  describe('connectToShadowDb()', () => {
    test('creates and cleans up shadow database', async () => {
      const factory = new PrismaBunFactory(pg.connectionUri)
      const shadow = await factory.connectToShadowDb()

      // Shadow adapter should work
      await shadow.executeScript(`
        CREATE TABLE _shadow_test (id serial PRIMARY KEY, name text);
        INSERT INTO _shadow_test (name) VALUES ('test');
      `)

      const result = await shadow.queryRaw({
        sql: 'SELECT name FROM _shadow_test',
        args: [],
        argTypes: [],
      })
      expect(result.rows[0]![0]).toBe('test')

      // Dispose should drop the shadow DB
      await shadow.dispose()

      // Verify shadow DB was dropped by listing databases
      const client = new SQL(pg.connectionUri)
      const dbs = await client.unsafe(
        "SELECT datname FROM pg_database WHERE datname LIKE 'prisma_migrate_shadow_db_%'"
      )
      expect(dbs.length).toBe(0)
      await client.close()
    }, TIMEOUT)

    test('shadow db is isolated from main db', async () => {
      const factory = new PrismaBunFactory(pg.connectionUri)

      // Create a table in main
      const main = await factory.connect()
      await main.executeScript('CREATE TABLE IF NOT EXISTS _main_only (id int)')

      // Shadow should NOT have this table
      const shadow = await factory.connectToShadowDb()
      try {
        await shadow.queryRaw({
          sql: 'SELECT * FROM _main_only',
          args: [],
          argTypes: [],
        })
        expect(true).toBe(false) // should not reach
      } catch (e: any) {
        expect(e.cause?.kind).toBe('TableDoesNotExist')
      }

      await shadow.dispose()
      await main.executeScript('DROP TABLE IF EXISTS _main_only')
      await main.dispose()
    }, TIMEOUT)
  })
})
