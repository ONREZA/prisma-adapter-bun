import { SQL } from 'bun'
import type {
  SqlDriverAdapter,
  SqlMigrationAwareDriverAdapterFactory,
} from '@prisma/driver-adapter-utils'
import { PrismaBunAdapter } from './adapter.ts'
import type { BunSqlConfig, PrismaBunOptions } from './types.ts'

const ADAPTER_NAME = '@onreza/prisma-adapter-bun'

/**
 * Detect a Bun SQL client instance via duck-typing.
 * `instanceof SQL` doesn't work because SQL.prototype is undefined in Bun.
 */
function isSqlClient(value: unknown): value is SQL {
  return (
    typeof value === 'function' &&
    typeof (value as any).unsafe === 'function' &&
    typeof (value as any).reserve === 'function' &&
    typeof (value as any).close === 'function'
  )
}

export class PrismaBunFactory implements SqlMigrationAwareDriverAdapterFactory {
  readonly provider = 'postgres' as const
  readonly adapterName = ADAPTER_NAME

  private readonly config: BunSqlConfig
  private readonly bunOptions?: PrismaBunOptions
  private externalClient: SQL | null = null

  constructor(config: BunSqlConfig, options?: PrismaBunOptions)
  constructor(client: SQL, options?: PrismaBunOptions)
  constructor(configOrClient: BunSqlConfig | SQL, options?: PrismaBunOptions) {
    this.bunOptions = options
    if (isSqlClient(configOrClient)) {
      this.externalClient = configOrClient
      this.config = {}
    } else {
      this.config = configOrClient
    }
  }

  async connect(): Promise<SqlDriverAdapter> {
    if (this.externalClient) {
      return new PrismaBunAdapter(this.externalClient, this.bunOptions, async () => {
        // Don't close external client by default
      })
    }

    const client = new SQL(this.config as any)

    return new PrismaBunAdapter(client, this.bunOptions, async () => {
      await client.close()
    })
  }

  async connectToShadowDb(): Promise<SqlDriverAdapter> {
    const mainAdapter = await this.connect()
    const database = `prisma_migrate_shadow_db_${crypto.randomUUID()}`

    await mainAdapter.executeScript(`CREATE DATABASE "${database}"`)

    let shadowClient: SQL
    try {
      const shadowConfig = this.buildShadowConfig(database)
      shadowClient = new SQL(shadowConfig as any)
    } catch (error) {
      try {
        await mainAdapter.executeScript(`DROP DATABASE IF EXISTS "${database}"`)
      } finally {
        await mainAdapter.dispose()
      }
      throw error
    }

    return new PrismaBunAdapter(shadowClient, this.bunOptions, async () => {
      await shadowClient.close()
      try {
        await mainAdapter.executeScript(`DROP DATABASE IF EXISTS "${database}"`)
      } finally {
        await mainAdapter.dispose()
      }
    })
  }

  private buildShadowConfig(database: string): SQL.PostgresOrMySQLOptions {
    if (typeof this.config === 'string' || this.config instanceof URL) {
      const url = new URL(this.config.toString())
      url.pathname = `/${database}`
      return { url: url.toString() }
    }

    if (typeof this.config === 'object' && this.config !== null) {
      const opts = this.config as SQL.PostgresOrMySQLOptions
      if (opts.url) {
        const url = new URL(opts.url.toString())
        url.pathname = `/${database}`
        return { ...opts, url: url.toString() }
      }
      return { ...opts, database }
    }

    return { database }
  }
}
