import type { SQL as BunSQL, ReservedSQL } from 'bun'
import {
  DriverAdapterError,
  type ColumnType,
  type ConnectionInfo,
  type IsolationLevel,
  type SqlDriverAdapter,
  type SqlQuery,
  type SqlQueryable,
  type SqlResultSet,
  type Transaction,
  type TransactionOptions,
} from '@prisma/driver-adapter-utils'
import {
  fieldToColumnType,
  findFirstNonNullInColumn,
  inferOidFromValue,
  mapArg,
  resultNormalizers,
  UnsupportedNativeDataType,
} from './conversion.ts'
import { convertDriverError } from './errors.ts'
import type { ColumnMetadata, PrismaBunOptions } from './types.ts'

const ADAPTER_NAME = '@onreza/prisma-adapter-bun'

const VALID_ISOLATION_LEVELS = new Set([
  'READ UNCOMMITTED',
  'READ COMMITTED',
  'REPEATABLE READ',
  'SERIALIZABLE',
])

type QueryResult = {
  columns: ColumnMetadata[]
  rows: unknown[][]
  rowCount: number
}

class BunQueryable implements SqlQueryable {
  readonly provider = 'postgres' as const
  readonly adapterName = ADAPTER_NAME

  constructor(
    protected readonly client: BunSQL | ReservedSQL,
    protected readonly bunOptions?: PrismaBunOptions,
  ) {}

  protected async performIO(query: SqlQuery): Promise<QueryResult> {
    const { sql, args, argTypes } = query
    const values = args.map((arg, i) => {
      const argType = argTypes[i]
      if (!argType) return arg
      return mapArg(arg, argType)
    })

    try {
      // Execute query in regular (object) mode
      const result = await this.client.unsafe(sql, values)
      const resultAny = result as any
      const rowCount: number = resultAny.count ?? result.length

      // Primary path: use .columns metadata if Bun.sql exposes it
      if (resultAny.columns && Array.isArray(resultAny.columns) && resultAny.columns.length > 0) {
        const columns: ColumnMetadata[] = resultAny.columns.map((c: any) => ({
          name: String(c.name),
          type: Number(c.type),
        }))
        // Convert object rows to array rows using column order from metadata
        const rows: unknown[][] = (result as any[]).map((row) =>
          columns.map((col) => row[col.name]),
        )
        return { columns, rows, rowCount }
      }

      // Fallback: extract column names from object keys, infer types from values
      if (result.length === 0) {
        return { columns: [], rows: [], rowCount }
      }

      const firstRow = result[0] as Record<string, unknown>
      const columnNames = Object.keys(firstRow)

      // Convert object rows to array rows
      const rows: unknown[][] = (result as any[]).map((row) =>
        columnNames.map((name) => row[name]),
      )

      // Infer OID for each column from first non-null value
      const columns: ColumnMetadata[] = columnNames.map((name, i) => ({
        name,
        type: inferOidFromValue(findFirstNonNullInColumn(rows, i)),
      }))

      return { columns, rows, rowCount }
    } catch (e) {
      throw new DriverAdapterError(convertDriverError(e))
    }
  }

  async queryRaw(query: SqlQuery): Promise<SqlResultSet> {
    const { columns, rows } = await this.performIO(query)

    const columnNames = columns.map((col) => col.name)

    let columnTypes: ColumnType[]
    try {
      columnTypes = columns.map((col) => fieldToColumnType(col.type))
    } catch (e) {
      if (e instanceof UnsupportedNativeDataType) {
        throw new DriverAdapterError({
          kind: 'UnsupportedNativeDataType',
          type: e.type,
        })
      }
      throw e
    }

    // Post-process values using normalizers (JSON -> string, Date -> ISO, etc.)
    for (let i = 0; i < columns.length; i++) {
      const col = columns[i]!
      const normalizer = resultNormalizers[col.type]
      if (normalizer) {
        for (let j = 0; j < rows.length; j++) {
          const row = rows[j]!
          if (row[i] !== null && row[i] !== undefined) {
            row[i] = normalizer(row[i])
          }
        }
      }
    }

    return { columnNames, columnTypes, rows }
  }

  async executeRaw(query: SqlQuery): Promise<number> {
    const { rowCount } = await this.performIO(query)
    return rowCount
  }
}

export class BunTransaction extends BunQueryable implements Transaction {
  readonly txOptions: TransactionOptions = { usePhantomQuery: false }
  private released = false

  constructor(
    protected override readonly client: ReservedSQL,
    bunOptions?: PrismaBunOptions,
  ) {
    super(client, bunOptions)
  }

  get options(): TransactionOptions {
    return this.txOptions
  }

  private release(): void {
    if (!this.released) {
      this.released = true
      this.client.release()
    }
  }

  async commit(): Promise<void> {
    // Prisma Engine sends COMMIT via executeRaw before calling commit().
    // We only release the reserved connection back to the pool.
    this.release()
  }

  async rollback(): Promise<void> {
    // Prisma Engine sends ROLLBACK via executeRaw before calling rollback().
    // We only release the reserved connection.
    this.release()
  }
}

export class PrismaBunAdapter extends BunQueryable implements SqlDriverAdapter {
  private readonly disposeCallback?: () => Promise<void>

  constructor(
    protected override readonly client: BunSQL,
    bunOptions?: PrismaBunOptions,
    disposeCallback?: () => Promise<void>,
  ) {
    super(client, bunOptions)
    this.disposeCallback = disposeCallback
  }

  async startTransaction(isolationLevel?: IsolationLevel): Promise<Transaction> {
    if (isolationLevel && !VALID_ISOLATION_LEVELS.has(isolationLevel.toUpperCase())) {
      throw new DriverAdapterError({
        kind: 'postgres',
        code: 'INVALID_ISOLATION_LEVEL',
        severity: 'ERROR',
        message: `Invalid isolation level: ${isolationLevel}`,
        detail: undefined,
        column: undefined,
        hint: undefined,
      })
    }

    const reserved = await this.client.reserve()

    try {
      const tx = new BunTransaction(reserved, this.bunOptions)

      await tx.executeRaw({ sql: 'BEGIN', args: [], argTypes: [] })

      if (isolationLevel) {
        await tx.executeRaw({
          sql: `SET TRANSACTION ISOLATION LEVEL ${isolationLevel.toUpperCase()}`,
          args: [],
          argTypes: [],
        })
      }

      return tx
    } catch (error) {
      try {
        await reserved.unsafe('ROLLBACK')
      } catch {
        // Ignore rollback errors during cleanup
      }
      reserved.release()
      throw error
    }
  }

  async executeScript(script: string): Promise<void> {
    try {
      await this.client.unsafe(script).simple()
    } catch (e) {
      throw new DriverAdapterError(convertDriverError(e))
    }
  }

  getConnectionInfo(): ConnectionInfo {
    return {
      schemaName: this.bunOptions?.schema ?? 'public',
      maxBindValues: 65535,
      supportsRelationJoins: true,
    }
  }

  async dispose(): Promise<void> {
    try {
      if (this.disposeCallback) {
        await this.disposeCallback()
      } else {
        await this.client.close()
      }
    } catch (e) {
      throw new DriverAdapterError(convertDriverError(e))
    }
  }
}
