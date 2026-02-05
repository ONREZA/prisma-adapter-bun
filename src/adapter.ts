import {
  type ColumnType,
  type ConnectionInfo,
  DriverAdapterError,
  type IsolationLevel,
  type SqlDriverAdapter,
  type SqlQuery,
  type SqlQueryable,
  type SqlResultSet,
  type Transaction,
  type TransactionOptions,
} from "@prisma/driver-adapter-utils";
import type { SQL as BunSQL, ReservedSQL } from "bun";
import {
  fieldToColumnType,
  findFirstNonNullInColumn,
  inferOidFromValue,
  mapArg,
  resultNormalizers,
  UnsupportedNativeDataType,
} from "./conversion.ts";
import { convertDriverError } from "./errors.ts";
import type { ColumnMetadata, PrismaBunOptions } from "./types.ts";

const ADAPTER_NAME = "@onreza/prisma-adapter-bun";

const VALID_ISOLATION_LEVELS = new Set(["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]);

type QueryResult = {
  columns: ColumnMetadata[];
  rows: unknown[][];
  rowCount: number;
};

class BunQueryable implements SqlQueryable {
  readonly provider = "postgres" as const;
  readonly adapterName = ADAPTER_NAME;

  protected readonly client: BunSQL | ReservedSQL;
  protected readonly bunOptions?: PrismaBunOptions;

  constructor(client: BunSQL | ReservedSQL, bunOptions?: PrismaBunOptions) {
    this.client = client;
    this.bunOptions = bunOptions;
  }

  protected async performIO(query: SqlQuery): Promise<QueryResult> {
    const { sql, args, argTypes } = query;
    const values = args.map((arg, i) => {
      const argType = argTypes[i];
      if (!argType) return arg;
      return mapArg(arg, argType);
    });

    try {
      // Execute query in regular (object) mode
      const result = await this.client.unsafe(sql, values);
      const resultRecord = result as unknown as Record<string, unknown>;
      const rowCount: number = (resultRecord.count as number) ?? result.length;

      // Primary path: use .columns metadata if Bun.sql exposes it
      const rawColumns = resultRecord.columns;
      if (Array.isArray(rawColumns) && rawColumns.length > 0) {
        const columns: ColumnMetadata[] = rawColumns.map((c: Record<string, unknown>) => ({
          name: String(c.name),
          type: Number(c.type),
        }));
        // Convert object rows to array rows using column order from metadata
        const objectRows = result as unknown as Record<string, unknown>[];
        const rows: unknown[][] = objectRows.map((row) => columns.map((col) => row[col.name]));
        return { columns, rowCount, rows };
      }

      // Fallback: extract column names from object keys, infer types from values
      if (result.length === 0) {
        return { columns: [], rowCount, rows: [] };
      }

      const firstRow = result[0] as Record<string, unknown>;
      const columnNames = Object.keys(firstRow);

      // Convert object rows to array rows
      const objectRows = result as unknown as Record<string, unknown>[];
      const rows: unknown[][] = objectRows.map((row) => columnNames.map((name) => row[name]));

      // Infer OID for each column from first non-null value
      const columns: ColumnMetadata[] = columnNames.map((name, i) => ({
        name,
        type: inferOidFromValue(findFirstNonNullInColumn(rows, i)),
      }));

      return { columns, rowCount, rows };
    } catch (e) {
      throw new DriverAdapterError(convertDriverError(e));
    }
  }

  async queryRaw(query: SqlQuery): Promise<SqlResultSet> {
    const { columns, rows } = await this.performIO(query);

    const columnNames = columns.map((col) => col.name);
    const columnTypes = this.mapColumnTypes(columns);
    this.normalizeRows(columns, rows);

    return { columnNames, columnTypes, rows };
  }

  private mapColumnTypes(columns: ColumnMetadata[]): ColumnType[] {
    try {
      return columns.map((col) => fieldToColumnType(col.type));
    } catch (e) {
      if (e instanceof UnsupportedNativeDataType) {
        throw new DriverAdapterError({
          kind: "UnsupportedNativeDataType",
          type: e.type,
        });
      }
      throw e;
    }
  }

  private normalizeRows(columns: ColumnMetadata[], rows: unknown[][]): void {
    for (let i = 0; i < columns.length; i++) {
      const col = columns[i];
      if (!col) continue;
      const normalizer = resultNormalizers[col.type];
      if (normalizer) {
        for (const row of rows) {
          if (row[i] !== null && row[i] !== undefined) {
            row[i] = normalizer(row[i]);
          }
        }
      }
    }
  }

  async executeRaw(query: SqlQuery): Promise<number> {
    const { rowCount } = await this.performIO(query);
    return rowCount;
  }
}

export class BunTransaction extends BunQueryable implements Transaction {
  readonly txOptions: TransactionOptions = { usePhantomQuery: false };
  private released = false;

  protected override readonly client: ReservedSQL;

  constructor(client: ReservedSQL, bunOptions?: PrismaBunOptions) {
    super(client, bunOptions);
    this.client = client;
  }

  get options(): TransactionOptions {
    return this.txOptions;
  }

  private release(): void {
    if (!this.released) {
      this.released = true;
      this.client.release();
    }
  }

  commit(): Promise<void> {
    // Prisma Engine sends COMMIT via executeRaw before calling commit().
    // We only release the reserved connection back to the pool.
    this.release();
    return Promise.resolve();
  }

  rollback(): Promise<void> {
    // Prisma Engine sends ROLLBACK via executeRaw before calling rollback().
    // We only release the reserved connection.
    this.release();
    return Promise.resolve();
  }
}

export class PrismaBunAdapter extends BunQueryable implements SqlDriverAdapter {
  private readonly disposeCallback?: () => Promise<void>;

  protected override readonly client: BunSQL;

  constructor(client: BunSQL, bunOptions?: PrismaBunOptions, disposeCallback?: () => Promise<void>) {
    super(client, bunOptions);
    this.client = client;
    this.disposeCallback = disposeCallback;
  }

  async startTransaction(isolationLevel?: IsolationLevel): Promise<Transaction> {
    if (isolationLevel && !VALID_ISOLATION_LEVELS.has(isolationLevel.toUpperCase())) {
      throw new DriverAdapterError({
        code: "INVALID_ISOLATION_LEVEL",
        column: undefined,
        detail: undefined,
        hint: undefined,
        kind: "postgres",
        message: `Invalid isolation level: ${isolationLevel}`,
        severity: "ERROR",
      });
    }

    const reserved = await this.client.reserve();

    try {
      const tx = new BunTransaction(reserved, this.bunOptions);

      await tx.executeRaw({ args: [], argTypes: [], sql: "BEGIN" });

      if (isolationLevel) {
        await tx.executeRaw({
          args: [],
          argTypes: [],
          sql: `SET TRANSACTION ISOLATION LEVEL ${isolationLevel.toUpperCase()}`,
        });
      }

      return tx;
    } catch (error) {
      try {
        await reserved.unsafe("ROLLBACK");
      } catch {
        // Ignore rollback errors during cleanup
      }
      reserved.release();
      throw error;
    }
  }

  async executeScript(script: string): Promise<void> {
    try {
      await this.client.unsafe(script).simple();
    } catch (e) {
      throw new DriverAdapterError(convertDriverError(e));
    }
  }

  getConnectionInfo(): ConnectionInfo {
    return {
      maxBindValues: 65535,
      schemaName: this.bunOptions?.schema ?? "public",
      supportsRelationJoins: true,
    };
  }

  async dispose(): Promise<void> {
    try {
      if (this.disposeCallback) {
        await this.disposeCallback();
      } else {
        await this.client.close();
      }
    } catch (e) {
      throw new DriverAdapterError(convertDriverError(e));
    }
  }
}
