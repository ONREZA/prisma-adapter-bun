import type { SQL } from 'bun'

export type PrismaBunOptions = {
  schema?: string
}

export type BunSqlConfig = string | URL | SQL.PostgresOrMySQLOptions

export type ColumnMetadata = {
  name: string
  type: number
}
