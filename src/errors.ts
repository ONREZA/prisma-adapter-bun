import { DriverAdapterError, type Error as AdapterError } from '@prisma/driver-adapter-utils'
import { SQL } from 'bun'

function extractConstraintFields(detail?: string): { fields: string[] } | undefined {
  if (!detail) return undefined
  // PostgreSQL detail format: "Key (field1, field2)=(value1, value2) already exists."
  const match = detail.match(/Key \((.+?)\)=/)
  if (match?.[1]) {
    return { fields: match[1].split(',').map((s) => s.trim()) }
  }
  return undefined
}

/**
 * Get the PostgreSQL error code from a Bun SQL.PostgresError.
 * Bun.sql stores the PG error code in `errno` (e.g., "42P01"),
 * while `code` contains Bun's internal error code (e.g., "ERR_POSTGRES_SERVER_ERROR").
 */
function getPgErrorCode(error: SQL.PostgresError): string {
  return error.errno ?? error.code
}

function mapPostgresError(error: SQL.PostgresError): AdapterError {
  const pgCode = getPgErrorCode(error)
  const base = {
    originalCode: pgCode,
    originalMessage: error.message,
  }

  switch (pgCode) {
    // Value too long for type
    case '22001':
      return { ...base, kind: 'LengthMismatch', column: error.column }

    // Numeric value out of range
    case '22003':
      return { ...base, kind: 'ValueOutOfRange', cause: error.message }

    // Invalid text representation / invalid input syntax
    case '22P02':
      return { ...base, kind: 'InvalidInputValue', message: error.message }

    // Unique violation
    case '23505': {
      const constraint = extractConstraintFields(error.detail)
        ?? (error.constraint ? { index: error.constraint } : undefined)
      return { ...base, kind: 'UniqueConstraintViolation', constraint }
    }

    // Not null violation
    case '23502': {
      const constraint = error.column ? { fields: [error.column] } : undefined
      return { ...base, kind: 'NullConstraintViolation', constraint }
    }

    // Foreign key violation
    case '23503': {
      const constraint = extractConstraintFields(error.detail)
        ?? (error.constraint ? { index: error.constraint } : undefined)
      return { ...base, kind: 'ForeignKeyConstraintViolation', constraint }
    }

    // Database does not exist
    case '3D000':
      return { ...base, kind: 'DatabaseDoesNotExist', db: error.message }

    // Database already exists
    case '42P04':
      return { ...base, kind: 'DatabaseAlreadyExists', db: error.message }

    // Insufficient privilege / authorization
    case '28000':
      return { ...base, kind: 'DatabaseAccessDenied' }

    // Password authentication failed
    case '28P01':
      return { ...base, kind: 'AuthenticationFailed' }

    // Serialization failure (transaction conflict)
    case '40001':
      return { ...base, kind: 'TransactionWriteConflict' }

    // Undefined table
    case '42P01':
      return { ...base, kind: 'TableDoesNotExist', table: error.table }

    // Undefined column
    case '42703':
      return { ...base, kind: 'ColumnNotFound', column: error.column }

    // Too many connections
    case '53300':
      return { ...base, kind: 'TooManyConnections', cause: error.message }

    // Default: pass through as raw PostgreSQL error
    default:
      return {
        ...base,
        kind: 'postgres',
        code: pgCode,
        severity: error.severity ?? 'ERROR',
        message: error.message,
        detail: error.detail,
        column: error.column,
        hint: error.hint,
      }
  }
}

function mapConnectionError(error: Error & { code?: string }): AdapterError {
  const code = error.code ?? ''
  const base = { originalCode: code, originalMessage: error.message }

  if (code.includes('CONNECTION_CLOSED') || code === 'ECONNRESET') {
    return { ...base, kind: 'ConnectionClosed' }
  }

  if (code.includes('CONNECTION_TIMEOUT') || code === 'ETIMEDOUT' || code.includes('IDLE_TIMEOUT') || code.includes('LIFETIME_TIMEOUT')) {
    return { ...base, kind: 'SocketTimeout' }
  }

  if (code.includes('TLS') || code.includes('SSL')) {
    return { ...base, kind: 'TlsConnectionError', reason: error.message }
  }

  if (code.includes('AUTHENTICATION_FAILED')) {
    return { ...base, kind: 'AuthenticationFailed' }
  }

  if (code === 'ENOTFOUND' || code === 'ECONNREFUSED') {
    return { ...base, kind: 'DatabaseNotReachable' }
  }

  return {
    ...base,
    kind: 'postgres',
    code,
    severity: 'ERROR',
    message: error.message,
    detail: undefined,
    column: undefined,
    hint: undefined,
  }
}

export function convertDriverError(error: unknown): AdapterError {
  if (error instanceof SQL.PostgresError) {
    return mapPostgresError(error)
  }

  if (error instanceof SQL.SQLError) {
    return mapConnectionError(error as Error & { code?: string })
  }

  if (error instanceof Error) {
    return mapConnectionError(error as Error & { code?: string })
  }

  return {
    kind: 'postgres',
    code: 'UNKNOWN',
    severity: 'ERROR',
    message: String(error),
    detail: undefined,
    column: undefined,
    hint: undefined,
  }
}
