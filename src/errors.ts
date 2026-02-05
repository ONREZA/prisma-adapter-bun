import type { Error as AdapterError } from "@prisma/driver-adapter-utils";
import { SQL } from "bun";

const RE_CONSTRAINT_KEY = /Key \((.+?)\)=/;

function extractConstraintFields(detail?: string): { fields: string[] } | undefined {
  if (!detail) return;
  // PostgreSQL detail format: "Key (field1, field2)=(value1, value2) already exists."
  const match = detail.match(RE_CONSTRAINT_KEY);
  if (match?.[1]) {
    return { fields: match[1].split(",").map((s) => s.trim()) };
  }
  return;
}

/**
 * Get the PostgreSQL error code from a Bun SQL.PostgresError.
 * Bun.sql stores the PG error code in `errno` (e.g., "42P01"),
 * while `code` contains Bun's internal error code (e.g., "ERR_POSTGRES_SERVER_ERROR").
 */
function getPgErrorCode(error: SQL.PostgresError): string {
  return error.errno ?? error.code;
}

function mapPostgresError(error: SQL.PostgresError): AdapterError {
  const pgCode = getPgErrorCode(error);
  const base = {
    originalCode: pgCode,
    originalMessage: error.message,
  };

  switch (pgCode) {
    // Value too long for type
    case "22001":
      return { ...base, column: error.column, kind: "LengthMismatch" };

    // Numeric value out of range
    case "22003":
      return { ...base, cause: error.message, kind: "ValueOutOfRange" };

    // Invalid text representation / invalid input syntax
    case "22P02":
      return { ...base, kind: "InvalidInputValue", message: error.message };

    // Unique violation
    case "23505": {
      const constraint =
        extractConstraintFields(error.detail) ?? (error.constraint ? { index: error.constraint } : undefined);
      return { ...base, constraint, kind: "UniqueConstraintViolation" };
    }

    // Not null violation
    case "23502": {
      const constraint = error.column ? { fields: [error.column] } : undefined;
      return { ...base, constraint, kind: "NullConstraintViolation" };
    }

    // Foreign key violation
    case "23503": {
      const constraint =
        extractConstraintFields(error.detail) ?? (error.constraint ? { index: error.constraint } : undefined);
      return { ...base, constraint, kind: "ForeignKeyConstraintViolation" };
    }

    // Database does not exist
    case "3D000":
      return { ...base, db: error.message, kind: "DatabaseDoesNotExist" };

    // Database already exists
    case "42P04":
      return { ...base, db: error.message, kind: "DatabaseAlreadyExists" };

    // Insufficient privilege / authorization
    case "28000":
      return { ...base, kind: "DatabaseAccessDenied" };

    // Password authentication failed
    case "28P01":
      return { ...base, kind: "AuthenticationFailed" };

    // Serialization failure (transaction conflict)
    case "40001":
      return { ...base, kind: "TransactionWriteConflict" };

    // Undefined table
    case "42P01":
      return { ...base, kind: "TableDoesNotExist", table: error.table };

    // Undefined column
    case "42703":
      return { ...base, column: error.column, kind: "ColumnNotFound" };

    // Too many connections
    case "53300":
      return { ...base, cause: error.message, kind: "TooManyConnections" };

    // Default: pass through as raw PostgreSQL error
    default:
      return {
        ...base,
        code: pgCode,
        column: error.column,
        detail: error.detail,
        hint: error.hint,
        kind: "postgres",
        message: error.message,
        severity: error.severity ?? "ERROR",
      };
  }
}

function mapConnectionError(error: Error & { code?: string }): AdapterError {
  const code = error.code ?? "";
  const base = { originalCode: code, originalMessage: error.message };

  if (code.includes("CONNECTION_CLOSED") || code === "ECONNRESET") {
    return { ...base, kind: "ConnectionClosed" };
  }

  if (
    code.includes("CONNECTION_TIMEOUT") ||
    code === "ETIMEDOUT" ||
    code.includes("IDLE_TIMEOUT") ||
    code.includes("LIFETIME_TIMEOUT")
  ) {
    return { ...base, kind: "SocketTimeout" };
  }

  if (code.includes("TLS") || code.includes("SSL")) {
    return { ...base, kind: "TlsConnectionError", reason: error.message };
  }

  if (code.includes("AUTHENTICATION_FAILED")) {
    return { ...base, kind: "AuthenticationFailed" };
  }

  if (code === "ENOTFOUND" || code === "ECONNREFUSED") {
    return { ...base, kind: "DatabaseNotReachable" };
  }

  return {
    ...base,
    code,
    column: undefined,
    detail: undefined,
    hint: undefined,
    kind: "postgres",
    message: error.message,
    severity: "ERROR",
  };
}

export function convertDriverError(error: unknown): AdapterError {
  if (error instanceof SQL.PostgresError) {
    return mapPostgresError(error);
  }

  if (error instanceof SQL.SQLError) {
    return mapConnectionError(error as Error & { code?: string });
  }

  if (error instanceof Error) {
    return mapConnectionError(error as Error & { code?: string });
  }

  return {
    code: "UNKNOWN",
    column: undefined,
    detail: undefined,
    hint: undefined,
    kind: "postgres",
    message: String(error),
    severity: "ERROR",
  };
}
