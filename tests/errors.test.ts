import { describe, expect, test } from "bun:test";
import { SQL } from "bun";
import { convertDriverError } from "../src/errors.ts";

/**
 * In Bun.sql, PostgresError has:
 * - `code`: Bun's internal error code (e.g., "ERR_POSTGRES_SERVER_ERROR")
 * - `errno`: PostgreSQL error code (e.g., "42P01")
 * Our adapter uses `errno` for PG error code matching.
 */
function makePgError(
  pgCode: string,
  opts: Partial<ConstructorParameters<typeof SQL.PostgresError>[1]> = {},
): SQL.PostgresError {
  return new SQL.PostgresError(`Test error: ${pgCode}`, {
    code: "ERR_POSTGRES_SERVER_ERROR",
    errno: pgCode,
    severity: "ERROR",
    ...opts,
  });
}

describe("convertDriverError", () => {
  describe("PostgreSQL error codes", () => {
    test("22001 -> LengthMismatch", () => {
      const err = makePgError("22001", { column: "name" });
      const result = convertDriverError(err);
      expect(result.kind).toBe("LengthMismatch");
      if (result.kind === "LengthMismatch") {
        expect(result.column).toBe("name");
      }
    });

    test("22003 -> ValueOutOfRange", () => {
      const err = makePgError("22003");
      const result = convertDriverError(err);
      expect(result.kind).toBe("ValueOutOfRange");
    });

    test("22P02 -> InvalidInputValue", () => {
      const err = makePgError("22P02");
      const result = convertDriverError(err);
      expect(result.kind).toBe("InvalidInputValue");
    });

    test("23505 -> UniqueConstraintViolation", () => {
      const err = makePgError("23505", {
        constraint: "users_email_key",
        detail: "Key (email)=(test@test.com) already exists.",
      });
      const result = convertDriverError(err);
      expect(result.kind).toBe("UniqueConstraintViolation");
      if (result.kind === "UniqueConstraintViolation") {
        expect(result.constraint).toEqual({ fields: ["email"] });
      }
    });

    test("23505 with no detail falls back to constraint name", () => {
      const err = makePgError("23505", { constraint: "users_email_key" });
      const result = convertDriverError(err);
      expect(result.kind).toBe("UniqueConstraintViolation");
      if (result.kind === "UniqueConstraintViolation") {
        expect(result.constraint).toEqual({ index: "users_email_key" });
      }
    });

    test("23502 -> NullConstraintViolation", () => {
      const err = makePgError("23502", { column: "name" });
      const result = convertDriverError(err);
      expect(result.kind).toBe("NullConstraintViolation");
      if (result.kind === "NullConstraintViolation") {
        expect(result.constraint).toEqual({ fields: ["name"] });
      }
    });

    test("23503 -> ForeignKeyConstraintViolation", () => {
      const err = makePgError("23503", {
        constraint: "posts_user_id_fkey",
        detail: 'Key (user_id)=(999) is not present in table "users".',
      });
      const result = convertDriverError(err);
      expect(result.kind).toBe("ForeignKeyConstraintViolation");
      if (result.kind === "ForeignKeyConstraintViolation") {
        expect(result.constraint).toEqual({ fields: ["user_id"] });
      }
    });

    test("3D000 -> DatabaseDoesNotExist", () => {
      const err = makePgError("3D000");
      const result = convertDriverError(err);
      expect(result.kind).toBe("DatabaseDoesNotExist");
    });

    test("42P04 -> DatabaseAlreadyExists", () => {
      const err = makePgError("42P04");
      const result = convertDriverError(err);
      expect(result.kind).toBe("DatabaseAlreadyExists");
    });

    test("28000 -> DatabaseAccessDenied", () => {
      const err = makePgError("28000");
      const result = convertDriverError(err);
      expect(result.kind).toBe("DatabaseAccessDenied");
    });

    test("28P01 -> AuthenticationFailed", () => {
      const err = makePgError("28P01");
      const result = convertDriverError(err);
      expect(result.kind).toBe("AuthenticationFailed");
    });

    test("40001 -> TransactionWriteConflict", () => {
      const err = makePgError("40001");
      const result = convertDriverError(err);
      expect(result.kind).toBe("TransactionWriteConflict");
    });

    test("42P01 -> TableDoesNotExist", () => {
      const err = makePgError("42P01", { table: "nonexistent" });
      const result = convertDriverError(err);
      expect(result.kind).toBe("TableDoesNotExist");
      if (result.kind === "TableDoesNotExist") {
        expect(result.table).toBe("nonexistent");
      }
    });

    test("42703 -> ColumnNotFound", () => {
      const err = makePgError("42703", { column: "missing_col" });
      const result = convertDriverError(err);
      expect(result.kind).toBe("ColumnNotFound");
      if (result.kind === "ColumnNotFound") {
        expect(result.column).toBe("missing_col");
      }
    });

    test("53300 -> TooManyConnections", () => {
      const err = makePgError("53300");
      const result = convertDriverError(err);
      expect(result.kind).toBe("TooManyConnections");
    });

    test("unknown code -> postgres passthrough", () => {
      const err = makePgError("99999", { hint: "some hint", severity: "FATAL" });
      const result = convertDriverError(err);
      expect(result.kind).toBe("postgres");
      if (result.kind === "postgres") {
        expect(result.code).toBe("99999");
        expect(result.severity).toBe("FATAL");
        expect(result.hint).toBe("some hint");
      }
    });
  });

  describe("connection errors", () => {
    test("ECONNREFUSED -> DatabaseNotReachable", () => {
      const err = Object.assign(new Error("Connection refused"), { code: "ECONNREFUSED" });
      const result = convertDriverError(err);
      expect(result.kind).toBe("DatabaseNotReachable");
    });

    test("ENOTFOUND -> DatabaseNotReachable", () => {
      const err = Object.assign(new Error("Host not found"), { code: "ENOTFOUND" });
      const result = convertDriverError(err);
      expect(result.kind).toBe("DatabaseNotReachable");
    });

    test("ETIMEDOUT -> SocketTimeout", () => {
      const err = Object.assign(new Error("Connection timed out"), { code: "ETIMEDOUT" });
      const result = convertDriverError(err);
      expect(result.kind).toBe("SocketTimeout");
    });

    test("CONNECTION_CLOSED code -> ConnectionClosed", () => {
      const err = Object.assign(new Error("Connection closed"), { code: "ERR_POSTGRES_CONNECTION_CLOSED" });
      const result = convertDriverError(err);
      expect(result.kind).toBe("ConnectionClosed");
    });

    test("TLS error -> TlsConnectionError", () => {
      const err = Object.assign(new Error("TLS handshake failed"), { code: "ERR_POSTGRES_TLS_NOT_AVAILABLE" });
      const result = convertDriverError(err);
      expect(result.kind).toBe("TlsConnectionError");
    });
  });

  describe("non-error values", () => {
    test("string error -> postgres passthrough", () => {
      const result = convertDriverError("some string error");
      expect(result.kind).toBe("postgres");
      if (result.kind === "postgres") {
        expect(result.message).toBe("some string error");
      }
    });
  });
});
