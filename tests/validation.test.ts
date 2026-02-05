import { afterEach, describe, expect, mock, test } from "bun:test";
import { validateConnectionUrl } from "../src/validation.ts";

const warnMock = mock();

afterEach(() => {
  warnMock.mockClear();
});

const originalWarn = console.warn;

// Replace console.warn for all tests
console.warn = (...args: unknown[]) => {
  warnMock(...args);
};

// Restore on process exit
process.on("exit", () => {
  console.warn = originalWarn;
});

describe("validateConnectionUrl", () => {
  describe("clean URLs", () => {
    test("passes URL without query parameters unchanged", () => {
      const result = validateConnectionUrl("postgresql://user:pass@localhost:5432/mydb");
      expect(result.url).toBe("postgresql://user:pass@localhost:5432/mydb");
      expect(result.schema).toBeUndefined();
      expect(warnMock).not.toHaveBeenCalled();
    });

    test("accepts URL object", () => {
      const url = new URL("postgresql://user:pass@localhost:5432/mydb");
      const result = validateConnectionUrl(url);
      expect(result.url).toBe("postgresql://user:pass@localhost:5432/mydb");
      expect(result.schema).toBeUndefined();
    });
  });

  describe("valid libpq parameters", () => {
    test("preserves sslmode", () => {
      const result = validateConnectionUrl("postgresql://localhost/db?sslmode=require");
      expect(result.url).toBe("postgresql://localhost/db?sslmode=require");
      expect(warnMock).not.toHaveBeenCalled();
    });

    test("preserves connect_timeout", () => {
      const result = validateConnectionUrl("postgresql://localhost/db?connect_timeout=10");
      expect(result.url).toBe("postgresql://localhost/db?connect_timeout=10");
      expect(warnMock).not.toHaveBeenCalled();
    });

    test("preserves application_name", () => {
      const result = validateConnectionUrl("postgresql://localhost/db?application_name=myapp");
      expect(result.url).toBe("postgresql://localhost/db?application_name=myapp");
      expect(warnMock).not.toHaveBeenCalled();
    });

    test("preserves multiple valid parameters", () => {
      const result = validateConnectionUrl(
        "postgresql://localhost/db?sslmode=require&connect_timeout=10&application_name=myapp",
      );
      expect(result.url).toBe("postgresql://localhost/db?sslmode=require&connect_timeout=10&application_name=myapp");
      expect(warnMock).not.toHaveBeenCalled();
    });
  });

  describe("schema extraction", () => {
    test("extracts schema parameter", () => {
      const result = validateConnectionUrl("postgresql://localhost/db?schema=custom");
      expect(result.schema).toBe("custom");
      expect(result.url).toBe("postgresql://localhost/db");
      expect(warnMock).not.toHaveBeenCalled();
    });

    test("treats empty schema as undefined", () => {
      const result = validateConnectionUrl("postgresql://localhost/db?schema=");
      expect(result.schema).toBeUndefined();
      expect(result.url).toBe("postgresql://localhost/db");
    });

    test("extracts schema alongside valid parameters", () => {
      const result = validateConnectionUrl("postgresql://localhost/db?schema=myschema&sslmode=require");
      expect(result.schema).toBe("myschema");
      expect(result.url).toBe("postgresql://localhost/db?sslmode=require");
      expect(warnMock).not.toHaveBeenCalled();
    });
  });

  describe("invalid parameter removal", () => {
    test("removes non-standard parameter with warning", () => {
      const result = validateConnectionUrl("postgresql://localhost/db?foo=bar");
      expect(result.url).toBe("postgresql://localhost/db");
      expect(warnMock).toHaveBeenCalledTimes(1);
      const firstCall = warnMock.mock.calls[0] as unknown[];
      expect(firstCall[0]).toContain('"foo"');
      expect(firstCall[0]).toContain("@onreza/prisma-adapter-bun");
    });

    test("removes multiple non-standard parameters", () => {
      const result = validateConnectionUrl("postgresql://localhost/db?foo=bar&baz=qux");
      expect(result.url).toBe("postgresql://localhost/db");
      expect(warnMock).toHaveBeenCalledTimes(2);
    });
  });

  describe("mixed parameters", () => {
    test("handles valid + invalid + schema together", () => {
      const result = validateConnectionUrl(
        "postgresql://localhost/db?sslmode=require&schema=custom&foo=bar&connect_timeout=10",
      );
      expect(result.schema).toBe("custom");
      expect(result.url).toBe("postgresql://localhost/db?sslmode=require&connect_timeout=10");
      expect(warnMock).toHaveBeenCalledTimes(1);
      const firstCall = warnMock.mock.calls[0] as unknown[];
      expect(firstCall[0]).toContain('"foo"');
    });
  });

  describe("edge cases", () => {
    test("preserves credentials in URL", () => {
      const result = validateConnectionUrl("postgresql://admin:s3cret@host:5432/db?schema=test");
      expect(result.url).toBe("postgresql://admin:s3cret@host:5432/db");
      expect(result.schema).toBe("test");
    });

    test("handles special characters in parameter values", () => {
      const result = validateConnectionUrl("postgresql://localhost/db?application_name=my%20app&schema=my%20schema");
      expect(result.schema).toBe("my schema");
      expect(result.url).toContain("application_name=my+app");
    });

    test("handles URL with port and path", () => {
      const result = validateConnectionUrl("postgresql://user:pass@db.example.com:6543/production?sslmode=verify-full");
      expect(result.url).toBe("postgresql://user:pass@db.example.com:6543/production?sslmode=verify-full");
      expect(result.schema).toBeUndefined();
    });
  });
});
