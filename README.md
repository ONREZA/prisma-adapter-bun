# @onreza/prisma-adapter-bun

Prisma 7+ driver adapter for [Bun.sql](https://bun.com/docs/api/sql) — use Bun's built-in PostgreSQL client instead of `pg`.

- Zero external database dependencies — uses Bun's native PostgreSQL bindings
- Full Prisma compatibility — CRUD, relations, transactions, migrations, raw queries, TypedSQL
- Type-safe — ships as TypeScript source, ready to use with Bun

## Installation

```bash
bun add @onreza/prisma-adapter-bun @prisma/driver-adapter-utils
```

## Quick Start

### 1. Prisma Schema

```prisma
// prisma/schema.prisma
datasource db {
  provider = "postgresql"
}

generator client {
  provider = "prisma-client"
  output   = "./generated"
}

model User {
  id    Int    @id @default(autoincrement())
  email String @unique
  name  String?
}
```

### 2. Prisma Config (for migrations)

```ts
// prisma.config.ts
import { defineConfig } from "prisma/config";

export default defineConfig({
  schema: "prisma/schema.prisma",
  datasource: {
    url: process.env.DATABASE_URL!,
  },
});
```

### 3. Generate & Migrate

```bash
bunx prisma generate
DATABASE_URL="postgres://user:pass@localhost:5432/mydb" bunx prisma db push
```

### 4. Use

```ts
import { PrismaClient } from "./prisma/generated/client";
import { PrismaBun } from "@onreza/prisma-adapter-bun";

const adapter = new PrismaBun("postgres://user:pass@localhost:5432/mydb");
const prisma = new PrismaClient({ adapter });

const users = await prisma.user.findMany();
console.log(users);
```

## Configuration

### Connection String

```ts
const adapter = new PrismaBun("postgres://user:pass@localhost:5432/mydb");
```

### Connection Options

```ts
import { SQL } from "bun";

const adapter = new PrismaBun({
  hostname: "localhost",
  port: 5432,
  database: "mydb",
  username: "user",
  password: "pass",
  tls: true,
} satisfies SQL.PostgresOrMySQLOptions);
```

### Existing Bun.sql Client

```ts
import { SQL } from "bun";

const client = new SQL("postgres://user:pass@localhost:5432/mydb");
const adapter = new PrismaBun(client);
```

### Custom Bun.sql Options

Use `validateConnectionUrl` to pass custom Bun-specific options while preserving the `schema` option:

```ts
import { SQL } from "bun";
import { PrismaBun, validateConnectionUrl } from "@onreza/prisma-adapter-bun";

const { url, schema } = validateConnectionUrl(process.env.DATABASE_URL!);

const client = new SQL({
  url,
  idleTimeout: 30,
  maxLifetime: 3600,
  connectionTimeout: 10,
});

const adapter = new PrismaBun(client, { schema });
```

### Schema Option

```ts
const adapter = new PrismaBun("postgres://...", { schema: "my_schema" });
```

## API

### `PrismaBun` (factory)

The main export. Implements Prisma's `SqlMigrationAwareDriverAdapterFactory`.

```ts
new PrismaBun(config: string | URL | SQL.PostgresOrMySQLOptions, options?: PrismaBunOptions)
new PrismaBun(client: SQL, options?: PrismaBunOptions)
```

**Options:**

| Option   | Type     | Default    | Description             |
|----------|----------|------------|-------------------------|
| `schema` | `string` | `"public"` | PostgreSQL search path  |

**Methods:**

| Method              | Description                                         |
|---------------------|-----------------------------------------------------|
| `connect()`         | Creates a new connection and returns an adapter      |
| `connectToShadowDb()` | Creates a temporary database for Prisma Migrate   |

### `PrismaBunAdapter`

Low-level adapter for direct use (without factory). Implements `SqlDriverAdapter`.

```ts
import { SQL } from "bun";
import { PrismaBunAdapter } from "@onreza/prisma-adapter-bun";

const client = new SQL("postgres://...");
const adapter = new PrismaBunAdapter(client);
```

## Supported Features

| Feature                     | Status |
|-----------------------------|--------|
| CRUD (create, read, update, delete) | Supported |
| Relations (1:1, 1:N, M:N)  | Supported |
| Interactive transactions    | Supported |
| Batch transactions          | Supported |
| Isolation levels            | Supported |
| `$queryRaw` / `$executeRaw`| Supported |
| TypedSQL                    | Supported |
| Prisma Migrate              | Supported |
| Shadow database             | Supported |
| All PostgreSQL scalar types | Supported |
| Array types (`String[]`, etc.) | Supported |
| Json / Bytes / Decimal / BigInt | Supported |
| Enums                       | Supported |
| Connection pooling          | Built-in (Bun.sql) |

## Requirements

- **Bun** >= 1.2.0 (for `Bun.sql` support)
- **PostgreSQL** >= 12
- **Prisma** >= 7.0.0

## Development

```bash
# Install dependencies
bun install

# Run unit tests (no database needed)
bun test tests/conversion.test.ts tests/errors.test.ts

# Run integration tests (needs PostgreSQL)
DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres" bun test tests/adapter.test.ts

# Run e2e tests (needs Docker)
bun test --timeout 120000 tests/factory.test.ts tests/prisma.test.ts

# Run all tests
DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres" bun test --timeout 120000

# Type check
bunx tsc --noEmit
```

## How It Works

This adapter bridges Prisma's driver adapter protocol with Bun's built-in `Bun.sql` PostgreSQL client:

1. **Queries** — Prisma generates parameterized SQL (`$1`, `$2`, ...). The adapter passes them to `Bun.sql.unsafe(sql, values)`, which binds parameters at the PostgreSQL wire protocol level.

2. **Type Mapping** — PostgreSQL column types are mapped to Prisma's `ColumnType` enum. Since Bun.sql doesn't yet expose column OID metadata, types are inferred from JavaScript values. When Bun adds `.columns` support, the adapter will automatically use native OIDs.

3. **Transactions** — Uses `Bun.sql.reserve()` to get a dedicated connection, then sends `BEGIN`/`COMMIT`/`ROLLBACK` as Prisma expects.

4. **Migrations** — The factory's `connectToShadowDb()` creates a temporary database for Prisma Migrate, and cleans it up on dispose.

## License

MIT
