# @onreza/prisma-adapter-bun

Prisma 7+ driver adapter for Bun.sql (built-in PostgreSQL client).

## Runtime

Use Bun (>=1.2.0), not Node.js.

- `bun install`, `bun test`, `bun run <script>`, `bunx <pkg>`
- Bun auto-loads `.env` — don't use dotenv
- `Bun.sql` for PostgreSQL — don't use `pg` or `postgres.js`

## Architecture

```
src/
  index.ts        — Public API: PrismaBun (factory alias), PrismaBunAdapter, types
  adapter.ts      — BunQueryable -> PrismaBunAdapter (SqlDriverAdapter), BunTransaction
  factory.ts      — PrismaBunFactory (SqlMigrationAwareDriverAdapterFactory), shadow DB
  conversion.ts   — PostgreSQL OID <-> Prisma ColumnType, value normalizers, mapArg
  errors.ts       — SQL.PostgresError -> DriverAdapterError mapping
  types.ts        — PrismaBunOptions, BunSqlConfig, ColumnMetadata
```

## Key Technical Details

### Bun.sql specifics
- **No `.columns` metadata** on query results — types inferred from JS values (`inferOidFromValue`)
- **`instanceof SQL` doesn't work** — use duck-typing (`isSqlClient()` in factory.ts)
- **`sql.unsafe()` doesn't accept JS arrays** — convert to PG array literal `{a,b,c}` via `toPgArrayLiteral()`
- **Error codes**: `SQL.PostgresError.errno` = PG code (`42P01`), `.code` = Bun internal code
- **DML row count**: use `result.count`, not `result.length` (which is 0 for INSERT/UPDATE/DELETE)

### Prisma 7
- No `url` in datasource block — configured in `prisma.config.ts`
- PrismaClient requires `adapter` (SqlDriverAdapterFactory)
- Generated client: `prisma/generated/client.ts` (gitignored, run `bunx prisma generate`)

## Commands

```sh
bun run check             # biome check + tsc --noEmit
bun run lint:fix           # biome auto-fix
bun run format             # biome format
bun run test               # unit tests (no DB needed)
bun run test:integration   # adapter tests (needs DATABASE_URL)
bun run test:e2e           # factory + prisma tests (needs Docker)
bun run test:all           # all 139 tests (needs DATABASE_URL + Docker)
```

## Testing

- Unit tests (`conversion.test.ts`, `errors.test.ts`): no external dependencies
- Integration (`adapter.test.ts`): requires `DATABASE_URL` pointing to PostgreSQL
- E2E (`factory.test.ts`, `prisma.test.ts`): start Docker containers via `tests/helpers/pg-container.ts`
- `metadata-probe.test.ts`: diagnostic — detects when Bun adds native column metadata support
- Before running typecheck, generate Prisma client: `bunx prisma generate`

## Code Style

- **Biome** for linting and formatting (not ESLint/Prettier)
- **Lefthook** git hooks: pre-commit (biome + typecheck), pre-push (+ tests), commit-msg (commitlint)
- **Conventional Commits** enforced. Types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore`, `style`, `revert`. Scopes: `deps`, `ci`, `docs`, `release`
- No `any` — use `unknown` + type narrowing
- No constructor parameter properties (`erasableSyntaxOnly` compatibility)
- Top-level regex constants (biome: `useTopLevelRegex`)
- No barrel files except `src/index.ts`

## Release

- **onreza-release** for versioning, CHANGELOG, GitHub Releases
- Config: `.onrezarelease.jsonc`
- `skipHooks: true` bypasses lefthook during release commits
- CI workflow: `.github/workflows/release.yml` (manual trigger)
- Publishing: `npm publish --provenance` with OIDC trusted publishing (no NPM_TOKEN needed)
