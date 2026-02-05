# Contributing

Thanks for your interest in contributing!

## Development Setup

```bash
# Clone the repo
git clone https://github.com/ONREZA/prisma-adapter-bun.git
cd prisma-adapter-bun

# Install dependencies (requires Bun >= 1.2.0)
bun install

# Generate Prisma client (needed for typecheck)
bunx prisma generate
```

## Commands

```bash
bun run check          # biome lint + typecheck
bun run lint:fix       # auto-fix lint issues
bun run format         # format with biome
bun run test           # unit tests (no DB required)
bun run test:integration  # adapter tests (requires DATABASE_URL)
bun run test:e2e       # factory + prisma tests (requires Docker)
bun run test:all       # all tests
```

## Workflow

1. Fork the repo and create a feature branch from `main`
2. Make your changes
3. Ensure `bun run check` and `bun run test` pass
4. Commit using [Conventional Commits](https://www.conventionalcommits.org/) format:
   - `feat: add new feature`
   - `fix: resolve issue`
   - `docs: update readme`
   - `test: add tests`
   - `refactor: improve code`
5. Open a Pull Request

## Commit Convention

This project uses [Conventional Commits](https://www.conventionalcommits.org/). Commits are validated by commitlint via lefthook pre-commit hook.

Allowed types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore`, `style`, `revert`

## Running Integration Tests

Integration and e2e tests require PostgreSQL. You can use Docker:

```bash
docker run -d --name pg-test -p 5432:5432 \
  -e POSTGRES_PASSWORD=test \
  postgres:16-alpine

DATABASE_URL="postgres://postgres:test@localhost:5432/postgres" bun run test:integration
```

E2e tests manage Docker containers automatically.

## Code Style

- Formatting and linting handled by [Biome](https://biomejs.dev/)
- No manual formatting needed — lefthook runs biome on pre-commit
