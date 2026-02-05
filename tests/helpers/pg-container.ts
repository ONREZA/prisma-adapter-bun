import { SQL } from 'bun'

const IMAGE = 'postgres:16-alpine'
const PG_USER = 'test'
const PG_PASSWORD = 'test'
const PG_DB = 'test'

export interface PgContainer {
  connectionUri: string
  host: string
  port: number
  stop(): Promise<void>
}

let counter = 0

export async function startPostgres(): Promise<PgContainer> {
  const name = `prisma-adapter-test-${process.pid}-${++counter}`

  // Start container with random port
  const run = Bun.spawnSync([
    'docker', 'run', '-d',
    '--name', name,
    '-p', '0:5432',
    '-e', `POSTGRES_USER=${PG_USER}`,
    '-e', `POSTGRES_PASSWORD=${PG_PASSWORD}`,
    '-e', `POSTGRES_DB=${PG_DB}`,
    IMAGE,
  ])
  if (run.exitCode !== 0) {
    throw new Error(`Failed to start container: ${run.stderr.toString()}`)
  }

  // Get the mapped port
  const portOut = Bun.spawnSync(['docker', 'port', name, '5432'])
  const portLine = portOut.stdout.toString().trim().split('\n')[0]!
  const port = parseInt(portLine.split(':').pop()!, 10)
  const host = '127.0.0.1'

  const connectionUri = `postgres://${PG_USER}:${PG_PASSWORD}@${host}:${port}/${PG_DB}`

  // Wait for PostgreSQL to be ready (up to 30s)
  const deadline = Date.now() + 30_000
  while (Date.now() < deadline) {
    try {
      const client = new SQL(connectionUri)
      await client.unsafe('SELECT 1')
      await client.close()
      break
    } catch {
      await Bun.sleep(200)
    }
  }

  // Final check
  const client = new SQL(connectionUri)
  await client.unsafe('SELECT 1')
  await client.close()

  return {
    connectionUri,
    host,
    port,
    async stop() {
      Bun.spawnSync(['docker', 'rm', '-f', name])
    },
  }
}
