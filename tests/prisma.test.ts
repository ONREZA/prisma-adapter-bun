import { test, expect, describe, beforeAll, afterAll } from 'bun:test'
import { PrismaClient } from '../prisma/generated/client.ts'
import { PrismaBun } from '../src/index.ts'
import { Decimal } from '@prisma/client/runtime/client'
import { startPostgres, type PgContainer } from './helpers/pg-container.ts'

const TIMEOUT = 60_000

let pg: PgContainer
let prisma: InstanceType<typeof PrismaClient>

beforeAll(async () => {
  pg = await startPostgres()

  // Push schema to the container database
  const proc = Bun.spawn(
    ['bunx', 'prisma', 'db', 'push', '--accept-data-loss'],
    { env: { ...process.env, DATABASE_URL: pg.connectionUri }, stdout: 'pipe', stderr: 'pipe' },
  )
  const exitCode = await proc.exited
  if (exitCode !== 0) {
    const stderr = await new Response(proc.stderr).text()
    throw new Error(`prisma db push failed (exit ${exitCode}): ${stderr}`)
  }

  const adapter = new PrismaBun(pg.connectionUri)
  prisma = new PrismaClient({ adapter })
}, TIMEOUT)

afterAll(async () => {
  await prisma?.$disconnect()
  await pg?.stop()
})

describe('Prisma e2e: CRUD', () => {
  test('create and findUnique', async () => {
    const user = await prisma.user.create({
      data: {
        email: 'alice@test.com',
        name: 'Alice',
        age: 30,
        score: 95.5,
        active: true,
        role: 'ADMIN',
        metadata: { level: 5, permissions: ['read', 'write'] },
        balance: new Decimal('1234.56'),
        data: Buffer.from('hello'),
        tags: ['typescript', 'prisma'],
      },
    })

    expect(user.id).toBeGreaterThan(0)
    expect(user.email).toBe('alice@test.com')
    expect(user.name).toBe('Alice')
    expect(user.age).toBe(30)
    expect(user.score).toBe(95.5)
    expect(user.active).toBe(true)
    expect(user.role).toBe('ADMIN')
    expect(user.tags).toEqual(['typescript', 'prisma'])
    expect(user.createdAt).toBeInstanceOf(Date)
    expect(user.updatedAt).toBeInstanceOf(Date)

    const found = await prisma.user.findUnique({ where: { email: 'alice@test.com' } })
    expect(found).not.toBeNull()
    expect(found!.name).toBe('Alice')
  })

  test('create with relations', async () => {
    const user = await prisma.user.create({
      data: {
        email: 'bob@test.com',
        name: 'Bob',
        posts: {
          create: [
            { title: 'First Post', content: 'Hello world', published: true, views: BigInt(100) },
            { title: 'Draft', content: null, published: false },
          ],
        },
        profile: {
          create: { bio: 'Software developer' },
        },
      },
      include: { posts: true, profile: true },
    })

    expect(user.posts.length).toBe(2)
    expect(user.posts.find((p) => p.title === 'First Post')?.published).toBe(true)
    expect(user.profile?.bio).toBe('Software developer')
  })

  test('findMany with filters and ordering', async () => {
    const users = await prisma.user.findMany({
      where: { active: true },
      orderBy: { email: 'asc' },
    })

    expect(users.length).toBeGreaterThanOrEqual(2)
    expect(users[0]!.email.localeCompare(users[1]!.email)).toBeLessThan(0)
  })

  test('findMany with relation includes', async () => {
    const users = await prisma.user.findMany({
      where: { email: 'bob@test.com' },
      include: { posts: { where: { published: true } }, profile: true },
    })

    expect(users.length).toBe(1)
    expect(users[0]!.posts.length).toBe(1)
    expect(users[0]!.posts[0]!.title).toBe('First Post')
  })

  test('update', async () => {
    const updated = await prisma.user.update({
      where: { email: 'alice@test.com' },
      data: { name: 'Alice Updated', age: 31, role: 'MODERATOR' },
    })

    expect(updated.name).toBe('Alice Updated')
    expect(updated.age).toBe(31)
    expect(updated.role).toBe('MODERATOR')
  })

  test('updateMany', async () => {
    const result = await prisma.user.updateMany({
      where: { active: true },
      data: { active: false },
    })

    expect(result.count).toBeGreaterThanOrEqual(2)
  })

  test('upsert: create', async () => {
    const user = await prisma.user.upsert({
      where: { email: 'charlie@test.com' },
      create: { email: 'charlie@test.com', name: 'Charlie', age: 25 },
      update: { name: 'Charlie Updated' },
    })

    expect(user.email).toBe('charlie@test.com')
    expect(user.name).toBe('Charlie')
  })

  test('upsert: update', async () => {
    const user = await prisma.user.upsert({
      where: { email: 'charlie@test.com' },
      create: { email: 'charlie@test.com', name: 'Charlie', age: 25 },
      update: { name: 'Charlie Updated' },
    })

    expect(user.name).toBe('Charlie Updated')
  })

  test('delete', async () => {
    await prisma.user.delete({ where: { email: 'charlie@test.com' } })

    const found = await prisma.user.findUnique({ where: { email: 'charlie@test.com' } })
    expect(found).toBeNull()
  })

  test('count and aggregate', async () => {
    const count = await prisma.user.count()
    expect(count).toBeGreaterThanOrEqual(2)

    const agg = await prisma.user.aggregate({
      _avg: { age: true },
      _max: { age: true },
      _min: { age: true },
    })
    expect(agg._avg.age).not.toBeNull()
  })
})

describe('Prisma e2e: types', () => {
  test('Decimal round-trip', async () => {
    const user = await prisma.user.findUnique({ where: { email: 'alice@test.com' } })
    expect(user!.balance).not.toBeNull()
    expect(user!.balance!.toString()).toBe('1234.56')
  })

  test('Json round-trip', async () => {
    const user = await prisma.user.findUnique({ where: { email: 'alice@test.com' } })
    expect(user!.metadata).toEqual({ level: 5, permissions: ['read', 'write'] })
  })

  test('Bytes round-trip', async () => {
    const user = await prisma.user.findUnique({ where: { email: 'alice@test.com' } })
    expect(user!.data).not.toBeNull()
    expect(Buffer.from(user!.data!).toString()).toBe('hello')
  })

  test('DateTime', async () => {
    const user = await prisma.user.findUnique({ where: { email: 'alice@test.com' } })
    expect(user!.createdAt).toBeInstanceOf(Date)
    expect(user!.createdAt.getTime()).toBeLessThanOrEqual(Date.now())
  })

  test('BigInt', async () => {
    const post = await prisma.post.findFirst({ where: { title: 'First Post' } })
    expect(post!.views).toBe(BigInt(100))
  })

  test('Enum', async () => {
    const user = await prisma.user.findUnique({ where: { email: 'alice@test.com' } })
    expect(['USER', 'ADMIN', 'MODERATOR']).toContain(user!.role)
  })

  test('String array', async () => {
    const user = await prisma.user.findUnique({ where: { email: 'alice@test.com' } })
    expect(user!.tags).toEqual(['typescript', 'prisma'])
  })

  test('nullable fields', async () => {
    const user = await prisma.user.create({
      data: { email: 'nullable@test.com' },
    })

    expect(user.name).toBeNull()
    expect(user.age).toBeNull()
    expect(user.score).toBeNull()
    expect(user.metadata).toBeNull()
    expect(user.balance).toBeNull()
    expect(user.data).toBeNull()
  })
})

describe('Prisma e2e: transactions', () => {
  test('interactive transaction: commit', async () => {
    const result = await prisma.$transaction(async (tx) => {
      const user = await tx.user.create({
        data: { email: 'tx-commit@test.com', name: 'TxCommit' },
      })
      await tx.post.create({
        data: { title: 'Tx Post', authorId: user.id, published: true },
      })
      return user
    })

    expect(result.email).toBe('tx-commit@test.com')

    const posts = await prisma.post.findMany({ where: { author: { email: 'tx-commit@test.com' } } })
    expect(posts.length).toBe(1)
  })

  test('interactive transaction: rollback on error', async () => {
    const emailBefore = 'tx-rollback@test.com'
    await prisma.user.create({ data: { email: emailBefore, name: 'TxRollback' } })

    try {
      await prisma.$transaction(async (tx) => {
        await tx.user.update({
          where: { email: emailBefore },
          data: { name: 'Should Not Persist' },
        })
        throw new Error('Intentional rollback')
      })
    } catch (e: any) {
      expect(e.message).toBe('Intentional rollback')
    }

    const user = await prisma.user.findUnique({ where: { email: emailBefore } })
    expect(user!.name).toBe('TxRollback')
  })

  test('sequential transaction (batch)', async () => {
    const [user, count] = await prisma.$transaction([
      prisma.user.create({ data: { email: 'batch@test.com', name: 'Batch' } }),
      prisma.user.count(),
    ])

    expect(user.email).toBe('batch@test.com')
    expect(count).toBeGreaterThanOrEqual(1)
  })
})

describe('Prisma e2e: errors', () => {
  test('unique constraint violation', async () => {
    await prisma.user.create({ data: { email: 'unique@test.com' } })

    try {
      await prisma.user.create({ data: { email: 'unique@test.com' } })
      expect(true).toBe(false) // should not reach
    } catch (e: any) {
      expect(e.code).toBe('P2002') // Unique constraint violation
    }
  })

  test('record not found on update', async () => {
    try {
      await prisma.user.update({
        where: { email: 'nonexistent@test.com' },
        data: { name: 'Nope' },
      })
      expect(true).toBe(false)
    } catch (e: any) {
      expect(e.code).toBe('P2025') // Record not found
    }
  })

  test('foreign key constraint', async () => {
    try {
      await prisma.post.create({
        data: { title: 'Orphan', authorId: 999999 },
      })
      expect(true).toBe(false)
    } catch (e: any) {
      expect(e.code).toBe('P2003') // Foreign key constraint
    }
  })
})

describe('Prisma e2e: advanced queries', () => {
  test('groupBy', async () => {
    const groups = await prisma.user.groupBy({
      by: ['role'],
      _count: { _all: true },
    })

    expect(groups.length).toBeGreaterThanOrEqual(1)
    for (const g of groups) {
      expect(g._count._all).toBeGreaterThanOrEqual(1)
    }
  })

  test('raw query via $queryRaw', async () => {
    const result = await prisma.$queryRaw<{ count: number | bigint }[]>`SELECT COUNT(*)::bigint as count FROM "User"`
    expect(result.length).toBe(1)
    // Prisma may return bigint or number depending on the adapter type inference
    expect(Number(result[0]!.count)).toBeGreaterThanOrEqual(1)
  })

  test('raw execute via $executeRaw', async () => {
    const affected = await prisma.$executeRaw`UPDATE "User" SET "active" = true WHERE "active" = false`
    expect(affected).toBeGreaterThanOrEqual(0)
  })

  test('pagination: skip + take', async () => {
    const page = await prisma.user.findMany({
      skip: 0,
      take: 2,
      orderBy: { id: 'asc' },
    })

    expect(page.length).toBeLessThanOrEqual(2)
  })

  test('select specific fields', async () => {
    const user = await prisma.user.findFirst({
      select: { email: true, name: true },
    })

    expect(user).toHaveProperty('email')
    expect(user).toHaveProperty('name')
    expect(user).not.toHaveProperty('age')
  })
})
