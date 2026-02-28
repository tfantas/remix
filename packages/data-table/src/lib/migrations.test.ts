import * as assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { rawSql } from './sql.ts'
import type {
  DataManipulationRequest,
  DataMigrationRequest,
  DataMigrationResult,
  DataMigrationOperation,
  DataManipulationResult,
  DatabaseAdapter,
  TransactionToken,
} from './adapter.ts'
import { column, createMigration, createMigrationRunner, parseMigrationFilename } from './migrations.ts'
import type { SqlStatement } from './sql.ts'

type JournalRow = {
  id: string
  name: string
  checksum: string
  batch: number
  applied_at: string
}

class MemoryMigrationAdapter implements DatabaseAdapter {
  dialect = 'memory'
  capabilities = {
    returning: true,
    savepoints: true,
    upsert: true,
    transactionalDdl: true,
    migrationLock: true,
  }
  journalTableCreated = false
  journalTableName = 'data_table_migrations'
  journalRows: JournalRow[] = []
  migratedOperations: DataMigrationOperation[] = []
  executedRawSql: SqlStatement[] = []
  lockAcquireCount = 0
  lockReleaseCount = 0
  beginTransactionCount = 0
  commitTransactionCount = 0
  rollbackTransactionCount = 0
  failOnMigrateKind: DataMigrationOperation['kind'] | undefined
  #transactionCounter = 0
  #tokens = new Set<string>()

  compileSql(operation: DataMigrationOperation | DataManipulationRequest['operation']): SqlStatement[] {
    return [{ text: operation.kind, values: [] }]
  }

  async execute(request: DataManipulationRequest): Promise<DataManipulationResult> {
    if (request.operation.kind !== 'raw') {
      throw new Error('MemoryMigrationAdapter only supports raw execute operations')
    }

    let statement = request.operation.sql
    let text = statement.text.toLowerCase()

    if (text.startsWith('select 1 from ')) {
      if (!this.journalTableCreated) {
        throw new Error('Journal table does not exist')
      }

      return { rows: [] }
    }

    if (text.includes('select id, name, checksum, batch, applied_at from ')) {
      if (!this.journalTableCreated) {
        throw new Error('Journal table does not exist')
      }

      return {
        rows: this.journalRows.map((row) => ({
          id: row.id,
          name: row.name,
          checksum: row.checksum,
          batch: row.batch,
          applied_at: row.applied_at,
        })),
      }
    }

    if (text.startsWith('insert into ')) {
      let [id, name, checksum, batch, appliedAt] = statement.values

      this.journalRows.push({
        id: String(id),
        name: String(name),
        checksum: String(checksum),
        batch: Number(batch),
        applied_at: String(appliedAt),
      })

      return { affectedRows: 1 }
    }

    if (text.startsWith('delete from ')) {
      let [id] = statement.values
      this.journalRows = this.journalRows.filter((row) => row.id !== String(id))

      return { affectedRows: 1 }
    }

    this.executedRawSql.push(statement)
    return { affectedRows: 0 }
  }

  async migrate(request: DataMigrationRequest): Promise<DataMigrationResult> {
    let operation = request.operation

    if (
      operation.kind === 'createTable' &&
      operation.table.name === this.journalTableName &&
      operation.table.schema === undefined
    ) {
      this.journalTableCreated = true
      return { affectedObjects: 1 }
    }

    if (this.failOnMigrateKind && operation.kind === this.failOnMigrateKind) {
      throw new Error('Forced migrate failure for kind ' + operation.kind)
    }

    this.migratedOperations.push(operation)
    return { affectedObjects: 1 }
  }

  async beginTransaction(): Promise<TransactionToken> {
    this.beginTransactionCount += 1
    this.#transactionCounter += 1
    let token = { id: 'tx_' + String(this.#transactionCounter) }
    this.#tokens.add(token.id)
    return token
  }

  async commitTransaction(token: TransactionToken): Promise<void> {
    this.#assertToken(token)
    this.commitTransactionCount += 1
    this.#tokens.delete(token.id)
  }

  async rollbackTransaction(token: TransactionToken): Promise<void> {
    this.#assertToken(token)
    this.rollbackTransactionCount += 1
    this.#tokens.delete(token.id)
  }

  async createSavepoint(token: TransactionToken): Promise<void> {
    this.#assertToken(token)
  }

  async rollbackToSavepoint(token: TransactionToken): Promise<void> {
    this.#assertToken(token)
  }

  async releaseSavepoint(token: TransactionToken): Promise<void> {
    this.#assertToken(token)
  }

  async acquireMigrationLock(): Promise<void> {
    this.lockAcquireCount += 1
  }

  async releaseMigrationLock(): Promise<void> {
    this.lockReleaseCount += 1
  }

  #assertToken(token: TransactionToken): void {
    if (!this.#tokens.has(token.id)) {
      throw new Error('Unknown transaction token: ' + token.id)
    }
  }
}

describe('migration column builder', () => {
  it('builds canonical column specs with chainable methods', () => {
    let columnSpec = column
      .varchar(255)
      .notNull()
      .default('hello')
      .unique('users_email_unique')
      .references('auth.users', ['id'], { name: 'users_auth_fk' })
      .onDelete('cascade')
      .onUpdate('restrict')
      .check('length(email) > 3', 'users_email_len')
      .comment('Primary email')
      .computed('lower(email)', { stored: false })
      .collate('en_US')
      .charset('utf8mb4')
      .build()

    assert.deepEqual(columnSpec, {
      type: 'varchar',
      length: 255,
      nullable: false,
      default: { kind: 'literal', value: 'hello' },
      unique: { name: 'users_email_unique' },
      references: {
        table: { schema: 'auth', name: 'users' },
        columns: ['id'],
        name: 'users_auth_fk',
        onDelete: 'cascade',
        onUpdate: 'restrict',
      },
      checks: [{ expression: 'length(email) > 3', name: 'users_email_len' }],
      comment: 'Primary email',
      computed: { expression: 'lower(email)', stored: false },
      collate: 'en_US',
      charset: 'utf8mb4',
    })
  })

  it('throws when onDelete is called before references', () => {
    assert.throws(() => column.integer().onDelete('cascade'), /requires references\(\) to be set first/)
  })
})

describe('migration runner', () => {
  it('builds deterministic schema plans from migration APIs', async () => {
    let adapter = new MemoryMigrationAdapter()
    let migration = createMigration({
      async up({ schema }) {
        await schema.createTable('app.users', (table) => {
          table.addColumn('id', column.integer().primaryKey())
          table.addColumn('email', column.text().notNull())
          table.addIndex('users_email_idx', 'email', { unique: true })
          table.comment('Users table')
        })

        await schema.alterTable('app.users', (table) => {
          table.addColumn('status', column.text().default('active'))
          table.addCheck("status in ('active', 'disabled')", { name: 'users_status_check' })
          table.addIndex('users_status_idx', 'status')
        })

        await schema.renameIndex('app.users', 'users_status_idx', 'users_status_idx_v2')
        await schema.raw('vacuum')
      },
      async down() {},
    })

    let runner = createMigrationRunner(adapter, [{ id: '20260101000000', name: 'users', migration }])

    await runner.up()

    assert.deepEqual(
      adapter.migratedOperations.map((operation) => operation.kind),
      ['createTable', 'createIndex', 'alterTable', 'createIndex', 'renameIndex', 'raw'],
    )

    let createTableOperation = adapter.migratedOperations[0]
    assert.equal(createTableOperation.kind, 'createTable')
    assert.deepEqual(createTableOperation.table, { schema: 'app', name: 'users' })

    let createIndexOperation = adapter.migratedOperations[1]
    assert.equal(createIndexOperation.kind, 'createIndex')
    assert.deepEqual(createIndexOperation.index.columns, ['email'])

    let alterIndexOperation = adapter.migratedOperations[3]
    assert.equal(alterIndexOperation.kind, 'createIndex')
    assert.deepEqual(alterIndexOperation.index.columns, ['status'])

    let rawOperation = adapter.migratedOperations[5]
    assert.equal(rawOperation.kind, 'raw')
    assert.deepEqual(rawOperation.sql, rawSql('vacuum'))
  })

  it('applies, reverts by step, and reverts by target', async () => {
    let adapter = new MemoryMigrationAdapter()
    let migrations = [
      {
        id: '20260101000000',
        name: 'users',
        migration: createMigration({
          async up({ schema }) {
            await schema.createTable('users', (table) => {
              table.addColumn('id', column.integer().primaryKey())
            })
          },
          async down({ schema }) {
            await schema.dropTable('users')
          },
        }),
      },
      {
        id: '20260102000000',
        name: 'posts',
        migration: createMigration({
          async up({ schema }) {
            await schema.createTable('posts', (table) => {
              table.addColumn('id', column.integer().primaryKey())
            })
          },
          async down({ schema }) {
            await schema.dropTable('posts')
          },
        }),
      },
    ]

    let runner = createMigrationRunner(adapter, migrations)

    await runner.up()
    let statusAfterUp = await runner.status()
    assert.deepEqual(
      statusAfterUp.map((entry) => entry.status),
      ['applied', 'applied'],
    )

    await runner.down({ step: 1 })
    let statusAfterStepDown = await runner.status()
    assert.deepEqual(
      statusAfterStepDown.map((entry) => entry.status),
      ['applied', 'pending'],
    )

    await runner.down({ to: '20260101000000' })
    let statusAfterTargetDown = await runner.status()
    assert.deepEqual(
      statusAfterTargetDown.map((entry) => entry.status),
      ['pending', 'pending'],
    )
  })

  it('supports dryRun planning without executing migration DDL', async () => {
    let adapter = new MemoryMigrationAdapter()
    let migration = createMigration({
      async up({ schema }) {
        await schema.createTable('users', (table) => {
          table.addColumn('id', column.integer().primaryKey())
        })
      },
      async down() {},
    })

    let runner = createMigrationRunner(adapter, [{ id: '20260101000000', name: 'users', migration }])

    let result = await runner.up({ dryRun: true })

    assert.deepEqual(result.sql, [{ text: 'createTable', values: [] }])
    assert.equal(adapter.migratedOperations.length, 0)
    assert.equal(adapter.journalRows.length, 0)
  })

  it('detects checksum drift before applying more migrations', async () => {
    let adapter = new MemoryMigrationAdapter()
    let appliedMigration = createMigration({
      async up({ schema }) {
        await schema.createTable('users', (table) => {
          table.addColumn('id', column.integer().primaryKey())
        })
      },
      async down() {},
    })

    let runner = createMigrationRunner(adapter, [
      {
        id: '20260101000000',
        name: 'users',
        checksum: 'checksum_a',
        migration: appliedMigration,
      },
    ])

    await runner.up()

    let driftedRunner = createMigrationRunner(adapter, [
      {
        id: '20260101000000',
        name: 'users',
        checksum: 'checksum_b',
        migration: appliedMigration,
      },
    ])

    await assert.rejects(() => driftedRunner.up(), /checksum drift detected/)
  })

  it('balances migration lock hooks when migration execution fails', async () => {
    let adapter = new MemoryMigrationAdapter()
    adapter.failOnMigrateKind = 'createTable'

    let migration = createMigration({
      async up({ schema }) {
        await schema.createTable('users', (table) => {
          table.addColumn('id', column.integer().primaryKey())
        })
      },
      async down() {},
    })

    let runner = createMigrationRunner(adapter, [{ id: '20260101000000', name: 'users', migration }])

    await assert.rejects(() => runner.up(), /Forced migrate failure/)
    assert.equal(adapter.lockAcquireCount, 1)
    assert.equal(adapter.lockReleaseCount, 1)
  })

  it('throws when required transactions are requested on non-transactional adapters', async () => {
    let adapter = new MemoryMigrationAdapter()
    adapter.capabilities.transactionalDdl = false

    let migration = createMigration({
      transaction: 'required',
      async up({ schema }) {
        await schema.createTable('users', (table) => {
          table.addColumn('id', column.integer().primaryKey())
        })
      },
      async down() {},
    })

    let runner = createMigrationRunner(adapter, [{ id: '20260101000000', name: 'users', migration }])

    await assert.rejects(() => runner.up(), /requires transactional DDL/)
  })

  it('throws for unknown migration targets and invalid step values', async () => {
    let adapter = new MemoryMigrationAdapter()
    let migration = createMigration({
      async up({ schema }) {
        await schema.createTable('users', (table) => {
          table.addColumn('id', column.integer().primaryKey())
        })
      },
      async down() {},
    })

    let runner = createMigrationRunner(adapter, [{ id: '20260101000000', name: 'users', migration }])

    await assert.rejects(() => runner.up({ to: '99999999999999' }), /Unknown migration target/)
    await assert.rejects(() => runner.up({ step: 0 }), /positive integer/)
  })
})

describe('migration filename parsing', () => {
  it('parses migration ids and names from standard filenames', () => {
    let parsed = parseMigrationFilename('20260101010101_create_users_table.ts')

    assert.deepEqual(parsed, {
      id: '20260101010101',
      name: 'create_users_table',
    })
  })

  it('rejects invalid migration filenames', () => {
    assert.throws(
      () => parseMigrationFilename('create_users_table.ts'),
      /Expected format YYYYMMDDHHmmss_name\.ts/,
    )
  })
})
