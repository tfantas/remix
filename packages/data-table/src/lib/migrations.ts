import { createDatabase } from './database.ts'
import type { Database } from './database.ts'
import type {
  AlterTableChange,
  CheckConstraint,
  ColumnDefinition,
  DataMigrationOperation,
  DatabaseAdapter,
  ForeignKeyAction,
  ForeignKeyConstraint,
  IndexDefinition,
  PrimaryKeyConstraint,
  TableRef,
  TransactionToken,
  UniqueConstraint,
} from './adapter.ts'
import { ColumnBuilder, column } from './column-builder.ts'
import { rawSql } from './sql.ts'

export { ColumnBuilder, column }
export type { ColumnNamespace } from './column-builder.ts'

export type MigrationTransactionMode = 'auto' | 'required' | 'none'

export type MigrationContext = {
  dialect: string
  schema: MigrationSchemaApi
  db: Database
}

export type CreateMigrationInput = {
  up: (context: MigrationContext) => Promise<void> | void
  down: (context: MigrationContext) => Promise<void> | void
  transaction?: MigrationTransactionMode
}

export type Migration = {
  up: CreateMigrationInput['up']
  down: CreateMigrationInput['down']
  transaction: MigrationTransactionMode
}

export function createMigration(input: CreateMigrationInput): Migration {
  return {
    up: input.up,
    down: input.down,
    transaction: input.transaction ?? 'auto',
  }
}

export type MigrationDescriptor = {
  id: string
  name: string
  path?: string
  checksum?: string
  migration: Migration
}

export type MigrationDirection = 'up' | 'down'

export type MigrationPlan = {
  migration: MigrationDescriptor
  direction: MigrationDirection
  transaction: MigrationTransactionMode
  statements: DataMigrationOperation[]
}

export type MigrationJournalRow = {
  id: string
  name: string
  checksum: string
  batch: number
  appliedAt: Date
}

export type MigrationStatus = 'applied' | 'pending' | 'drifted'

export type MigrationStatusEntry = {
  id: string
  name: string
  status: MigrationStatus
  appliedAt?: Date
  batch?: number
  checksum?: string
}

export type MigrateOptions = {
  to?: string
  step?: number
  dryRun?: boolean
}

export type MigrateResult = {
  applied: MigrationStatusEntry[]
  reverted: MigrationStatusEntry[]
  sql: Array<{ text: string; values: unknown[] }>
}

export type CreateTableOptions = { ifNotExists?: boolean }
export type AlterTableOptions = { ifExists?: boolean }
export type DropTableOptions = { ifExists?: boolean; cascade?: boolean }
export type IndexColumns = string | string[]

let migrationFilenamePattern = /^(\d{14})_(.+)\.(?:m?ts|m?js|cts|cjs)$/

function toTableRef(name: string): TableRef {
  let segments = name.split('.')

  if (segments.length === 1) {
    return { name }
  }

  return {
    schema: segments[0],
    name: segments.slice(1).join('.'),
  }
}

function normalizeIndexColumns(columns: IndexColumns): string[] {
  if (Array.isArray(columns)) {
    return [...columns]
  }

  return [columns]
}

export interface CreateTableBuilder {
  addColumn(name: string, definition: ColumnDefinition | ColumnBuilder): void
  addPrimaryKey(columns: string[], options?: { name?: string }): void
  addUnique(columns: string[], options?: { name?: string }): void
  addForeignKey(
    columns: string[],
    refTable: string,
    refColumns?: string[],
    options?: { name?: string; onDelete?: ForeignKeyAction; onUpdate?: ForeignKeyAction },
  ): void
  addCheck(name: string, expression: string): void
  addIndex(
    name: string,
    columns: IndexColumns,
    options?: Omit<IndexDefinition, 'table' | 'name' | 'columns'>,
  ): void
  comment(text: string): void
}

export interface AlterTableBuilder {
  addColumn(name: string, definition: ColumnDefinition | ColumnBuilder): void
  changeColumn(name: string, definition: ColumnDefinition | ColumnBuilder): void
  renameColumn(from: string, to: string): void
  dropColumn(name: string, options?: { ifExists?: boolean }): void
  addPrimaryKey(columns: string[], options?: { name?: string }): void
  dropPrimaryKey(name?: string): void
  addUnique(columns: string[], options?: { name?: string }): void
  dropUnique(name: string): void
  addForeignKey(
    columns: string[],
    refTable: string,
    refColumns?: string[],
    options?: { name?: string; onDelete?: ForeignKeyAction; onUpdate?: ForeignKeyAction },
  ): void
  dropForeignKey(name: string): void
  addCheck(name: string, expression: string): void
  dropCheck(name: string): void
  addIndex(
    name: string,
    columns: IndexColumns,
    options?: Omit<IndexDefinition, 'table' | 'name' | 'columns'>,
  ): void
  dropIndex(name: string): void
  comment(text: string): void
}

export interface MigrationSchemaApi {
  createTable(
    name: string,
    migrate: (table: CreateTableBuilder) => void,
    options?: CreateTableOptions,
  ): Promise<void>
  alterTable(
    name: string,
    migrate: (table: AlterTableBuilder) => void,
    options?: AlterTableOptions,
  ): Promise<void>
  renameTable(from: string, to: string): Promise<void>
  dropTable(name: string, options?: DropTableOptions): Promise<void>
  createIndex(
    table: string,
    columns: IndexColumns,
    options?: Omit<IndexDefinition, 'table' | 'columns'>,
  ): Promise<void>
  dropIndex(table: string, name: string, options?: { ifExists?: boolean }): Promise<void>
  renameIndex(table: string, from: string, to: string): Promise<void>
  addForeignKey(
    table: string,
    columns: string[],
    refTable: string,
    refColumns?: string[],
    options?: { name?: string; onDelete?: ForeignKeyAction; onUpdate?: ForeignKeyAction },
  ): Promise<void>
  dropForeignKey(table: string, name: string): Promise<void>
  addCheck(table: string, name: string, expression: string): Promise<void>
  dropCheck(table: string, name: string): Promise<void>
  raw(sql: string): Promise<void>
  tableExists(name: string): Promise<boolean>
  columnExists(table: string, column: string): Promise<boolean>
}

function asColumnDefinition(definition: ColumnDefinition | ColumnBuilder): ColumnDefinition {
  if (definition instanceof ColumnBuilder) {
    return definition.build()
  }

  return definition
}

class CreateTableBuilderRuntime implements CreateTableBuilder {
  columns: Record<string, ColumnDefinition> = {}
  primaryKey: PrimaryKeyConstraint | undefined
  uniques: UniqueConstraint[] = []
  checks: CheckConstraint[] = []
  foreignKeys: ForeignKeyConstraint[] = []
  indexes: Omit<IndexDefinition, 'table'>[] = []
  tableComment: string | undefined

  addColumn(name: string, definition: ColumnDefinition | ColumnBuilder): void {
    this.columns[name] = asColumnDefinition(definition)
  }

  addPrimaryKey(columns: string[], options?: { name?: string }): void {
    this.primaryKey = { columns: [...columns], name: options?.name }
  }

  addUnique(columns: string[], options?: { name?: string }): void {
    this.uniques.push({ columns: [...columns], name: options?.name })
  }

  addForeignKey(
    columns: string[],
    refTable: string,
    refColumns?: string[],
    options?: { name?: string; onDelete?: ForeignKeyAction; onUpdate?: ForeignKeyAction },
  ): void {
    this.foreignKeys.push({
      columns: [...columns],
      references: {
        table: toTableRef(refTable),
        columns: refColumns ? [...refColumns] : ['id'],
      },
      name: options?.name,
      onDelete: options?.onDelete,
      onUpdate: options?.onUpdate,
    })
  }

  addCheck(name: string, expression: string): void {
    this.checks.push({ expression, name })
  }

  addIndex(
    name: string,
    columns: IndexColumns,
    options?: Omit<IndexDefinition, 'table' | 'name' | 'columns'>,
  ): void {
    this.indexes.push({
      name,
      columns: normalizeIndexColumns(columns),
      ...options,
    })
  }

  comment(text: string): void {
    this.tableComment = text
  }
}

class AlterTableBuilderRuntime implements AlterTableBuilder {
  alterChanges: AlterTableChange[] = []
  extraStatements: DataMigrationOperation[] = []
  table: TableRef

  constructor(table: TableRef) {
    this.table = table
  }

  addColumn(name: string, definition: ColumnDefinition | ColumnBuilder): void {
    this.alterChanges.push({ kind: 'addColumn', column: name, definition: asColumnDefinition(definition) })
  }

  changeColumn(name: string, definition: ColumnDefinition | ColumnBuilder): void {
    this.alterChanges.push({
      kind: 'changeColumn',
      column: name,
      definition: asColumnDefinition(definition),
    })
  }

  renameColumn(from: string, to: string): void {
    this.alterChanges.push({ kind: 'renameColumn', from, to })
  }

  dropColumn(name: string, options?: { ifExists?: boolean }): void {
    this.alterChanges.push({ kind: 'dropColumn', column: name, ifExists: options?.ifExists })
  }

  addPrimaryKey(columns: string[], options?: { name?: string }): void {
    this.alterChanges.push({
      kind: 'addPrimaryKey',
      constraint: { columns: [...columns], name: options?.name },
    })
  }

  dropPrimaryKey(name?: string): void {
    this.alterChanges.push({ kind: 'dropPrimaryKey', name })
  }

  addUnique(columns: string[], options?: { name?: string }): void {
    this.alterChanges.push({
      kind: 'addUnique',
      constraint: { columns: [...columns], name: options?.name },
    })
  }

  dropUnique(name: string): void {
    this.alterChanges.push({ kind: 'dropUnique', name })
  }

  addForeignKey(
    columns: string[],
    refTable: string,
    refColumns?: string[],
    options?: { name?: string; onDelete?: ForeignKeyAction; onUpdate?: ForeignKeyAction },
  ): void {
    this.alterChanges.push({
      kind: 'addForeignKey',
      constraint: {
        columns: [...columns],
        references: {
          table: toTableRef(refTable),
          columns: refColumns ? [...refColumns] : ['id'],
        },
        name: options?.name,
        onDelete: options?.onDelete,
        onUpdate: options?.onUpdate,
      },
    })
  }

  dropForeignKey(name: string): void {
    this.alterChanges.push({ kind: 'dropForeignKey', name })
  }

  addCheck(name: string, expression: string): void {
    this.alterChanges.push({
      kind: 'addCheck',
      constraint: { expression, name },
    })
  }

  dropCheck(name: string): void {
    this.alterChanges.push({ kind: 'dropCheck', name })
  }

  addIndex(
    name: string,
    columns: IndexColumns,
    options?: Omit<IndexDefinition, 'table' | 'name' | 'columns'>,
  ): void {
    this.extraStatements.push({
      kind: 'createIndex',
      index: {
        table: this.table,
        name,
        columns: normalizeIndexColumns(columns),
        ...options,
      },
    })
  }

  dropIndex(name: string): void {
    this.extraStatements.push({
      kind: 'dropIndex',
      table: this.table,
      name,
    })
  }

  comment(text: string): void {
    this.alterChanges.push({ kind: 'setTableComment', comment: text })
  }
}

function createSchemaApi(
  db: Database,
  emit: (operation: DataMigrationOperation) => Promise<void>,
): MigrationSchemaApi {
  return {
    async createTable(name, migrate, options) {
      let builder = new CreateTableBuilderRuntime()
      migrate(builder)

      await emit({
        kind: 'createTable',
        table: toTableRef(name),
        ifNotExists: options?.ifNotExists,
        columns: builder.columns,
        primaryKey: builder.primaryKey,
        uniques: builder.uniques,
        checks: builder.checks,
        foreignKeys: builder.foreignKeys,
        comment: builder.tableComment,
      })

      for (let index of builder.indexes) {
        await emit({
          kind: 'createIndex',
          index: {
            table: toTableRef(name),
            ...index,
          },
        })
      }
    },
    async alterTable(name, migrate, options) {
      let table = toTableRef(name)
      let builder = new AlterTableBuilderRuntime(table)
      migrate(builder)

      if (builder.alterChanges.length > 0) {
        await emit({
          kind: 'alterTable',
          table,
          changes: builder.alterChanges,
          ifExists: options?.ifExists,
        })
      }

      for (let operation of builder.extraStatements) {
        await emit(operation)
      }
    },
    async renameTable(from, to) {
      await emit({ kind: 'renameTable', from: toTableRef(from), to: toTableRef(to) })
    },
    async dropTable(name, options) {
      await emit({
        kind: 'dropTable',
        table: toTableRef(name),
        ifExists: options?.ifExists,
        cascade: options?.cascade,
      })
    },
    async createIndex(table, columns, options) {
      await emit({
        kind: 'createIndex',
        index: {
          table: toTableRef(table),
          columns: normalizeIndexColumns(columns),
          ...options,
        },
      })
    },
    async dropIndex(table, name, options) {
      await emit({
        kind: 'dropIndex',
        table: toTableRef(table),
        name,
        ifExists: options?.ifExists,
      })
    },
    async renameIndex(table, from, to) {
      await emit({
        kind: 'renameIndex',
        table: toTableRef(table),
        from,
        to,
      })
    },
    async addForeignKey(table, columns, refTable, refColumns, options) {
      await emit({
        kind: 'addForeignKey',
        table: toTableRef(table),
        constraint: {
          columns: [...columns],
          references: {
            table: toTableRef(refTable),
            columns: refColumns ? [...refColumns] : ['id'],
          },
          name: options?.name,
          onDelete: options?.onDelete,
          onUpdate: options?.onUpdate,
        },
      })
    },
    async dropForeignKey(table, name) {
      await emit({
        kind: 'dropForeignKey',
        table: toTableRef(table),
        name,
      })
    },
    async addCheck(table, name, expression) {
      await emit({
        kind: 'addCheck',
        table: toTableRef(table),
        constraint: {
          expression,
          name,
        },
      })
    },
    async dropCheck(table, name) {
      await emit({
        kind: 'dropCheck',
        table: toTableRef(table),
        name,
      })
    },
    async raw(sql) {
      await emit({
        kind: 'raw',
        sql: rawSql(sql),
      })
    },
    async tableExists(name) {
      try {
        await db.exec(rawSql('select 1 from ' + name + ' limit 1'))
        return true
      } catch {
        return false
      }
    },
    async columnExists(table, columnName) {
      try {
        await db.exec(rawSql('select ' + columnName + ' from ' + table + ' where 1 = 0'))
        return true
      } catch {
        return false
      }
    },
  }
}

function normalizeChecksum(migration: MigrationDescriptor): string {
  if (migration.checksum) {
    return migration.checksum
  }

  return migration.id + ':' + migration.name
}

async function ensureMigrationJournal(adapter: DatabaseAdapter, tableName: string): Promise<void> {
  await adapter.migrate({
    operation: {
      kind: 'createTable',
      table: { name: tableName },
      ifNotExists: true,
      columns: {
        id: { type: 'varchar', length: 64, nullable: false, primaryKey: true },
        name: { type: 'varchar', length: 255, nullable: false },
        checksum: { type: 'varchar', length: 128, nullable: false },
        batch: { type: 'integer', nullable: false },
        applied_at: { type: 'timestamp', nullable: false, default: { kind: 'now' } },
      },
    },
  })
}

async function hasMigrationJournal(adapter: DatabaseAdapter, tableName: string): Promise<boolean> {
  try {
    await adapter.execute({
      operation: {
        kind: 'raw',
        sql: rawSql('select 1 from ' + tableName + ' limit 1'),
      },
    })

    return true
  } catch {
    return false
  }
}

async function loadJournalRows(
  adapter: DatabaseAdapter,
  tableName: string,
): Promise<MigrationJournalRow[]> {
  let result = await adapter.execute({
    operation: {
      kind: 'raw',
      sql: rawSql(
        'select id, name, checksum, batch, applied_at from ' + tableName + ' order by id asc',
      ),
    },
  })

  let rows = result.rows ?? []

  return rows.map((row) => ({
    id: String(row.id),
    name: String(row.name),
    checksum: String(row.checksum),
    batch: Number(row.batch),
    appliedAt: new Date(String(row.applied_at)),
  }))
}

async function insertJournalRow(
  adapter: DatabaseAdapter,
  tableName: string,
  row: {
    id: string
    name: string
    checksum: string
    batch: number
  },
  transaction?: TransactionToken,
): Promise<void> {
  await adapter.execute({
    operation: {
      kind: 'raw',
      sql: rawSql(
        'insert into ' + tableName + ' (id, name, checksum, batch, applied_at) values (?, ?, ?, ?, ?)',
        [row.id, row.name, row.checksum, row.batch, new Date().toISOString()],
      ),
    },
    transaction,
  })
}

async function deleteJournalRow(
  adapter: DatabaseAdapter,
  tableName: string,
  id: string,
  transaction?: TransactionToken,
): Promise<void> {
  await adapter.execute({
    operation: {
      kind: 'raw',
      sql: rawSql('delete from ' + tableName + ' where id = ?', [id]),
    },
    transaction,
  })
}

function sortMigrations(migrations: MigrationDescriptor[]): MigrationDescriptor[] {
  return [...migrations].sort((left, right) => left.id.localeCompare(right.id))
}

function getBatch(rows: MigrationJournalRow[]): number {
  if (rows.length === 0) {
    return 1
  }

  let max = Math.max(...rows.map((row) => row.batch))
  return max + 1
}

export type MigrationRegistry = {
  register(migration: MigrationDescriptor): void
  list(): MigrationDescriptor[]
}

export function createMigrationRegistry(initial: MigrationDescriptor[] = []): MigrationRegistry {
  let migrations = new Map<string, MigrationDescriptor>()

  for (let migration of initial) {
    if (migrations.has(migration.id)) {
      throw new Error('Duplicate migration id: ' + migration.id)
    }

    migrations.set(migration.id, migration)
  }

  return {
    register(migration: MigrationDescriptor) {
      if (migrations.has(migration.id)) {
        throw new Error('Duplicate migration id: ' + migration.id)
      }

      migrations.set(migration.id, migration)
    },
    list() {
      return sortMigrations(Array.from(migrations.values()))
    },
  }
}

export type MigrationRunnerOptions = {
  tableName?: string
}

export type MigrationRunner = {
  up(options?: MigrateOptions): Promise<MigrateResult>
  down(options?: MigrateOptions): Promise<MigrateResult>
  status(): Promise<MigrationStatusEntry[]>
}

function resolveMigrations(input: MigrationDescriptor[] | MigrationRegistry): MigrationDescriptor[] {
  if (Array.isArray(input)) {
    return sortMigrations(input)
  }

  return input.list()
}

export function createMigrationRunner(
  adapter: DatabaseAdapter,
  migrations: MigrationDescriptor[] | MigrationRegistry,
  options: MigrationRunnerOptions = {},
): MigrationRunner {
  let tableName = options.tableName ?? 'data_table_migrations'

  return {
    async up(runOptions: MigrateOptions = {}): Promise<MigrateResult> {
      return runMigrations({
        adapter,
        migrations: resolveMigrations(migrations),
        tableName,
        direction: 'up',
        options: runOptions,
      })
    },
    async down(runOptions: MigrateOptions = {}): Promise<MigrateResult> {
      return runMigrations({
        adapter,
        migrations: resolveMigrations(migrations),
        tableName,
        direction: 'down',
        options: runOptions,
      })
    },
    async status(): Promise<MigrationStatusEntry[]> {
      await ensureMigrationJournal(adapter, tableName)

      let journal = await loadJournalRows(adapter, tableName)
      let journalMap = new Map(journal.map((row) => [row.id, row]))
      let sortedMigrations = resolveMigrations(migrations)

      return sortedMigrations.map((migration) => {
        let journalRow = journalMap.get(migration.id)

        if (!journalRow) {
          return {
            id: migration.id,
            name: migration.name,
            status: 'pending' as MigrationStatus,
          }
        }

        let checksum = normalizeChecksum(migration)

        return {
          id: migration.id,
          name: migration.name,
          status: checksum === journalRow.checksum ? ('applied' as MigrationStatus) : ('drifted' as MigrationStatus),
          appliedAt: journalRow.appliedAt,
          batch: journalRow.batch,
          checksum: journalRow.checksum,
        }
      })
    },
  }
}

type RunMigrationsInput = {
  adapter: DatabaseAdapter
  migrations: MigrationDescriptor[]
  tableName: string
  direction: MigrationDirection
  options: MigrateOptions
}

function assertStepOption(step: number | undefined): void {
  if (step === undefined) {
    return
  }

  if (!Number.isInteger(step) || step < 1) {
    throw new Error('Invalid migration step option. Expected a positive integer.')
  }
}

function assertTargetOption(migrations: MigrationDescriptor[], to: string | undefined): void {
  if (!to) {
    return
  }

  let target = migrations.find((migration) => migration.id === to)

  if (!target) {
    throw new Error('Unknown migration target: ' + to)
  }
}

function assertNoMigrationDrift(
  migrations: MigrationDescriptor[],
  journal: MigrationJournalRow[],
): void {
  let migrationMap = new Map(migrations.map((migration) => [migration.id, migration]))

  for (let row of journal) {
    let migration = migrationMap.get(row.id)

    if (!migration) {
      continue
    }

    let expected = normalizeChecksum(migration)

    if (expected !== row.checksum) {
      throw new Error(
        'Migration checksum drift detected for "' +
          row.id +
          '" (journal=' +
          row.checksum +
          ', current=' +
          expected +
          ')',
      )
    }
  }
}

function createDryRunDatabase(adapter: DatabaseAdapter): Database {
  let error = new Error('Cannot execute data operations while running migrations with dryRun')
  let throwDryRunError = async (): Promise<never> => {
    throw error
  }
  let dryRunAdapter: DatabaseAdapter = {
    dialect: adapter.dialect,
    capabilities: adapter.capabilities,
    compileSql(operation) {
      return adapter.compileSql(operation)
    },
    execute: throwDryRunError,
    migrate: throwDryRunError,
    beginTransaction: throwDryRunError,
    commitTransaction: throwDryRunError,
    rollbackTransaction: throwDryRunError,
    createSavepoint: throwDryRunError,
    rollbackToSavepoint: throwDryRunError,
    releaseSavepoint: throwDryRunError,
  }

  return createDatabase(dryRunAdapter)
}

async function runMigrations(input: RunMigrationsInput): Promise<MigrateResult> {
  let adapter = input.adapter
  let migrations = input.migrations
  let tableName = input.tableName
  let dryRun = Boolean(input.options.dryRun)
  let target = input.options.to
  let step = input.options.step

  assertStepOption(step)
  assertTargetOption(migrations, target)

  let sql: Array<{ text: string; values: unknown[] }> = []

  await adapter.acquireMigrationLock?.()

  try {
    let journal: MigrationJournalRow[] = []

    if (dryRun) {
      let canReadJournal = await hasMigrationJournal(adapter, tableName)

      if (canReadJournal) {
        journal = await loadJournalRows(adapter, tableName)
      }
    } else {
      await ensureMigrationJournal(adapter, tableName)
      journal = await loadJournalRows(adapter, tableName)
    }

    let appliedMap = new Map(journal.map((row) => [row.id, row]))
    assertNoMigrationDrift(migrations, journal)
    let toRun: MigrationDescriptor[] = []

    if (input.direction === 'up') {
      for (let migration of migrations) {
        if (!appliedMap.has(migration.id)) {
          toRun.push(migration)
        }
      }

      if (target) {
        toRun = toRun.filter((migration) => migration.id <= target)
      }

      if (step !== undefined) {
        toRun = toRun.slice(0, step)
      }
    } else {
      let appliedMigrations = migrations.filter((migration) => appliedMap.has(migration.id)).reverse()

      if (target) {
        appliedMigrations = appliedMigrations.filter((migration) => migration.id >= target)
      }

      if (step !== undefined) {
        appliedMigrations = appliedMigrations.slice(0, step)
      }

      toRun = appliedMigrations
    }

    let applied: MigrationStatusEntry[] = []
    let reverted: MigrationStatusEntry[] = []
    let batch = getBatch(journal)

    for (let migration of toRun) {
      if (migration.migration.transaction === 'required' && !adapter.capabilities.transactionalDdl) {
        throw new Error(
          'Migration "' + migration.id + '" requires transactional DDL, but adapter does not support it',
        )
      }

      let shouldUseTransaction =
        !dryRun &&
        migration.migration.transaction !== 'none' &&
        adapter.capabilities.transactionalDdl
      let token: TransactionToken | undefined
      let db = dryRun ? createDryRunDatabase(adapter) : createDatabase(adapter)

      if (shouldUseTransaction) {
        token = await adapter.beginTransaction()
      }

      let schema = createSchemaApi(db, async (operation) => {
        let compiled = adapter.compileSql(operation)
        sql.push(...compiled)

        if (!dryRun) {
          await adapter.migrate({ operation, transaction: token })
        }
      })

      let context: MigrationContext = {
        dialect: adapter.dialect,
        schema,
        db,
      }

      try {
        if (input.direction === 'up') {
          await migration.migration.up(context)

          if (!dryRun) {
            await insertJournalRow(adapter, tableName, {
              id: migration.id,
              name: migration.name,
              checksum: normalizeChecksum(migration),
              batch,
            }, token)
          }

          applied.push({
            id: migration.id,
            name: migration.name,
            status: 'applied',
          })
        } else {
          await migration.migration.down(context)

          if (!dryRun) {
            await deleteJournalRow(adapter, tableName, migration.id, token)
          }

          reverted.push({
            id: migration.id,
            name: migration.name,
            status: 'pending',
          })
        }

        if (token) {
          await adapter.commitTransaction(token)
        }
      } catch (error) {
        if (token) {
          await adapter.rollbackTransaction(token)
        }

        throw error
      }
    }

    return {
      applied,
      reverted,
      sql,
    }
  } finally {
    await adapter.releaseMigrationLock?.()
  }
}

export type MigrationFileInfo = {
  id: string
  name: string
  path: string
  checksum: string
}

export function parseMigrationFilename(filename: string): { id: string; name: string } {
  let match = filename.match(migrationFilenamePattern)

  if (!match) {
    throw new Error(
      'Invalid migration filename "' +
        filename +
        '". Expected format YYYYMMDDHHmmss_name.ts (or .js/.mts/.mjs/.cts/.cjs)',
    )
  }

  return {
    id: match[1],
    name: match[2],
  }
}
