import type { Database as DataManipulationDatabase } from './database.ts'
import type {
  ColumnDefinition,
  ForeignKeyAction,
  IndexDefinition,
} from './adapter.ts'
import type { ColumnBuilder } from './column.ts'
import type { SqlStatement } from './sql.ts'
import type { AnyTable } from './table.ts'

/**
 * Controls how each migration is wrapped in transactions.
 */
export type MigrationTransactionMode = 'auto' | 'required' | 'none'

/**
 * Database API available inside migrations.
 */
export type Database = DataManipulationDatabase & MigrationOperations

/**
 * Runtime context passed to migration `up`/`down` handlers.
 */
export type MigrationContext = {
  dialect: string
  db: Database
}

/**
 * Authoring shape for `createMigration(...)`.
 */
export type CreateMigrationInput = {
  up: (context: MigrationContext) => Promise<void> | void
  down: (context: MigrationContext) => Promise<void> | void
  transaction?: MigrationTransactionMode
}

/**
 * Normalized migration object consumed by the registry/runner.
 */
export type Migration = {
  up: CreateMigrationInput['up']
  down: CreateMigrationInput['down']
  transaction: MigrationTransactionMode
}

/**
 * Creates a migration descriptor with normalized defaults.
 * @param input Migration handlers and transaction mode.
 * @returns A normalized migration object.
 * @example
 * ```ts
 * import { createMigration, column as c } from 'remix/data-table/migrations'
 * import { table } from 'remix/data-table'
 *
 * let users = table({
 *   name: 'users',
 *   columns: {
 *     id: c.integer().primaryKey().autoIncrement(),
 *     email: c.varchar(255).notNull().unique(),
 *   },
 * })
 *
 * export default createMigration({
 *   async up({ db }) {
 *     await db.createTable(users)
 *   },
 *   async down({ db }) {
 *     await db.dropTable('users', { ifExists: true })
 *   },
 * })
 * ```
 */
export function createMigration(input: CreateMigrationInput): Migration {
  return {
    up: input.up,
    down: input.down,
    transaction: input.transaction ?? 'auto',
  }
}

/**
 * Migration metadata stored in registries and returned by loaders.
 */
export type MigrationDescriptor = {
  id: string
  name: string
  path?: string
  checksum?: string
  migration: Migration
}

/**
 * Direction used by migration runner operations.
 */
export type MigrationDirection = 'up' | 'down'

/**
 * Row shape persisted in the migration journal table.
 */
export type MigrationJournalRow = {
  id: string
  name: string
  checksum: string
  batch: number
  appliedAt: Date
}

/**
 * Effective status for a known migration.
 */
export type MigrationStatus = 'applied' | 'pending' | 'drifted'

/**
 * Status row returned by `runner.status()` and `runner.up/down(...)`.
 */
export type MigrationStatusEntry = {
  id: string
  name: string
  status: MigrationStatus
  appliedAt?: Date
  batch?: number
  checksum?: string
}

/**
 * Common options for `runner.up(...)` and `runner.down(...)`.
 * `to` and `step` are mutually exclusive.
 */
export type MigrateOptions =
  | {
      to: string
      step?: never
      dryRun?: boolean
    }
  | {
      to?: never
      step: number
      dryRun?: boolean
    }
  | {
      to?: undefined
      step?: undefined
      dryRun?: boolean
    }

/**
 * Result shape returned by migration runner commands.
 */
export type MigrateResult = {
  applied: MigrationStatusEntry[]
  reverted: MigrationStatusEntry[]
  /**
   * Compiled SQL statements for operations processed during this run.
   * Includes planned SQL when running with `dryRun: true`.
   */
  sql: SqlStatement[]
}

/**
 * Options for `db.createTable(...)` migration operations.
 */
export type CreateTableOptions = { ifNotExists?: boolean }
/**
 * Options for `db.alterTable(...)` migration operations.
 */
export type AlterTableOptions = { ifExists?: boolean }
/**
 * Options for `db.dropTable(...)` migration operations.
 */
export type DropTableOptions = { ifExists?: boolean; cascade?: boolean }
/**
 * Accepts either one index column or multiple (compound index).
 */
export type IndexColumns = string | string[]

/**
 * Accepts either a SQL table name or a `table(...)` object.
 */
export type TableInput = string | AnyTable

/**
 * Builder API available inside `db.alterTable(name, table => ...)`.
 */
export interface AlterTableBuilder {
  addColumn(name: string, definition: ColumnDefinition | ColumnBuilder): void
  changeColumn(name: string, definition: ColumnDefinition | ColumnBuilder): void
  renameColumn(from: string, to: string): void
  dropColumn(name: string, options?: { ifExists?: boolean }): void
  addPrimaryKey(name: string, columns: string[]): void
  dropPrimaryKey(name: string): void
  addUnique(name: string, columns: string[]): void
  dropUnique(name: string): void
  addForeignKey(
    name: string,
    columns: string[],
    refTable: TableInput,
    refColumns?: string[],
    options?: { onDelete?: ForeignKeyAction; onUpdate?: ForeignKeyAction },
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

/**
 * DDL-focused operations mixed into the migration `db` object.
 */
export interface MigrationOperations {
  createTable<table extends AnyTable>(table: table, options?: CreateTableOptions): Promise<void>
  alterTable(
    table: TableInput,
    migrate: (table: AlterTableBuilder) => void,
    options?: AlterTableOptions,
  ): Promise<void>
  renameTable(from: TableInput, to: TableInput): Promise<void>
  dropTable(table: TableInput, options?: DropTableOptions): Promise<void>
  createIndex(
    table: TableInput,
    name: string,
    columns: IndexColumns,
    options?: Omit<IndexDefinition, 'table' | 'name' | 'columns'>,
  ): Promise<void>
  dropIndex(table: TableInput, name: string, options?: { ifExists?: boolean }): Promise<void>
  renameIndex(table: TableInput, from: string, to: string): Promise<void>
  addForeignKey(
    table: TableInput,
    name: string,
    columns: string[],
    refTable: TableInput,
    refColumns?: string[],
    options?: { onDelete?: ForeignKeyAction; onUpdate?: ForeignKeyAction },
  ): Promise<void>
  dropForeignKey(table: TableInput, name: string): Promise<void>
  addCheck(table: TableInput, name: string, expression: string): Promise<void>
  dropCheck(table: TableInput, name: string): Promise<void>
  /**
   * Adds raw SQL to the migration plan as a migration operation.
   */
  plan(sql: string | SqlStatement): Promise<void>
  /**
   * Returns `true` when the table exists in the current database.
   */
  hasTable(table: TableInput): Promise<boolean>
  /**
   * Returns `true` when the column exists on the given table.
   */
  hasColumn(table: TableInput, column: string): Promise<boolean>
}

/**
 * Runtime-agnostic migration registry abstraction.
 */
export type MigrationRegistry = {
  register(migration: MigrationDescriptor): void
  list(): MigrationDescriptor[]
}

/**
 * Options for creating a migration runner.
 */
export type MigrationRunnerOptions = {
  /**
   * Journal table used to record applied migrations.
   * Defaults to `data_table_migrations`.
   */
  journalTable?: string
}

/**
 * Migration runner API for applying, reverting, and inspecting migration state.
 */
export type MigrationRunner = {
  up(options?: MigrateOptions): Promise<MigrateResult>
  down(options?: MigrateOptions): Promise<MigrateResult>
  status(): Promise<MigrationStatusEntry[]>
}
