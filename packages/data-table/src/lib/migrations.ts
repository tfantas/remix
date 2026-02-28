import type { Database } from './database.ts'
import type {
  ColumnDefinition,
  DataMigrationOperation,
  ForeignKeyAction,
  IndexDefinition,
} from './adapter.ts'
import type { ColumnBuilder } from './migrations/column-builder.ts'

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

export type ColumnNamespace = {
  varchar(length: number): ColumnBuilder
  text(): ColumnBuilder
  integer(): ColumnBuilder
  bigint(): ColumnBuilder
  decimal(precision: number, scale: number): ColumnBuilder
  boolean(): ColumnBuilder
  uuid(): ColumnBuilder
  date(): ColumnBuilder
  time(options?: { precision?: number; withTimezone?: boolean }): ColumnBuilder
  timestamp(options?: { precision?: number; withTimezone?: boolean }): ColumnBuilder
  json(): ColumnBuilder
  binary(length?: number): ColumnBuilder
  enum(values: readonly string[]): ColumnBuilder
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

export type MigrationRegistry = {
  register(migration: MigrationDescriptor): void
  list(): MigrationDescriptor[]
}

export type MigrationRunnerOptions = {
  tableName?: string
}

export type MigrationRunner = {
  up(options?: MigrateOptions): Promise<MigrateResult>
  down(options?: MigrateOptions): Promise<MigrateResult>
  status(): Promise<MigrationStatusEntry[]>
}

export type MigrationFileInfo = {
  id: string
  name: string
  path: string
  checksum: string
}
