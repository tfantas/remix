import type { ColumnDefinition } from '@remix-run/data-table'
import type { SqlCompilerDialect } from '@remix-run/data-table/internal/sql-compiler'

export let sqliteSqlDialect: SqlCompilerDialect = {
  name: 'sqlite',
  quoteIdentifier(value) {
    return '"' + value.replace(/"/g, '""') + '"'
  },
  quoteLiteral(value) {
    return quoteSqlLiteral(value, true)
  },
  placeholder() {
    return '?'
  },
  normalizeBoundValue(value) {
    if (typeof value === 'boolean') {
      return value ? 1 : 0
    }

    return value
  },
  compileColumnType(definition) {
    return compileSqliteColumnType(definition)
  },
  defaultInsertValuesClause: ' default values',
  nowExpression: 'current_timestamp',
  supportsReturning: true,
  supportsIlikeOperator: false,
  rewriteRawQuestionPlaceholders: false,
  upsertKind: 'onConflict',
  migration: {
    createTableCommentStyle: 'none',
    setTableCommentStyle: 'none',
    changeColumnStyle: 'alterType',
    dropColumnSupportsIfExists: false,
    addPrimaryKeyConstraintName: false,
    dropPrimaryKeyStyle: 'dropPrimaryKey',
    dropUniqueStyle: 'dropConstraint',
    dropForeignKeyStyle: 'dropConstraint',
    dropCheckStyle: 'dropConstraint',
    renameTableStyle: 'alterTableRename',
    dropTableSupportsCascade: false,
    createIndexSupportsIfNotExists: true,
    createIndexSupportsUsing: false,
    dropIndexStyle: 'dropIndex',
    renameIndexStyle: 'alterTableRenameIndex',
    computedColumnStyle: 'storedOrVirtual',
    virtualComputedKeyword: 'virtual',
    computedStoredOnlyError: 'SQLite supports virtual and stored computed/generated columns',
  },
}

function compileSqliteColumnType(definition: ColumnDefinition): string {
  if (definition.type === 'varchar') {
    return 'text'
  }

  if (definition.type === 'text') {
    return 'text'
  }

  if (definition.type === 'integer') {
    return 'integer'
  }

  if (definition.type === 'bigint') {
    return 'integer'
  }

  if (definition.type === 'decimal') {
    return 'numeric'
  }

  if (definition.type === 'boolean') {
    return 'integer'
  }

  if (definition.type === 'uuid') {
    return 'text'
  }

  if (definition.type === 'date') {
    return 'text'
  }

  if (definition.type === 'time') {
    return 'text'
  }

  if (definition.type === 'timestamp') {
    return 'text'
  }

  if (definition.type === 'json') {
    return 'text'
  }

  if (definition.type === 'binary') {
    return 'blob'
  }

  if (definition.type === 'enum') {
    return 'text'
  }

  return 'text'
}

function quoteSqlLiteral(value: unknown, booleanAsInteger: boolean): string {
  if (value === null) {
    return 'null'
  }

  if (typeof value === 'number' || typeof value === 'bigint') {
    return String(value)
  }

  if (typeof value === 'boolean') {
    if (booleanAsInteger) {
      return value ? '1' : '0'
    }

    return value ? 'true' : 'false'
  }

  if (value instanceof Date) {
    return quoteSqlLiteral(value.toISOString(), booleanAsInteger)
  }

  return "'" + String(value).replace(/'/g, "''") + "'"
}
