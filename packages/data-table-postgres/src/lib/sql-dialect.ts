import type { ColumnDefinition } from '@remix-run/data-table'
import type { SqlCompilerDialect } from '@remix-run/data-table/internal/sql-compiler'

export let postgresSqlDialect: SqlCompilerDialect = {
  name: 'postgres',
  quoteIdentifier(value) {
    return '"' + value.replace(/"/g, '""') + '"'
  },
  quoteLiteral(value) {
    return quoteSqlLiteral(value, false)
  },
  placeholder(index) {
    return '$' + String(index)
  },
  normalizeBoundValue(value) {
    return value
  },
  compileColumnType(definition) {
    return compilePostgresColumnType(definition)
  },
  defaultInsertValuesClause: ' default values',
  nowExpression: 'now()',
  supportsReturning: true,
  supportsIlikeOperator: true,
  rewriteRawQuestionPlaceholders: true,
  upsertKind: 'onConflict',
  migration: {
    createTableCommentStyle: 'commentOnTable',
    setTableCommentStyle: 'commentOnTable',
    changeColumnStyle: 'alterType',
    dropColumnSupportsIfExists: true,
    addPrimaryKeyConstraintName: true,
    dropPrimaryKeyStyle: 'dropConstraint',
    dropUniqueStyle: 'dropConstraint',
    dropForeignKeyStyle: 'dropConstraint',
    dropCheckStyle: 'dropConstraint',
    renameTableStyle: 'alterTableRename',
    dropTableSupportsCascade: true,
    createIndexSupportsIfNotExists: true,
    createIndexSupportsUsing: true,
    dropIndexStyle: 'dropIndex',
    renameIndexStyle: 'alterIndexRename',
    computedColumnStyle: 'storedOnly',
    virtualComputedKeyword: 'virtual',
    computedStoredOnlyError: 'Postgres only supports stored computed/generated columns',
  },
}

function compilePostgresColumnType(definition: ColumnDefinition): string {
  if (definition.type === 'varchar') {
    return 'varchar(' + String(definition.length ?? 255) + ')'
  }

  if (definition.type === 'text') {
    return 'text'
  }

  if (definition.type === 'integer') {
    return 'integer'
  }

  if (definition.type === 'bigint') {
    return 'bigint'
  }

  if (definition.type === 'decimal') {
    if (definition.precision !== undefined && definition.scale !== undefined) {
      return 'decimal(' + String(definition.precision) + ', ' + String(definition.scale) + ')'
    }

    return 'decimal'
  }

  if (definition.type === 'boolean') {
    return 'boolean'
  }

  if (definition.type === 'uuid') {
    return 'uuid'
  }

  if (definition.type === 'date') {
    return 'date'
  }

  if (definition.type === 'time') {
    return definition.withTimezone ? 'time with time zone' : 'time without time zone'
  }

  if (definition.type === 'timestamp') {
    return definition.withTimezone
      ? 'timestamp with time zone'
      : 'timestamp without time zone'
  }

  if (definition.type === 'json') {
    return 'jsonb'
  }

  if (definition.type === 'binary') {
    return 'bytea'
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
