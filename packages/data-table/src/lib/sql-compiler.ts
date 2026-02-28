import type {
  ColumnDefinition,
  DataMigrationOperation,
  DataManipulationOperation,
  TableRef,
} from './adapter.ts'
import type { Predicate } from './operators.ts'
import { getTableName, getTablePrimaryKey } from './table.ts'
import type { SqlStatement } from './sql.ts'

export type SqlCompilerDialect = {
  name: string
  quoteIdentifier: (value: string) => string
  quoteLiteral: (value: unknown) => string
  placeholder: (index: number) => string
  normalizeBoundValue: (value: unknown) => unknown
  compileColumnType: (
    definition: ColumnDefinition,
    tools: { quoteLiteral: (value: unknown) => string },
  ) => string
  defaultInsertValuesClause: string
  nowExpression: string
  supportsReturning: boolean
  supportsIlikeOperator: boolean
  rewriteRawQuestionPlaceholders: boolean
  upsertKind: 'onConflict' | 'onDuplicateKeyUpdate'
  migration: {
    createTableCommentStyle: 'none' | 'commentOnTable' | 'alterTableComment'
    setTableCommentStyle: 'none' | 'commentOnTable' | 'alterTableComment'
    changeColumnStyle: 'modifyColumn' | 'alterType'
    dropColumnSupportsIfExists: boolean
    addPrimaryKeyConstraintName: boolean
    dropPrimaryKeyStyle: 'dropPrimaryKey' | 'dropConstraint'
    dropUniqueStyle: 'dropIndex' | 'dropConstraint'
    dropForeignKeyStyle: 'dropForeignKey' | 'dropConstraint'
    dropCheckStyle: 'dropCheck' | 'dropConstraint'
    renameTableStyle: 'renameTable' | 'alterTableRename'
    dropTableSupportsCascade: boolean
    createIndexSupportsIfNotExists: boolean
    createIndexSupportsUsing: boolean
    dropIndexStyle: 'dropIndex' | 'dropIndexOnTable'
    renameIndexStyle: 'alterIndexRename' | 'alterTableRenameIndex'
    computedColumnStyle: 'storedOnly' | 'storedOrVirtual'
    virtualComputedKeyword: string
    computedStoredOnlyError: string
  }
}

export type SqlCompilerOptions = {
  dialect: SqlCompilerDialect
  rewriteMigrationOperation?: (operation: DataMigrationOperation) => DataMigrationOperation[]
}

type JoinClause = Extract<DataManipulationOperation, { kind: 'select' }>['joins'][number]
type UpsertOperation = Extract<DataManipulationOperation, { kind: 'upsert' }>
type OperationTable = Extract<DataManipulationOperation, { kind: 'select' }>['table']

type CompileContext = {
  dialect: SqlCompilerDialect
  values: unknown[]
}

export function compileOperationToSql(
  operation: DataManipulationOperation | DataMigrationOperation,
  options: SqlCompilerOptions,
): SqlStatement[] {
  if (isDataManipulationOperation(operation)) {
    return [compileDataManipulationOperation(operation, options)]
  }

  return compileDataMigrationOperations(operation, options)
}

export function compileDataManipulationOperation(
  operation: DataManipulationOperation,
  options: SqlCompilerOptions,
): SqlStatement {
  if (operation.kind === 'raw') {
    return compileRawManipulationStatement(operation.sql, options.dialect)
  }

  let context: CompileContext = {
    dialect: options.dialect,
    values: [],
  }

  if (operation.kind === 'select') {
    let selection = '*'

    if (operation.select !== '*') {
      selection = operation.select
        .map((field) => quotePath(options.dialect, field.column) + ' as ' + quoteIdentifier(options.dialect, field.alias))
        .join(', ')
    }

    return {
      text:
        'select ' +
        (operation.distinct ? 'distinct ' : '') +
        selection +
        compileFromClause(operation.table, operation.joins, context) +
        compileWhereClause(operation.where, context) +
        compileGroupByClause(operation.groupBy, options.dialect) +
        compileHavingClause(operation.having, context) +
        compileOrderByClause(operation.orderBy, options.dialect) +
        compileLimitClause(operation.limit) +
        compileOffsetClause(operation.offset),
      values: context.values,
    }
  }

  if (operation.kind === 'count' || operation.kind === 'exists') {
    let inner =
      'select 1' +
      compileFromClause(operation.table, operation.joins, context) +
      compileWhereClause(operation.where, context) +
      compileGroupByClause(operation.groupBy, options.dialect) +
      compileHavingClause(operation.having, context)

    return {
      text:
        'select count(*) as ' +
        quoteIdentifier(options.dialect, 'count') +
        ' from (' +
        inner +
        ') as ' +
        quoteIdentifier(options.dialect, '__dt_count'),
      values: context.values,
    }
  }

  if (operation.kind === 'insert') {
    return compileInsertOperation(operation.table, operation.values, operation.returning, context)
  }

  if (operation.kind === 'insertMany') {
    return compileInsertManyOperation(operation.table, operation.values, operation.returning, context)
  }

  if (operation.kind === 'update') {
    let columns = Object.keys(operation.changes)

    return {
      text:
        'update ' +
        quotePath(options.dialect, getTableName(operation.table)) +
        ' set ' +
        columns
          .map(
            (column) =>
              quotePath(options.dialect, column) + ' = ' + pushValue(context, operation.changes[column]),
          )
          .join(', ') +
        compileWhereClause(operation.where, context) +
        compileReturningClause(operation.returning, options.dialect),
      values: context.values,
    }
  }

  if (operation.kind === 'delete') {
    return {
      text:
        'delete from ' +
        quotePath(options.dialect, getTableName(operation.table)) +
        compileWhereClause(operation.where, context) +
        compileReturningClause(operation.returning, options.dialect),
      values: context.values,
    }
  }

  if (operation.kind === 'upsert') {
    return compileUpsertOperation(operation, context)
  }

  throw new Error('Unsupported statement kind')
}

export function compileDataMigrationOperations(
  operation: DataMigrationOperation,
  options: SqlCompilerOptions,
): SqlStatement[] {
  let operations = options.rewriteMigrationOperation
    ? options.rewriteMigrationOperation(operation)
    : [operation]

  let statements: SqlStatement[] = []

  for (let currentOperation of operations) {
    statements.push(...compileDataMigrationOperation(currentOperation, options.dialect))
  }

  return statements
}

function compileInsertOperation(
  table: OperationTable,
  values: Record<string, unknown>,
  returning: '*' | string[] | undefined,
  context: CompileContext,
): SqlStatement {
  let columns = Object.keys(values)

  if (columns.length === 0) {
    return {
      text:
        'insert into ' +
        quotePath(context.dialect, getTableName(table)) +
        compileDefaultInsertValuesClause(context.dialect) +
        compileReturningClause(returning, context.dialect),
      values: context.values,
    }
  }

  return {
    text:
      'insert into ' +
      quotePath(context.dialect, getTableName(table)) +
      ' (' +
      columns.map((column) => quotePath(context.dialect, column)).join(', ') +
      ') values (' +
      columns.map((column) => pushValue(context, values[column])).join(', ') +
      ')' +
      compileReturningClause(returning, context.dialect),
    values: context.values,
  }
}

function compileInsertManyOperation(
  table: OperationTable,
  rows: Record<string, unknown>[],
  returning: '*' | string[] | undefined,
  context: CompileContext,
): SqlStatement {
  if (rows.length === 0) {
    return {
      text: 'select 0 where 1 = 0',
      values: context.values,
    }
  }

  let columns = collectColumns(rows)

  if (columns.length === 0) {
    return {
      text:
        'insert into ' +
        quotePath(context.dialect, getTableName(table)) +
        compileDefaultInsertValuesClause(context.dialect) +
        compileReturningClause(returning, context.dialect),
      values: context.values,
    }
  }

  let values = rows.map(
    (row) =>
      '(' +
      columns
        .map((column) => {
          let value = Object.prototype.hasOwnProperty.call(row, column) ? row[column] : null
          return pushValue(context, value)
        })
        .join(', ') +
      ')',
  )

  return {
    text:
      'insert into ' +
      quotePath(context.dialect, getTableName(table)) +
      ' (' +
      columns.map((column) => quotePath(context.dialect, column)).join(', ') +
      ') values ' +
      values.join(', ') +
      compileReturningClause(returning, context.dialect),
    values: context.values,
  }
}

function compileUpsertOperation(operation: UpsertOperation, context: CompileContext): SqlStatement {
  let insertColumns = Object.keys(operation.values)

  if (insertColumns.length === 0) {
    throw new Error('upsert requires at least one value')
  }

  if (context.dialect.upsertKind === 'onDuplicateKeyUpdate') {
    let updateValues = operation.update ?? operation.values
    let updateColumns = Object.keys(updateValues)
    let fallbackNoopColumn = getTablePrimaryKey(operation.table)[0]

    let onDuplicate =
      updateColumns.length > 0
        ? updateColumns
            .map(
              (column) =>
                quotePath(context.dialect, column) +
                ' = ' +
                pushValue(context, updateValues[column]),
            )
            .join(', ')
        : quotePath(context.dialect, fallbackNoopColumn) +
          ' = ' +
          quotePath(context.dialect, fallbackNoopColumn)

    return {
      text:
        'insert into ' +
        quotePath(context.dialect, getTableName(operation.table)) +
        ' (' +
        insertColumns.map((column) => quotePath(context.dialect, column)).join(', ') +
        ') values (' +
        insertColumns
          .map((column) => pushValue(context, operation.values[column]))
          .join(', ') +
        ') on duplicate key update ' +
        onDuplicate,
      values: context.values,
    }
  }

  let conflictTarget = operation.conflictTarget ?? [...getTablePrimaryKey(operation.table)]
  let updateValues = operation.update ?? operation.values
  let updateColumns = Object.keys(updateValues)

  let insertPlaceholders: string[] | undefined

  if (context.dialect.placeholder(1) !== '?') {
    insertPlaceholders = insertColumns.map((column) => pushValue(context, operation.values[column]))
  }

  let conflictClause = ''

  if (updateColumns.length === 0) {
    conflictClause =
      ' on conflict (' +
      conflictTarget
        .map((column: string) => quotePath(context.dialect, column))
        .join(', ') +
      ') do nothing'
  } else {
    conflictClause =
      ' on conflict (' +
      conflictTarget
        .map((column: string) => quotePath(context.dialect, column))
        .join(', ') +
      ') do update set ' +
      updateColumns
        .map(
          (column) =>
            quotePath(context.dialect, column) +
            ' = ' +
            pushValue(context, updateValues[column]),
        )
        .join(', ')
  }

  return {
    text:
      'insert into ' +
      quotePath(context.dialect, getTableName(operation.table)) +
      ' (' +
      insertColumns.map((column) => quotePath(context.dialect, column)).join(', ') +
      ') values (' +
      (insertPlaceholders ??
        insertColumns.map((column) => pushValue(context, operation.values[column]))).join(', ') +
      ')' +
      conflictClause +
      compileReturningClause(operation.returning, context.dialect),
    values: context.values,
  }
}

function compileRawManipulationStatement(
  statement: SqlStatement,
  dialect: SqlCompilerDialect,
): SqlStatement {
  if (!dialect.rewriteRawQuestionPlaceholders) {
    return {
      text: statement.text,
      values: [...statement.values],
    }
  }

  if (!statement.text.includes('?')) {
    return {
      text: statement.text,
      values: [...statement.values],
    }
  }

  let index = 1
  let text = statement.text.replace(/\?/g, () => {
    let placeholder = dialect.placeholder(index)
    index += 1
    return placeholder
  })

  return {
    text,
    values: [...statement.values],
  }
}

function compileFromClause(table: OperationTable, joins: JoinClause[], context: CompileContext): string {
  let output = ' from ' + quotePath(context.dialect, getTableName(table))

  for (let join of joins) {
    output +=
      ' ' +
      normalizeJoinType(join.type) +
      ' join ' +
      quotePath(context.dialect, getTableName(join.table)) +
      ' on ' +
      compilePredicate(join.on, context)
  }

  return output
}

function compileWhereClause(predicates: Predicate[], context: CompileContext): string {
  if (predicates.length === 0) {
    return ''
  }

  return (
    ' where ' +
    predicates.map((predicate) => '(' + compilePredicate(predicate, context) + ')').join(' and ')
  )
}

function compileGroupByClause(columns: string[], dialect: SqlCompilerDialect): string {
  if (columns.length === 0) {
    return ''
  }

  return ' group by ' + columns.map((column) => quotePath(dialect, column)).join(', ')
}

function compileHavingClause(predicates: Predicate[], context: CompileContext): string {
  if (predicates.length === 0) {
    return ''
  }

  return (
    ' having ' +
    predicates.map((predicate) => '(' + compilePredicate(predicate, context) + ')').join(' and ')
  )
}

function compileOrderByClause(
  orderBy: { column: string; direction: 'asc' | 'desc' }[],
  dialect: SqlCompilerDialect,
): string {
  if (orderBy.length === 0) {
    return ''
  }

  return (
    ' order by ' +
    orderBy
      .map((clause) => quotePath(dialect, clause.column) + ' ' + clause.direction.toUpperCase())
      .join(', ')
  )
}

function compileLimitClause(limit: number | undefined): string {
  if (limit === undefined) {
    return ''
  }

  return ' limit ' + String(limit)
}

function compileOffsetClause(offset: number | undefined): string {
  if (offset === undefined) {
    return ''
  }

  return ' offset ' + String(offset)
}

function compileReturningClause(
  returning: '*' | string[] | undefined,
  dialect: SqlCompilerDialect,
): string {
  if (!dialect.supportsReturning) {
    return ''
  }

  if (!returning) {
    return ''
  }

  if (returning === '*') {
    return ' returning *'
  }

  return ' returning ' + returning.map((column) => quotePath(dialect, column)).join(', ')
}

function compileDefaultInsertValuesClause(dialect: SqlCompilerDialect): string {
  return dialect.defaultInsertValuesClause
}

function compilePredicate(predicate: Predicate, context: CompileContext): string {
  if (predicate.type === 'comparison') {
    let column = quotePath(context.dialect, predicate.column)

    if (predicate.operator === 'eq') {
      if (
        predicate.valueType === 'value' &&
        (predicate.value === null || predicate.value === undefined)
      ) {
        return column + ' is null'
      }

      let comparisonValue = compileComparisonValue(predicate, context)
      return column + ' = ' + comparisonValue
    }

    if (predicate.operator === 'ne') {
      if (
        predicate.valueType === 'value' &&
        (predicate.value === null || predicate.value === undefined)
      ) {
        return column + ' is not null'
      }

      let comparisonValue = compileComparisonValue(predicate, context)
      return column + ' <> ' + comparisonValue
    }

    if (predicate.operator === 'gt') {
      let comparisonValue = compileComparisonValue(predicate, context)
      return column + ' > ' + comparisonValue
    }

    if (predicate.operator === 'gte') {
      let comparisonValue = compileComparisonValue(predicate, context)
      return column + ' >= ' + comparisonValue
    }

    if (predicate.operator === 'lt') {
      let comparisonValue = compileComparisonValue(predicate, context)
      return column + ' < ' + comparisonValue
    }

    if (predicate.operator === 'lte') {
      let comparisonValue = compileComparisonValue(predicate, context)
      return column + ' <= ' + comparisonValue
    }

    if (predicate.operator === 'in' || predicate.operator === 'notIn') {
      let values: unknown[] = Array.isArray(predicate.value) ? predicate.value : []

      if (values.length === 0) {
        return predicate.operator === 'in' ? '1 = 0' : '1 = 1'
      }

      let keyword = predicate.operator === 'in' ? 'in' : 'not in'

      return (
        column +
        ' ' +
        keyword +
        ' (' +
        values.map((value) => pushValue(context, value)).join(', ') +
        ')'
      )
    }

    if (predicate.operator === 'like') {
      let comparisonValue = compileComparisonValue(predicate, context)
      return column + ' like ' + comparisonValue
    }

    if (predicate.operator === 'ilike') {
      let comparisonValue = compileComparisonValue(predicate, context)

      if (context.dialect.supportsIlikeOperator) {
        return column + ' ilike ' + comparisonValue
      }

      return 'lower(' + column + ') like lower(' + comparisonValue + ')'
    }
  }

  if (predicate.type === 'between') {
    return (
      quotePath(context.dialect, predicate.column) +
      ' between ' +
      pushValue(context, predicate.lower) +
      ' and ' +
      pushValue(context, predicate.upper)
    )
  }

  if (predicate.type === 'null') {
    return (
      quotePath(context.dialect, predicate.column) +
      (predicate.operator === 'isNull' ? ' is null' : ' is not null')
    )
  }

  if (predicate.type === 'logical') {
    if (predicate.predicates.length === 0) {
      return predicate.operator === 'and' ? '1 = 1' : '1 = 0'
    }

    let joiner = predicate.operator === 'and' ? ' and ' : ' or '

    return predicate.predicates
      .map((child) => '(' + compilePredicate(child, context) + ')')
      .join(joiner)
  }

  throw new Error('Unsupported predicate')
}

function compileComparisonValue(
  predicate: Extract<Predicate, { type: 'comparison' }>,
  context: CompileContext,
): string {
  if (predicate.valueType === 'column') {
    return quotePath(context.dialect, predicate.value)
  }

  return pushValue(context, predicate.value)
}

function normalizeJoinType(type: string): string {
  if (type === 'left') {
    return 'left'
  }

  if (type === 'right') {
    return 'right'
  }

  return 'inner'
}

function pushValue(context: CompileContext, value: unknown): string {
  context.values.push(normalizeBoundValue(context.dialect, value))
  return context.dialect.placeholder(context.values.length)
}

function normalizeBoundValue(dialect: SqlCompilerDialect, value: unknown): unknown {
  return dialect.normalizeBoundValue(value)
}

function collectColumns(rows: Record<string, unknown>[]): string[] {
  let columns: string[] = []
  let seen = new Set<string>()

  for (let row of rows) {
    for (let key in row) {
      if (!Object.prototype.hasOwnProperty.call(row, key)) {
        continue
      }

      if (seen.has(key)) {
        continue
      }

      seen.add(key)
      columns.push(key)
    }
  }

  return columns
}

function compileDataMigrationOperation(
  operation: DataMigrationOperation,
  dialect: SqlCompilerDialect,
): SqlStatement[] {
  if (operation.kind === 'raw') {
    return [{ text: operation.sql.text, values: [...operation.sql.values] }]
  }

  if (operation.kind === 'createTable') {
    let columns = Object.keys(operation.columns).map(
      (columnName) =>
        quoteIdentifier(dialect, columnName) + ' ' + compileColumn(dialect, operation.columns[columnName]),
    )
    let constraints: string[] = []

    if (operation.primaryKey) {
      constraints.push(
        'primary key (' +
          operation.primaryKey.columns
            .map((column) => quoteIdentifier(dialect, column))
            .join(', ') +
          ')',
      )
    }

    for (let unique of operation.uniques ?? []) {
      constraints.push(
        (unique.name ? 'constraint ' + quoteIdentifier(dialect, unique.name) + ' ' : '') +
          'unique (' +
          unique.columns.map((column) => quoteIdentifier(dialect, column)).join(', ') +
          ')',
      )
    }

    for (let check of operation.checks ?? []) {
      constraints.push(
        (check.name ? 'constraint ' + quoteIdentifier(dialect, check.name) + ' ' : '') +
          'check (' +
          check.expression +
          ')',
      )
    }

    for (let foreignKey of operation.foreignKeys ?? []) {
      let clause =
        (foreignKey.name ? 'constraint ' + quoteIdentifier(dialect, foreignKey.name) + ' ' : '') +
        'foreign key (' +
        foreignKey.columns.map((column) => quoteIdentifier(dialect, column)).join(', ') +
        ') references ' +
        quoteTableRef(dialect, foreignKey.references.table) +
        ' (' +
        foreignKey.references.columns.map((column) => quoteIdentifier(dialect, column)).join(', ') +
        ')'

      if (foreignKey.onDelete) {
        clause += ' on delete ' + foreignKey.onDelete
      }

      if (foreignKey.onUpdate) {
        clause += ' on update ' + foreignKey.onUpdate
      }

      constraints.push(clause)
    }

    let statements: SqlStatement[] = [
      {
        text:
          'create table ' +
          (operation.ifNotExists ? 'if not exists ' : '') +
          quoteTableRef(dialect, operation.table) +
          ' (' +
          [...columns, ...constraints].join(', ') +
          ')',
        values: [],
      },
    ]

    if (operation.comment) {
      let tableCommentStatement = compileTableComment(
        dialect,
        operation.table,
        operation.comment,
        dialect.migration.createTableCommentStyle,
      )

      if (tableCommentStatement) {
        statements.push(tableCommentStatement)
      }
    }

    return statements
  }

  if (operation.kind === 'alterTable') {
    let statements: SqlStatement[] = []

    for (let change of operation.changes) {
      let sql = 'alter table ' + quoteTableRef(dialect, operation.table) + ' '

      if (change.kind === 'addColumn') {
        sql +=
          'add column ' +
          quoteIdentifier(dialect, change.column) +
          ' ' +
          compileColumn(dialect, change.definition)
      } else if (change.kind === 'changeColumn') {
        if (dialect.migration.changeColumnStyle === 'modifyColumn') {
          sql +=
            'modify column ' +
            quoteIdentifier(dialect, change.column) +
            ' ' +
            compileColumn(dialect, change.definition)
        } else {
          sql +=
            'alter column ' +
            quoteIdentifier(dialect, change.column) +
            ' type ' +
            compileColumnType(dialect, change.definition)
        }
      } else if (change.kind === 'renameColumn') {
        sql +=
          'rename column ' +
          quoteIdentifier(dialect, change.from) +
          ' to ' +
          quoteIdentifier(dialect, change.to)
      } else if (change.kind === 'dropColumn') {
        sql +=
          'drop column ' +
          (dialect.migration.dropColumnSupportsIfExists && change.ifExists ? 'if exists ' : '') +
          quoteIdentifier(dialect, change.column)
      } else if (change.kind === 'addPrimaryKey') {
        if (dialect.migration.addPrimaryKeyConstraintName) {
          sql +=
            'add ' +
            (change.constraint.name
              ? 'constraint ' + quoteIdentifier(dialect, change.constraint.name) + ' '
              : '') +
            'primary key (' +
            change.constraint.columns.map((column) => quoteIdentifier(dialect, column)).join(', ') +
            ')'
        } else {
          sql +=
            'add primary key (' +
            change.constraint.columns.map((column) => quoteIdentifier(dialect, column)).join(', ') +
            ')'
        }
      } else if (change.kind === 'dropPrimaryKey') {
        if (dialect.migration.dropPrimaryKeyStyle === 'dropConstraint') {
          sql += 'drop constraint ' + quoteIdentifier(dialect, change.name ?? 'PRIMARY')
        } else {
          sql += 'drop primary key'
        }
      } else if (change.kind === 'addUnique') {
        sql +=
          'add ' +
          (change.constraint.name
            ? 'constraint ' + quoteIdentifier(dialect, change.constraint.name) + ' '
            : '') +
          'unique (' +
          change.constraint.columns.map((column) => quoteIdentifier(dialect, column)).join(', ') +
          ')'
      } else if (change.kind === 'dropUnique') {
        if (dialect.migration.dropUniqueStyle === 'dropIndex') {
          sql += 'drop index ' + quoteIdentifier(dialect, change.name)
        } else {
          sql += 'drop constraint ' + quoteIdentifier(dialect, change.name)
        }
      } else if (change.kind === 'addForeignKey') {
        sql +=
          'add ' +
          (change.constraint.name
            ? 'constraint ' + quoteIdentifier(dialect, change.constraint.name) + ' '
            : '') +
          'foreign key (' +
          change.constraint.columns.map((column) => quoteIdentifier(dialect, column)).join(', ') +
          ') references ' +
          quoteTableRef(dialect, change.constraint.references.table) +
          ' (' +
          change.constraint.references.columns
            .map((column) => quoteIdentifier(dialect, column))
            .join(', ') +
          ')'
      } else if (change.kind === 'dropForeignKey') {
        if (dialect.migration.dropForeignKeyStyle === 'dropForeignKey') {
          sql += 'drop foreign key ' + quoteIdentifier(dialect, change.name)
        } else {
          sql += 'drop constraint ' + quoteIdentifier(dialect, change.name)
        }
      } else if (change.kind === 'addCheck') {
        sql +=
          'add ' +
          (change.constraint.name
            ? 'constraint ' + quoteIdentifier(dialect, change.constraint.name) + ' '
            : '') +
          'check (' +
          change.constraint.expression +
          ')'
      } else if (change.kind === 'dropCheck') {
        if (dialect.migration.dropCheckStyle === 'dropCheck') {
          sql += 'drop check ' + quoteIdentifier(dialect, change.name)
        } else {
          sql += 'drop constraint ' + quoteIdentifier(dialect, change.name)
        }
      } else if (change.kind === 'setTableComment') {
        let tableCommentStatement = compileTableComment(
          dialect,
          operation.table,
          change.comment,
          dialect.migration.setTableCommentStyle,
        )

        if (tableCommentStatement) {
          statements.push(tableCommentStatement)
        }

        continue
      } else {
        continue
      }

      statements.push({ text: sql, values: [] })
    }

    return statements
  }

  if (operation.kind === 'renameTable') {
    if (dialect.migration.renameTableStyle === 'renameTable') {
      return [
        {
          text:
            'rename table ' +
            quoteTableRef(dialect, operation.from) +
            ' to ' +
            quoteTableRef(dialect, operation.to),
          values: [],
        },
      ]
    }

    return [
      {
        text:
          'alter table ' +
          quoteTableRef(dialect, operation.from) +
          ' rename to ' +
          quoteIdentifier(dialect, operation.to.name),
        values: [],
      },
    ]
  }

  if (operation.kind === 'dropTable') {
    return [
      {
        text:
          'drop table ' +
          (operation.ifExists ? 'if exists ' : '') +
          quoteTableRef(dialect, operation.table) +
          (dialect.migration.dropTableSupportsCascade && operation.cascade ? ' cascade' : ''),
        values: [],
      },
    ]
  }

  if (operation.kind === 'createIndex') {
    return [
      {
        text:
          'create ' +
          (operation.index.unique ? 'unique ' : '') +
          'index ' +
          (dialect.migration.createIndexSupportsIfNotExists && operation.ifNotExists
            ? 'if not exists '
            : '') +
          quoteIdentifier(dialect, operation.index.name ?? defaultIndexName(operation.index.columns)) +
          ' on ' +
          quoteTableRef(dialect, operation.index.table) +
          (dialect.migration.createIndexSupportsUsing && operation.index.using
            ? ' using ' + operation.index.using
            : '') +
          ' (' +
          operation.index.columns.map((column) => quoteIdentifier(dialect, column)).join(', ') +
          ')' +
          (operation.index.where ? ' where ' + operation.index.where : ''),
        values: [],
      },
    ]
  }

  if (operation.kind === 'dropIndex') {
    if (dialect.migration.dropIndexStyle === 'dropIndexOnTable') {
      return [
        {
          text:
            'drop index ' +
            quoteIdentifier(dialect, operation.name) +
            ' on ' +
            quoteTableRef(dialect, operation.table),
          values: [],
        },
      ]
    }

    return [
      {
        text:
          'drop index ' +
          (operation.ifExists ? 'if exists ' : '') +
          quoteIdentifier(dialect, operation.name),
        values: [],
      },
    ]
  }

  if (operation.kind === 'renameIndex') {
    if (dialect.migration.renameIndexStyle === 'alterIndexRename') {
      return [
        {
          text:
            'alter index ' +
            quoteIdentifier(dialect, operation.from) +
            ' rename to ' +
            quoteIdentifier(dialect, operation.to),
          values: [],
        },
      ]
    }

    return [
      {
        text:
          'alter table ' +
          quoteTableRef(dialect, operation.table) +
          ' rename index ' +
          quoteIdentifier(dialect, operation.from) +
          ' to ' +
          quoteIdentifier(dialect, operation.to),
        values: [],
      },
    ]
  }

  if (operation.kind === 'addForeignKey') {
    return [
      {
        text:
          'alter table ' +
          quoteTableRef(dialect, operation.table) +
          ' add ' +
          (operation.constraint.name
            ? 'constraint ' + quoteIdentifier(dialect, operation.constraint.name) + ' '
            : '') +
          'foreign key (' +
          operation.constraint.columns.map((column) => quoteIdentifier(dialect, column)).join(', ') +
          ') references ' +
          quoteTableRef(dialect, operation.constraint.references.table) +
          ' (' +
          operation.constraint.references.columns
            .map((column) => quoteIdentifier(dialect, column))
            .join(', ') +
          ')' +
          (operation.constraint.onDelete ? ' on delete ' + operation.constraint.onDelete : '') +
          (operation.constraint.onUpdate ? ' on update ' + operation.constraint.onUpdate : ''),
        values: [],
      },
    ]
  }

  if (operation.kind === 'dropForeignKey') {
    return [
      {
        text:
          'alter table ' +
          quoteTableRef(dialect, operation.table) +
          (dialect.migration.dropForeignKeyStyle === 'dropForeignKey'
            ? ' drop foreign key '
            : ' drop constraint ') +
          quoteIdentifier(dialect, operation.name),
        values: [],
      },
    ]
  }

  if (operation.kind === 'addCheck') {
    return [
      {
        text:
          'alter table ' +
          quoteTableRef(dialect, operation.table) +
          ' add ' +
          (operation.constraint.name
            ? 'constraint ' + quoteIdentifier(dialect, operation.constraint.name) + ' '
            : '') +
          'check (' +
          operation.constraint.expression +
          ')',
        values: [],
      },
    ]
  }

  if (operation.kind === 'dropCheck') {
    return [
      {
        text:
          'alter table ' +
          quoteTableRef(dialect, operation.table) +
          (dialect.migration.dropCheckStyle === 'dropCheck' ? ' drop check ' : ' drop constraint ') +
          quoteIdentifier(dialect, operation.name),
        values: [],
      },
    ]
  }

  throw new Error('Unsupported data migration operation kind')
}

function compileColumn(dialect: SqlCompilerDialect, definition: ColumnDefinition): string {
  let parts = [compileColumnType(dialect, definition)]

  if (definition.nullable === false) {
    parts.push('not null')
  }

  if (definition.default) {
    if (definition.default.kind === 'now') {
      parts.push('default ' + dialect.nowExpression)
    } else if (definition.default.kind === 'sql') {
      parts.push('default ' + definition.default.expression)
    } else {
      parts.push('default ' + quoteLiteral(dialect, definition.default.value))
    }
  }

  if (dialect.migration.changeColumnStyle === 'modifyColumn' && definition.autoIncrement) {
    parts.push('auto_increment')
  }

  if (definition.primaryKey) {
    parts.push('primary key')
  }

  if (definition.unique) {
    parts.push('unique')
  }

  if (definition.computed) {
    if (dialect.migration.computedColumnStyle === 'storedOnly') {
      if (!definition.computed.stored) {
        throw new Error(dialect.migration.computedStoredOnlyError)
      }

      parts.push('generated always as (' + definition.computed.expression + ') stored')
    } else {
      parts.push('generated always as (' + definition.computed.expression + ')')
      parts.push(definition.computed.stored ? 'stored' : dialect.migration.virtualComputedKeyword)
    }
  }

  if (definition.references) {
    let clause =
      'references ' +
      quoteTableRef(dialect, definition.references.table) +
      ' (' +
      definition.references.columns.map((column) => quoteIdentifier(dialect, column)).join(', ') +
      ')'

    if (definition.references.onDelete) {
      clause += ' on delete ' + definition.references.onDelete
    }

    if (definition.references.onUpdate) {
      clause += ' on update ' + definition.references.onUpdate
    }

    parts.push(clause)
  }

  if (definition.checks && definition.checks.length > 0) {
    for (let check of definition.checks) {
      parts.push('check (' + check.expression + ')')
    }
  }

  return parts.join(' ')
}

function compileColumnType(dialect: SqlCompilerDialect, definition: ColumnDefinition): string {
  return dialect.compileColumnType(definition, {
    quoteLiteral(value) {
      return quoteLiteral(dialect, value)
    },
  })
}

function quoteIdentifier(dialect: SqlCompilerDialect, value: string): string {
  return dialect.quoteIdentifier(value)
}

function quotePath(dialect: SqlCompilerDialect, path: string): string {
  if (path === '*') {
    return '*'
  }

  return path
    .split('.')
    .map((segment) => {
      if (segment === '*') {
        return '*'
      }

      return quoteIdentifier(dialect, segment)
    })
    .join('.')
}

function quoteTableRef(dialect: SqlCompilerDialect, table: TableRef): string {
  if (table.schema) {
    return quoteIdentifier(dialect, table.schema) + '.' + quoteIdentifier(dialect, table.name)
  }

  return quoteIdentifier(dialect, table.name)
}

function quoteLiteral(dialect: SqlCompilerDialect, value: unknown): string {
  return dialect.quoteLiteral(value)
}

function compileTableComment(
  dialect: SqlCompilerDialect,
  table: TableRef,
  comment: string,
  style: SqlCompilerDialect['migration']['createTableCommentStyle'],
): SqlStatement | undefined {
  if (style === 'none') {
    return undefined
  }

  if (style === 'commentOnTable') {
    return {
      text: 'comment on table ' + quoteTableRef(dialect, table) + ' is ' + quoteLiteral(dialect, comment),
      values: [],
    }
  }

  return {
    text: 'alter table ' + quoteTableRef(dialect, table) + ' comment = ' + quoteLiteral(dialect, comment),
    values: [],
  }
}

function defaultIndexName(columns: string[]): string {
  return columns.join('_') + '_idx'
}

function isDataManipulationOperation(
  operation: DataManipulationOperation | DataMigrationOperation,
): operation is DataManipulationOperation {
  return (
    operation.kind === 'select' ||
    operation.kind === 'count' ||
    operation.kind === 'exists' ||
    operation.kind === 'insert' ||
    operation.kind === 'insertMany' ||
    operation.kind === 'update' ||
    operation.kind === 'delete' ||
    operation.kind === 'upsert' ||
    operation.kind === 'raw'
  )
}
