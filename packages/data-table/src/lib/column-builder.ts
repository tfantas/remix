import type {
  ColumnDefinition,
  ForeignKeyAction,
  IdentityOptions,
  TableRef,
} from './adapter.ts'

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

export class ColumnBuilder {
  #definition: ColumnDefinition

  constructor(definition: ColumnDefinition) {
    this.#definition = definition
  }

  nullable(): ColumnBuilder {
    this.#definition.nullable = true
    return this
  }

  notNull(): ColumnBuilder {
    this.#definition.nullable = false
    return this
  }

  default(value: unknown): ColumnBuilder {
    this.#definition.default = {
      kind: 'literal',
      value,
    }
    return this
  }

  defaultNow(): ColumnBuilder {
    this.#definition.default = {
      kind: 'now',
    }
    return this
  }

  defaultSql(expression: string): ColumnBuilder {
    this.#definition.default = {
      kind: 'sql',
      expression,
    }
    return this
  }

  primaryKey(): ColumnBuilder {
    this.#definition.primaryKey = true
    return this
  }

  unique(name?: string): ColumnBuilder {
    this.#definition.unique = name ? { name } : true
    return this
  }

  references(
    table: string,
    columns: string | string[] = 'id',
    options?: { name?: string },
  ): ColumnBuilder {
    this.#definition.references = {
      table: toTableRef(table),
      columns: Array.isArray(columns) ? [...columns] : [columns],
      onDelete: this.#definition.references?.onDelete,
      onUpdate: this.#definition.references?.onUpdate,
      name: options?.name ?? this.#definition.references?.name,
    }
    return this
  }

  onDelete(action: ForeignKeyAction): ColumnBuilder {
    if (!this.#definition.references) {
      throw new Error('onDelete() requires references() to be set first')
    }

    this.#definition.references.onDelete = action
    return this
  }

  onUpdate(action: ForeignKeyAction): ColumnBuilder {
    if (!this.#definition.references) {
      throw new Error('onUpdate() requires references() to be set first')
    }

    this.#definition.references.onUpdate = action
    return this
  }

  check(expression: string, name?: string): ColumnBuilder {
    let checks = this.#definition.checks ?? []

    checks.push({ expression, name })
    this.#definition.checks = checks

    return this
  }

  comment(text: string): ColumnBuilder {
    this.#definition.comment = text
    return this
  }

  computed(expression: string, options?: { stored?: boolean }): ColumnBuilder {
    this.#definition.computed = {
      expression,
      stored: options?.stored ?? true,
    }
    return this
  }

  unsigned(): ColumnBuilder {
    this.#definition.unsigned = true
    return this
  }

  autoIncrement(): ColumnBuilder {
    this.#definition.autoIncrement = true
    return this
  }

  identity(options?: IdentityOptions): ColumnBuilder {
    this.#definition.identity = options ?? {}
    return this
  }

  collate(name: string): ColumnBuilder {
    this.#definition.collate = name
    return this
  }

  charset(name: string): ColumnBuilder {
    this.#definition.charset = name
    return this
  }

  length(value: number): ColumnBuilder {
    this.#definition.length = value
    return this
  }

  precision(value: number, scale?: number): ColumnBuilder {
    this.#definition.precision = value

    if (scale !== undefined) {
      this.#definition.scale = scale
    }

    return this
  }

  scale(value: number): ColumnBuilder {
    this.#definition.scale = value
    return this
  }

  timezone(enabled = true): ColumnBuilder {
    this.#definition.withTimezone = enabled
    return this
  }

  build(): ColumnDefinition {
    return {
      ...this.#definition,
      checks: this.#definition.checks ? [...this.#definition.checks] : undefined,
    }
  }
}

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

function createColumnBuilder(type: ColumnDefinition['type']): ColumnBuilder {
  return new ColumnBuilder({ type })
}

export let column: ColumnNamespace = {
  varchar(length: number) {
    return new ColumnBuilder({ type: 'varchar', length })
  },
  text() {
    return createColumnBuilder('text')
  },
  integer() {
    return createColumnBuilder('integer')
  },
  bigint() {
    return createColumnBuilder('bigint')
  },
  decimal(precision: number, scale: number) {
    return new ColumnBuilder({ type: 'decimal', precision, scale })
  },
  boolean() {
    return createColumnBuilder('boolean')
  },
  uuid() {
    return createColumnBuilder('uuid')
  },
  date() {
    return createColumnBuilder('date')
  },
  time(options?: { precision?: number; withTimezone?: boolean }) {
    return new ColumnBuilder({
      type: 'time',
      precision: options?.precision,
      withTimezone: options?.withTimezone,
    })
  },
  timestamp(options?: { precision?: number; withTimezone?: boolean }) {
    return new ColumnBuilder({
      type: 'timestamp',
      precision: options?.precision,
      withTimezone: options?.withTimezone,
    })
  },
  json() {
    return createColumnBuilder('json')
  },
  binary(length?: number) {
    return new ColumnBuilder({ type: 'binary', length })
  },
  enum(values: readonly string[]) {
    return new ColumnBuilder({ type: 'enum', enumValues: [...values] })
  },
}
