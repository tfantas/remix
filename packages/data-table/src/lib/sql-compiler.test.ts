import * as assert from 'node:assert/strict'
import { describe, it } from 'node:test'
import { boolean, number, string } from '@remix-run/data-schema'

import type { DataManipulationOperation, DataMigrationOperation } from './adapter.ts'
import {
  and,
  between,
  eq,
  gt,
  gte,
  ilike,
  inList,
  isNull,
  like,
  lt,
  lte,
  ne,
  notInList,
  notNull,
  or,
} from './operators.ts'
import {
  compileDataManipulationOperation,
  compileDataMigrationOperations,
  compileOperationToSql,
} from './sql-compiler.ts'
import {
  mysqlCompilerOptions,
  postgresCompilerOptions,
  sqliteCompilerOptions,
  sqliteCompilerOptionsWithRewrite,
} from './sql-compiler-test-dialects.ts'
import { createTable } from './table.ts'

let accounts = createTable({
  name: 'accounts',
  columns: {
    id: number(),
    email: string(),
    status: string(),
    is_admin: boolean(),
    deleted_at: string(),
  },
})

let projects = createTable({
  name: 'projects',
  columns: {
    id: number(),
    account_id: number(),
    name: string(),
  },
})

let accountProjects = createTable({
  name: 'account_projects',
  columns: {
    account_id: number(),
    project_id: number(),
  },
  primaryKey: ['account_id', 'project_id'],
})

describe('shared sql compiler', () => {
  describe('data manipulation', () => {
    it('compiles complex postgres select operations', () => {
      let operation: DataManipulationOperation = {
        kind: 'select',
        table: accounts,
        select: [
          { column: 'accounts.id', alias: 'id' },
          { column: 'accounts.*', alias: 'all_accounts' },
        ],
        distinct: true,
        joins: [
          {
            type: 'right',
            table: projects,
            on: eq('accounts.id', 'projects.account_id'),
          },
          {
            type: 'unknown' as any,
            table: projects,
            on: eq('accounts.id', 'projects.account_id'),
          },
        ],
        where: [
          eq('status', null),
          ne('deleted_at', undefined),
          ne('status', 'disabled'),
          gt('id', 10),
          gte('id', 11),
          lte('id', 20),
          lt('id', 50),
          inList('id', [1, 2]),
          notInList('id', []),
          like('email', '%@example.com'),
          ilike('email', '%@example.com'),
          between('id', 1, 10),
          isNull('deleted_at'),
          notNull('email'),
          and(),
          or(),
          and(eq('status', 'enabled'), or(eq('status', 'pending'), eq('status', 'disabled'))),
        ],
        groupBy: ['accounts.id'],
        having: [eq('accounts.id', 1)],
        orderBy: [{ column: 'accounts.id', direction: 'desc' }],
        limit: 10,
        offset: 5,
      }

      let compiled = compileDataManipulationOperation(operation, postgresCompilerOptions)

      assert.match(compiled.text, /^select distinct /)
      assert.match(compiled.text, /"accounts"\.\* as "all_accounts"/)
      assert.match(compiled.text, /right join "projects" on "accounts"\."id" = "projects"\."account_id"/)
      assert.match(compiled.text, /inner join "projects" on "accounts"\."id" = "projects"\."account_id"/)
      assert.match(compiled.text, /"status" is null/)
      assert.match(compiled.text, /"deleted_at" is not null/)
      assert.match(compiled.text, /"status" <> \$1/)
      assert.match(compiled.text, /"id" > \$2/)
      assert.match(compiled.text, /"id" >= \$3/)
      assert.match(compiled.text, /"id" <= \$4/)
      assert.match(compiled.text, /"id" < \$5/)
      assert.match(compiled.text, /"id" in \(\$6, \$7\)/)
      assert.match(compiled.text, /1 = 1/)
      assert.match(compiled.text, /"email" like \$8/)
      assert.match(compiled.text, /"email" ilike \$9/)
      assert.match(compiled.text, /"id" between \$10 and \$11/)
      assert.match(compiled.text, /group by "accounts"\."id"/)
      assert.match(compiled.text, /having \("accounts"\."id" = \$15\)/)
      assert.match(compiled.text, /order by "accounts"\."id" DESC limit 10 offset 5$/)
      assert.deepEqual(compiled.values, [
        'disabled',
        10,
        11,
        20,
        50,
        1,
        2,
        '%@example.com',
        '%@example.com',
        1,
        10,
        'enabled',
        'pending',
        'disabled',
        1,
      ])
    })

    it('compiles select operations with empty where/group/having/order clauses', () => {
      let operation: DataManipulationOperation = {
        kind: 'select',
        table: accounts,
        select: '*',
        distinct: false,
        joins: [],
        where: [],
        groupBy: [],
        having: [],
        orderBy: [],
      }

      let compiled = compileDataManipulationOperation(operation, postgresCompilerOptions)

      assert.deepEqual(compiled, {
        text: 'select * from "accounts"',
        values: [],
      })
    })

    it('compiles mysql ilike predicates with lower() fallback', () => {
      let operation: DataManipulationOperation = {
        kind: 'select',
        table: accounts,
        select: '*',
        distinct: false,
        joins: [],
        where: [ilike('email', '%@example.com')],
        groupBy: [],
        having: [],
        orderBy: [],
      }

      let compiled = compileDataManipulationOperation(operation, mysqlCompilerOptions)

      assert.equal(compiled.text, 'select * from `accounts` where (lower(`email`) like lower(?))')
      assert.deepEqual(compiled.values, ['%@example.com'])
    })

    it('normalizes sqlite boolean bound values', () => {
      let operation: DataManipulationOperation = {
        kind: 'update',
        table: accounts,
        changes: {
          is_admin: true,
        },
        where: [eq('id', 123), eq('is_admin', false)],
        returning: ['id'],
      }

      let compiled = compileDataManipulationOperation(operation, sqliteCompilerOptions)

      assert.equal(
        compiled.text,
        'update "accounts" set "is_admin" = ? where ("id" = ?) and ("is_admin" = ?) returning "id"',
      )
      assert.deepEqual(compiled.values, [1, 123, 0])
    })

    it('compiles count and exists wrappers', () => {
      let countOperation: DataManipulationOperation = {
        kind: 'count',
        table: accounts,
        joins: [
          {
            type: 'left',
            table: projects,
            on: eq('accounts.id', 'projects.account_id'),
          },
        ],
        where: [eq('status', 'enabled')],
        groupBy: ['accounts.id'],
        having: [eq('accounts.id', 1)],
      }

      let existsOperation: DataManipulationOperation = {
        kind: 'exists',
        table: accounts,
        joins: [],
        where: [eq('status', 'enabled')],
        groupBy: [],
        having: [],
      }

      let countCompiled = compileDataManipulationOperation(countOperation, postgresCompilerOptions)
      let existsCompiled = compileDataManipulationOperation(existsOperation, mysqlCompilerOptions)

      assert.match(countCompiled.text, /^select count\(\*\) as "count" from \(select 1 from "accounts"/)
      assert.match(countCompiled.text, /group by "accounts"\."id" having \("accounts"\."id" = \$2\)/)
      assert.deepEqual(countCompiled.values, ['enabled', 1])

      assert.equal(
        existsCompiled.text,
        'select count(*) as `count` from (select 1 from `accounts` where (`status` = ?)) as `__dt_count`',
      )
      assert.deepEqual(existsCompiled.values, ['enabled'])
    })

    it('compiles insert variants', () => {
      let insertOperation: DataManipulationOperation = {
        kind: 'insert',
        table: accounts,
        values: {
          id: 1,
          email: 'a@example.com',
        },
        returning: '*',
      }

      let insertDefaultPostgres: DataManipulationOperation = {
        kind: 'insert',
        table: accounts,
        values: {},
        returning: ['id'],
      }

      let insertDefaultMysql: DataManipulationOperation = {
        kind: 'insert',
        table: accounts,
        values: {},
        returning: ['id'],
      }

      let postgresInsert = compileDataManipulationOperation(insertOperation, postgresCompilerOptions)
      let postgresDefaultInsert = compileDataManipulationOperation(
        insertDefaultPostgres,
        postgresCompilerOptions,
      )
      let mysqlDefaultInsert = compileDataManipulationOperation(
        insertDefaultMysql,
        mysqlCompilerOptions,
      )

      assert.deepEqual(postgresInsert, {
        text: 'insert into "accounts" ("id", "email") values ($1, $2) returning *',
        values: [1, 'a@example.com'],
      })

      assert.deepEqual(postgresDefaultInsert, {
        text: 'insert into "accounts" default values returning "id"',
        values: [],
      })

      assert.deepEqual(mysqlDefaultInsert, {
        text: 'insert into `accounts` () values ()',
        values: [],
      })
    })

    it('compiles insertMany variants', () => {
      let emptyOperation: DataManipulationOperation = {
        kind: 'insertMany',
        table: accounts,
        values: [],
      }

      let defaultValuesOperation: DataManipulationOperation = {
        kind: 'insertMany',
        table: accounts,
        values: [{}, {}],
        returning: ['id'],
      }

      let sparseRowsOperation: DataManipulationOperation = {
        kind: 'insertMany',
        table: accounts,
        values: [{ id: 1 }, { email: 'a@example.com' }],
        returning: ['id'],
      }

      let inheritedRow = Object.create({ inherited_only: 'skip me' }) as Record<string, unknown>
      inheritedRow.id = 9

      let duplicateColumnRowsOperation: DataManipulationOperation = {
        kind: 'insertMany',
        table: accounts,
        values: [inheritedRow, { id: 10, email: 'b@example.com' }],
      }

      let emptyCompiled = compileDataManipulationOperation(emptyOperation, postgresCompilerOptions)
      let defaultCompiled = compileDataManipulationOperation(
        defaultValuesOperation,
        postgresCompilerOptions,
      )
      let sparseCompiled = compileDataManipulationOperation(sparseRowsOperation, sqliteCompilerOptions)
      let duplicateColumnRowsCompiled = compileDataManipulationOperation(
        duplicateColumnRowsOperation,
        mysqlCompilerOptions,
      )

      assert.deepEqual(emptyCompiled, {
        text: 'select 0 where 1 = 0',
        values: [],
      })

      assert.deepEqual(defaultCompiled, {
        text: 'insert into "accounts" default values returning "id"',
        values: [],
      })

      assert.equal(
        sparseCompiled.text,
        'insert into "accounts" ("id", "email") values (?, ?), (?, ?) returning "id"',
      )
      assert.deepEqual(sparseCompiled.values, [1, null, null, 'a@example.com'])

      assert.equal(
        duplicateColumnRowsCompiled.text,
        'insert into `accounts` (`id`, `email`) values (?, ?), (?, ?)',
      )
      assert.deepEqual(duplicateColumnRowsCompiled.values, [9, null, 10, 'b@example.com'])
    })

    it('compiles update and delete with dialect-aware returning behavior', () => {
      let updateOperation: DataManipulationOperation = {
        kind: 'update',
        table: accounts,
        changes: {
          email: 'next@example.com',
        },
        where: [eq('id', 123)],
        returning: ['id', 'email'],
      }

      let deleteOperation: DataManipulationOperation = {
        kind: 'delete',
        table: accounts,
        where: [eq('id', 123)],
        returning: '*',
      }

      let postgresUpdate = compileDataManipulationOperation(updateOperation, postgresCompilerOptions)
      let mysqlUpdate = compileDataManipulationOperation(updateOperation, mysqlCompilerOptions)
      let postgresDelete = compileDataManipulationOperation(deleteOperation, postgresCompilerOptions)
      let postgresUpdateWithoutReturning = compileDataManipulationOperation(
        {
          kind: 'update',
          table: accounts,
          changes: { status: 'enabled' },
          where: [],
        },
        postgresCompilerOptions,
      )
      let postgresDeleteReturningWildcardPath = compileDataManipulationOperation(
        {
          kind: 'delete',
          table: accounts,
          where: [eq('id', 123)],
          returning: ['*'],
        },
        postgresCompilerOptions,
      )

      assert.equal(
        postgresUpdate.text,
        'update "accounts" set "email" = $1 where ("id" = $2) returning "id", "email"',
      )
      assert.equal(mysqlUpdate.text, 'update `accounts` set `email` = ? where (`id` = ?)')
      assert.equal(postgresDelete.text, 'delete from "accounts" where ("id" = $1) returning *')
      assert.equal(postgresUpdateWithoutReturning.text, 'update "accounts" set "status" = $1')
      assert.equal(
        postgresDeleteReturningWildcardPath.text,
        'delete from "accounts" where ("id" = $1) returning *',
      )
    })

    it('compiles mysql upsert variants', () => {
      let updateOperation: DataManipulationOperation = {
        kind: 'upsert',
        table: accounts,
        values: {
          id: 1,
          email: 'a@example.com',
        },
      }

      let noOpOperation: DataManipulationOperation = {
        kind: 'upsert',
        table: accounts,
        values: {
          id: 2,
          email: 'b@example.com',
        },
        update: {},
      }

      let compiledWithUpdates = compileDataManipulationOperation(updateOperation, mysqlCompilerOptions)
      let compiledNoOp = compileDataManipulationOperation(noOpOperation, mysqlCompilerOptions)

      assert.equal(
        compiledWithUpdates.text,
        'insert into `accounts` (`id`, `email`) values (?, ?) on duplicate key update `id` = ?, `email` = ?',
      )
      assert.deepEqual(compiledWithUpdates.values, [1, 'a@example.com', 1, 'a@example.com'])

      assert.equal(
        compiledNoOp.text,
        'insert into `accounts` (`id`, `email`) values (?, ?) on duplicate key update `id` = `id`',
      )
      assert.deepEqual(compiledNoOp.values, [2, 'b@example.com'])
    })

    it('compiles postgres upsert variants', () => {
      let doNothingOperation: DataManipulationOperation = {
        kind: 'upsert',
        table: accounts,
        values: {
          id: 1,
          email: 'a@example.com',
        },
        update: {},
        returning: ['id'],
      }

      let doUpdateOperation: DataManipulationOperation = {
        kind: 'upsert',
        table: accounts,
        values: {
          id: 2,
          email: 'b@example.com',
        },
        conflictTarget: ['email'],
        update: {
          email: 'updated@example.com',
        },
        returning: '*',
      }

      let doNothing = compileDataManipulationOperation(doNothingOperation, postgresCompilerOptions)
      let doUpdate = compileDataManipulationOperation(doUpdateOperation, postgresCompilerOptions)

      assert.equal(
        doNothing.text,
        'insert into "accounts" ("id", "email") values ($1, $2) on conflict ("id") do nothing returning "id"',
      )
      assert.deepEqual(doNothing.values, [1, 'a@example.com'])

      assert.equal(
        doUpdate.text,
        'insert into "accounts" ("id", "email") values ($1, $2) on conflict ("email") do update set "email" = $3 returning *',
      )
      assert.deepEqual(doUpdate.values, [2, 'b@example.com', 'updated@example.com'])
    })

    it('throws for invalid upsert operations without values', () => {
      let operation: DataManipulationOperation = {
        kind: 'upsert',
        table: accounts,
        values: {},
      }

      assert.throws(
        () => compileDataManipulationOperation(operation, postgresCompilerOptions),
        /upsert requires at least one value/,
      )
    })

    it('converts postgres raw placeholders and preserves other dialect raw SQL', () => {
      let operation: DataManipulationOperation = {
        kind: 'raw',
        sql: {
          text: 'select ? as a, ? as b',
          values: [1, 2],
        },
      }

      let postgresCompiled = compileDataManipulationOperation(operation, postgresCompilerOptions)
      let mysqlCompiled = compileDataManipulationOperation(operation, mysqlCompilerOptions)

      assert.deepEqual(postgresCompiled, {
        text: 'select $1 as a, $2 as b',
        values: [1, 2],
      })

      assert.deepEqual(mysqlCompiled, {
        text: 'select ? as a, ? as b',
        values: [1, 2],
      })

      let postgresWithoutPlaceholders = compileDataManipulationOperation(
        {
          kind: 'raw',
          sql: {
            text: 'select now()',
            values: [],
          },
        },
        postgresCompilerOptions,
      )

      assert.deepEqual(postgresWithoutPlaceholders, {
        text: 'select now()',
        values: [],
      })
    })

    it('throws for unsupported predicate and statement kinds', () => {
      assert.throws(
        () =>
          compileDataManipulationOperation(
            {
              kind: 'select',
              table: accounts,
              select: '*',
              distinct: false,
              joins: [],
              where: [{ type: 'mystery' } as any],
              groupBy: [],
              having: [],
              orderBy: [],
            },
            postgresCompilerOptions,
          ),
        /Unsupported predicate/,
      )

      assert.throws(
        () => compileDataManipulationOperation({ kind: 'mystery' } as any, postgresCompilerOptions),
        /Unsupported statement kind/,
      )
    })
  })

  describe('data migration', () => {
    it('compiles rich postgres createTable operations', () => {
      let operation: DataMigrationOperation = {
        kind: 'createTable',
        table: { schema: 'app', name: 'users' },
        ifNotExists: true,
        columns: {
          id: { type: 'integer', nullable: false, primaryKey: true },
          public_id: { type: 'uuid' },
          email: { type: 'varchar', length: 320, nullable: false, unique: true },
          visits: { type: 'integer', default: { kind: 'literal', value: 0 } },
          big_visits: { type: 'bigint', default: { kind: 'literal', value: 12n } },
          is_admin: { type: 'boolean', default: { kind: 'literal', value: true } },
          nickname: { type: 'text', default: { kind: 'literal', value: null } },
          safe_slug: { type: 'text', default: { kind: 'sql', expression: 'md5(email)' } },
          created_at: { type: 'timestamp', withTimezone: true, default: { kind: 'now' } },
          birthday: {
            type: 'date',
            default: { kind: 'literal', value: new Date('2024-01-02T00:00:00.000Z') },
          },
          starts_at: { type: 'time', withTimezone: true },
          plain_time: { type: 'time', withTimezone: false },
          metadata: { type: 'json' },
          blob: { type: 'binary' },
          role: { type: 'enum', enumValues: ['admin', 'user'] },
          score: { type: 'decimal', precision: 10, scale: 2 },
          ratio: { type: 'decimal', precision: 10 },
          manager_id: {
            type: 'integer',
            references: {
              table: { schema: 'app', name: 'users' },
              columns: ['id'],
              onDelete: 'set null',
              onUpdate: 'cascade',
            },
          },
          name: { type: 'text', checks: [{ expression: 'length(name) > 1', name: 'users_name_len' }] },
          full_name: {
            type: 'text',
            computed: { expression: `first_name || ' ' || last_name`, stored: true },
          },
          fallback: { type: 'mystery' as any },
        },
        primaryKey: { columns: ['id'] },
        uniques: [{ columns: ['email'] }, { name: 'users_role_unique', columns: ['role'] }],
        checks: [{ expression: 'id > 0' }, { name: 'users_id_check', expression: 'id > 0' }],
        foreignKeys: [
          {
            name: 'users_account_fk',
            columns: ['id'],
            references: { table: { name: 'accounts' }, columns: ['id'] },
            onDelete: 'cascade',
            onUpdate: 'restrict',
          },
        ],
        comment: `owner's table`,
      }

      let statements = compileDataMigrationOperations(operation, postgresCompilerOptions)

      assert.equal(statements.length, 2)
      assert.match(statements[0].text, /^create table if not exists "app"\."users" \(/)
      assert.match(statements[0].text, /"email" varchar\(320\) not null unique/)
      assert.match(statements[0].text, /"public_id" uuid/)
      assert.match(statements[0].text, /"visits" integer default 0/)
      assert.match(statements[0].text, /"big_visits" bigint default 12/)
      assert.match(statements[0].text, /"is_admin" boolean default true/)
      assert.match(statements[0].text, /"nickname" text default null/)
      assert.match(statements[0].text, /"safe_slug" text default md5\(email\)/)
      assert.match(statements[0].text, /"created_at" timestamp with time zone default now\(\)/)
      assert.match(statements[0].text, /"birthday" date default '2024-01-02T00:00:00\.000Z'/)
      assert.match(statements[0].text, /"starts_at" time with time zone/)
      assert.match(statements[0].text, /"plain_time" time without time zone/)
      assert.match(statements[0].text, /"metadata" jsonb/)
      assert.match(statements[0].text, /"blob" bytea/)
      assert.match(statements[0].text, /"role" text/)
      assert.match(statements[0].text, /"score" decimal\(10, 2\)/)
      assert.match(statements[0].text, /"ratio" decimal/)
      assert.match(
        statements[0].text,
        /"manager_id" integer references "app"\."users" \("id"\) on delete set null on update cascade/,
      )
      assert.match(statements[0].text, /"name" text check \(length\(name\) > 1\)/)
      assert.match(
        statements[0].text,
        /"full_name" text generated always as \(first_name \|\| ' ' \|\| last_name\) stored/,
      )
      assert.match(statements[0].text, /"fallback" text/)
      assert.match(statements[0].text, /primary key \("id"\)/)
      assert.match(statements[0].text, /unique \("email"\)/)
      assert.match(statements[0].text, /constraint "users_role_unique" unique \("role"\)/)
      assert.match(statements[0].text, /check \(id > 0\)/)
      assert.match(statements[0].text, /constraint "users_id_check" check \(id > 0\)/)
      assert.match(
        statements[0].text,
        /constraint "users_account_fk" foreign key \("id"\) references "accounts" \("id"\) on delete cascade on update restrict/,
      )
      assert.equal(statements[1].text, `comment on table "app"."users" is 'owner''s table'`)
    })

    it('compiles mysql and sqlite createTable dialect differences', () => {
      let operation: DataMigrationOperation = {
        kind: 'createTable',
        table: { name: 'widgets' },
        ifNotExists: true,
        columns: {
          id: { type: 'integer', autoIncrement: true, primaryKey: true, unsigned: true },
          name: { type: 'varchar', length: 80 },
          description: { type: 'text' },
          uuid: { type: 'uuid' },
          payload: { type: 'json' },
          bytes: { type: 'binary' },
          big_total: { type: 'bigint', unsigned: true },
          published_on: { type: 'date' },
          starts_at: { type: 'time' },
          status: { type: 'enum', enumValues: ['active', 'disabled'] },
          fallback_status: { type: 'enum', enumValues: [] },
          score: { type: 'decimal', precision: 10, scale: 2 },
          plain_decimal: { type: 'decimal' },
          when_at: { type: 'timestamp', default: { kind: 'now' } },
          derived_score: {
            type: 'integer',
            computed: { expression: '(points + 1)', stored: false },
          },
          flagged: { type: 'boolean', default: { kind: 'literal', value: false } },
          fallback_type: { type: 'mystery' as any },
        },
        comment: 'widgets table',
      }

      let mysqlStatements = compileDataMigrationOperations(operation, mysqlCompilerOptions)
      let sqliteStatements = compileDataMigrationOperations(operation, sqliteCompilerOptions)

      assert.equal(mysqlStatements.length, 2)
      assert.match(mysqlStatements[0].text, /`id` int unsigned auto_increment primary key/)
      assert.match(mysqlStatements[0].text, /`name` varchar\(80\)/)
      assert.match(mysqlStatements[0].text, /`description` text/)
      assert.match(mysqlStatements[0].text, /`uuid` char\(36\)/)
      assert.match(mysqlStatements[0].text, /`payload` json/)
      assert.match(mysqlStatements[0].text, /`bytes` blob/)
      assert.match(mysqlStatements[0].text, /`big_total` bigint unsigned/)
      assert.match(mysqlStatements[0].text, /`published_on` date/)
      assert.match(mysqlStatements[0].text, /`starts_at` time/)
      assert.match(mysqlStatements[0].text, /`status` enum\('active', 'disabled'\)/)
      assert.match(mysqlStatements[0].text, /`fallback_status` text/)
      assert.match(mysqlStatements[0].text, /`score` decimal\(10, 2\)/)
      assert.match(mysqlStatements[0].text, /`plain_decimal` decimal/)
      assert.match(mysqlStatements[0].text, /`when_at` timestamp default current_timestamp/)
      assert.match(mysqlStatements[0].text, /`derived_score` int generated always as \(\(points \+ 1\)\) virtual/)
      assert.match(mysqlStatements[0].text, /`flagged` boolean default false/)
      assert.match(mysqlStatements[0].text, /`fallback_type` text/)
      assert.equal(mysqlStatements[1].text, "alter table `widgets` comment = 'widgets table'")

      assert.equal(sqliteStatements.length, 1)
      assert.match(sqliteStatements[0].text, /^create table if not exists "widgets" \(/)
      assert.match(sqliteStatements[0].text, /"id" integer primary key/)
      assert.match(sqliteStatements[0].text, /"name" text/)
      assert.match(sqliteStatements[0].text, /"description" text/)
      assert.match(sqliteStatements[0].text, /"uuid" text/)
      assert.match(sqliteStatements[0].text, /"payload" text/)
      assert.match(sqliteStatements[0].text, /"bytes" blob/)
      assert.match(sqliteStatements[0].text, /"big_total" integer/)
      assert.match(sqliteStatements[0].text, /"published_on" text/)
      assert.match(sqliteStatements[0].text, /"starts_at" text/)
      assert.match(sqliteStatements[0].text, /"score" numeric/)
      assert.match(sqliteStatements[0].text, /"derived_score" integer generated always as \(\(points \+ 1\)\) virtual/)
      assert.match(sqliteStatements[0].text, /"flagged" integer default 0/)
      assert.match(sqliteStatements[0].text, /"fallback_type" text/)
    })

    it('throws when postgres computed columns are not stored', () => {
      let operation: DataMigrationOperation = {
        kind: 'createTable',
        table: { name: 'users' },
        columns: {
          full_name: {
            type: 'text',
            computed: { expression: `first_name || ' ' || last_name`, stored: false },
          },
        },
      }

      assert.throws(
        () => compileDataMigrationOperations(operation, postgresCompilerOptions),
        /Postgres only supports stored computed\/generated columns/,
      )
    })

    it('compiles alterTable changes across dialects', () => {
      let postgresOperation: DataMigrationOperation = {
        kind: 'alterTable',
        table: { schema: 'app', name: 'users' },
        changes: [
          { kind: 'addColumn', column: 'email', definition: { type: 'text', nullable: false } },
          { kind: 'changeColumn', column: 'email', definition: { type: 'varchar', length: 255 } },
          { kind: 'renameColumn', from: 'email', to: 'contact_email' },
          { kind: 'dropColumn', column: 'legacy_email', ifExists: true },
          { kind: 'addPrimaryKey', constraint: { columns: ['id'], name: 'users_pk' } },
          { kind: 'dropPrimaryKey' },
          { kind: 'addUnique', constraint: { columns: ['contact_email'], name: 'users_email_unique' } },
          { kind: 'dropUnique', name: 'users_email_unique' },
          {
            kind: 'addForeignKey',
            constraint: {
              name: 'users_account_fk',
              columns: ['account_id'],
              references: { table: { name: 'accounts' }, columns: ['id'] },
            },
          },
          { kind: 'dropForeignKey', name: 'users_account_fk' },
          { kind: 'addCheck', constraint: { name: 'users_status_check', expression: "status <> 'deleted'" } },
          { kind: 'dropCheck', name: 'users_status_check' },
          { kind: 'setTableComment', comment: 'updated users table' },
          { kind: 'unknown_change' as any },
        ],
      }

      let mysqlOperation: DataMigrationOperation = {
        kind: 'alterTable',
        table: { name: 'users' },
        changes: [
          { kind: 'addPrimaryKey', constraint: { columns: ['id'] } },
          { kind: 'dropPrimaryKey' },
          { kind: 'changeColumn', column: 'email', definition: { type: 'varchar', length: 191 } },
          { kind: 'dropUnique', name: 'users_email_unique' },
          { kind: 'dropForeignKey', name: 'users_account_fk' },
          { kind: 'dropCheck', name: 'users_email_check' },
          { kind: 'setTableComment', comment: `owner's users` },
        ],
      }

      let sqliteOperation: DataMigrationOperation = {
        kind: 'alterTable',
        table: { name: 'users' },
        changes: [{ kind: 'setTableComment', comment: 'ignored' }],
      }

      let postgresStatements = compileDataMigrationOperations(
        postgresOperation,
        postgresCompilerOptions,
      )
      let mysqlStatements = compileDataMigrationOperations(mysqlOperation, mysqlCompilerOptions)
      let sqliteStatements = compileDataMigrationOperations(sqliteOperation, sqliteCompilerOptions)

      assert.equal(postgresStatements.length, 13)
      assert.equal(
        postgresStatements[1].text,
        'alter table "app"."users" alter column "email" type varchar(255)',
      )
      assert.equal(
        postgresStatements[5].text,
        'alter table "app"."users" drop constraint "PRIMARY"',
      )
      assert.equal(
        postgresStatements[12].text,
        'comment on table "app"."users" is \'updated users table\'',
      )

      assert.equal(mysqlStatements.length, 7)
      assert.equal(
        mysqlStatements[0].text,
        'alter table `users` add primary key (`id`)',
      )
      assert.equal(mysqlStatements[1].text, 'alter table `users` drop primary key')
      assert.equal(
        mysqlStatements[2].text,
        'alter table `users` modify column `email` varchar(191)',
      )
      assert.equal(mysqlStatements[3].text, 'alter table `users` drop index `users_email_unique`')
      assert.equal(mysqlStatements[4].text, 'alter table `users` drop foreign key `users_account_fk`')
      assert.equal(mysqlStatements[5].text, 'alter table `users` drop check `users_email_check`')
      assert.equal(mysqlStatements[6].text, "alter table `users` comment = 'owner''s users'")

      assert.deepEqual(sqliteStatements, [])
    })

    it('compiles table and index operations', () => {
      let postgresRename = compileDataMigrationOperations(
        {
          kind: 'renameTable',
          from: { schema: 'app', name: 'users' },
          to: { schema: 'app', name: 'accounts' },
        },
        postgresCompilerOptions,
      )

      let mysqlRename = compileDataMigrationOperations(
        {
          kind: 'renameTable',
          from: { name: 'users' },
          to: { name: 'accounts' },
        },
        mysqlCompilerOptions,
      )

      let postgresDrop = compileDataMigrationOperations(
        {
          kind: 'dropTable',
          table: { schema: 'app', name: 'accounts' },
          ifExists: true,
          cascade: true,
        },
        postgresCompilerOptions,
      )

      let mysqlDrop = compileDataMigrationOperations(
        {
          kind: 'dropTable',
          table: { name: 'accounts' },
          ifExists: true,
          cascade: true,
        },
        mysqlCompilerOptions,
      )

      let postgresCreateIndex = compileDataMigrationOperations(
        {
          kind: 'createIndex',
          ifNotExists: true,
          index: {
            table: { schema: 'app', name: 'users' },
            name: 'users_email_idx',
            columns: ['email'],
            using: 'btree',
            where: 'email is not null',
            unique: true,
          },
        },
        postgresCompilerOptions,
      )

      let mysqlCreateIndexDefaultName = compileDataMigrationOperations(
        {
          kind: 'createIndex',
          ifNotExists: true,
          index: {
            table: { name: 'users' },
            columns: ['email'],
          },
        },
        mysqlCompilerOptions,
      )

      let sqliteCreateIndex = compileDataMigrationOperations(
        {
          kind: 'createIndex',
          ifNotExists: true,
          index: {
            table: { name: 'users' },
            columns: ['email'],
          },
        },
        sqliteCompilerOptions,
      )

      let postgresDropIndex = compileDataMigrationOperations(
        {
          kind: 'dropIndex',
          table: { schema: 'app', name: 'users' },
          name: 'users_email_idx',
          ifExists: true,
        },
        postgresCompilerOptions,
      )

      let mysqlDropIndex = compileDataMigrationOperations(
        {
          kind: 'dropIndex',
          table: { name: 'users' },
          name: 'users_email_idx',
          ifExists: true,
        },
        mysqlCompilerOptions,
      )

      let postgresRenameIndex = compileDataMigrationOperations(
        {
          kind: 'renameIndex',
          table: { schema: 'app', name: 'users' },
          from: 'users_email_idx',
          to: 'users_contact_email_idx',
        },
        postgresCompilerOptions,
      )

      let mysqlRenameIndex = compileDataMigrationOperations(
        {
          kind: 'renameIndex',
          table: { name: 'users' },
          from: 'users_email_idx',
          to: 'users_contact_email_idx',
        },
        mysqlCompilerOptions,
      )

      assert.equal(
        postgresRename[0].text,
        'alter table "app"."users" rename to "accounts"',
      )
      assert.equal(mysqlRename[0].text, 'rename table `users` to `accounts`')
      assert.equal(postgresDrop[0].text, 'drop table if exists "app"."accounts" cascade')
      assert.equal(mysqlDrop[0].text, 'drop table if exists `accounts`')
      assert.equal(
        postgresCreateIndex[0].text,
        'create unique index if not exists "users_email_idx" on "app"."users" using btree ("email") where email is not null',
      )
      assert.equal(
        mysqlCreateIndexDefaultName[0].text,
        'create index `email_idx` on `users` (`email`)',
      )
      assert.equal(
        sqliteCreateIndex[0].text,
        'create index if not exists "email_idx" on "users" ("email")',
      )
      assert.equal(postgresDropIndex[0].text, 'drop index if exists "users_email_idx"')
      assert.equal(mysqlDropIndex[0].text, 'drop index `users_email_idx` on `users`')
      assert.equal(
        postgresRenameIndex[0].text,
        'alter index "users_email_idx" rename to "users_contact_email_idx"',
      )
      assert.equal(
        mysqlRenameIndex[0].text,
        'alter table `users` rename index `users_email_idx` to `users_contact_email_idx`',
      )
    })

    it('compiles top-level foreign key and check operations', () => {
      let addForeignKey = compileDataMigrationOperations(
        {
          kind: 'addForeignKey',
          table: { schema: 'app', name: 'users' },
          constraint: {
            name: 'users_account_fk',
            columns: ['account_id'],
            references: { table: { name: 'accounts' }, columns: ['id'] },
            onDelete: 'cascade',
            onUpdate: 'restrict',
          },
        },
        postgresCompilerOptions,
      )

      let dropForeignKeyMysql = compileDataMigrationOperations(
        {
          kind: 'dropForeignKey',
          table: { name: 'users' },
          name: 'users_account_fk',
        },
        mysqlCompilerOptions,
      )

      let dropForeignKeyPostgres = compileDataMigrationOperations(
        {
          kind: 'dropForeignKey',
          table: { name: 'users' },
          name: 'users_account_fk',
        },
        postgresCompilerOptions,
      )

      let addCheck = compileDataMigrationOperations(
        {
          kind: 'addCheck',
          table: { name: 'users' },
          constraint: {
            name: 'users_email_check',
            expression: 'char_length(email) > 3',
          },
        },
        sqliteCompilerOptions,
      )

      let dropCheckMysql = compileDataMigrationOperations(
        {
          kind: 'dropCheck',
          table: { name: 'users' },
          name: 'users_email_check',
        },
        mysqlCompilerOptions,
      )

      let dropCheckPostgres = compileDataMigrationOperations(
        {
          kind: 'dropCheck',
          table: { name: 'users' },
          name: 'users_email_check',
        },
        postgresCompilerOptions,
      )

      assert.equal(
        addForeignKey[0].text,
        'alter table "app"."users" add constraint "users_account_fk" foreign key ("account_id") references "accounts" ("id") on delete cascade on update restrict',
      )
      assert.equal(
        dropForeignKeyMysql[0].text,
        'alter table `users` drop foreign key `users_account_fk`',
      )
      assert.equal(
        dropForeignKeyPostgres[0].text,
        'alter table "users" drop constraint "users_account_fk"',
      )
      assert.equal(
        addCheck[0].text,
        'alter table "users" add constraint "users_email_check" check (char_length(email) > 3)',
      )
      assert.equal(dropCheckMysql[0].text, 'alter table `users` drop check `users_email_check`')
      assert.equal(
        dropCheckPostgres[0].text,
        'alter table "users" drop constraint "users_email_check"',
      )
    })

    it('handles raw and rewritten migration operations', () => {
      let rawOperation: DataMigrationOperation = {
        kind: 'raw',
        sql: {
          text: 'vacuum',
          values: [1],
        },
      }

      let rawCompiled = compileDataMigrationOperations(rawOperation, sqliteCompilerOptions)

      assert.deepEqual(rawCompiled, [{ text: 'vacuum', values: [1] }])

      let rewritten = compileDataMigrationOperations(
        {
          kind: 'alterTable',
          table: { name: 'users' },
          changes: [{ kind: 'setTableComment', comment: 'drop me' }],
        },
        sqliteCompilerOptionsWithRewrite((operation) => {
          if (operation.kind !== 'alterTable') {
            return [operation]
          }

          return []
        }),
      )

      assert.deepEqual(rewritten, [])
    })

    it('throws for unsupported migration operation kinds', () => {
      assert.throws(
        () => compileDataMigrationOperations({ kind: 'unsupported' } as any, postgresCompilerOptions),
        /Unsupported data migration operation kind/,
      )
    })
  })

  describe('operation router', () => {
    it('routes data-manipulation and data-migration operations through compileOperationToSql()', () => {
      let manipulation = compileOperationToSql(
        {
          kind: 'delete',
          table: accounts,
          where: [eq('id', 1)],
          returning: ['id'],
        },
        postgresCompilerOptions,
      )

      let migration = compileOperationToSql(
        {
          kind: 'dropTable',
          table: { name: 'users' },
          ifExists: true,
        },
        mysqlCompilerOptions,
      )

      assert.deepEqual(manipulation, [
        {
          text: 'delete from "accounts" where ("id" = $1) returning "id"',
          values: [1],
        },
      ])

      assert.deepEqual(migration, [
        {
          text: 'drop table if exists `users`',
          values: [],
        },
      ])
    })

    it('routes raw operations as data manipulation operations', () => {
      let rawManipulation = compileOperationToSql(
        {
          kind: 'raw',
          sql: {
            text: 'select ? as value',
            values: [1],
          },
        },
        postgresCompilerOptions,
      )

      assert.deepEqual(rawManipulation, [
        {
          text: 'select $1 as value',
          values: [1],
        },
      ])

      let migrationRaw = compileDataMigrationOperations(
        {
          kind: 'raw',
          sql: {
            text: 'pragma optimize',
            values: [],
          },
        },
        sqliteCompilerOptions,
      )

      assert.deepEqual(migrationRaw, [{ text: 'pragma optimize', values: [] }])
    })

    it('compiles mysql upsert no-op fallback for composite primary keys', () => {
      let operation: DataManipulationOperation = {
        kind: 'upsert',
        table: accountProjects,
        values: {
          account_id: 1,
          project_id: 2,
        },
        update: {},
      }

      let compiled = compileDataManipulationOperation(operation, mysqlCompilerOptions)

      assert.equal(
        compiled.text,
        'insert into `account_projects` (`account_id`, `project_id`) values (?, ?) on duplicate key update `account_id` = `account_id`',
      )
      assert.deepEqual(compiled.values, [1, 2])
    })
  })
})
