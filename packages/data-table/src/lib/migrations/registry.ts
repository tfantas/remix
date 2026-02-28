import type { MigrationDescriptor, MigrationRegistry } from '../migrations.ts'

export function sortMigrations(migrations: MigrationDescriptor[]): MigrationDescriptor[] {
  return [...migrations].sort((left, right) => left.id.localeCompare(right.id))
}

export function resolveMigrations(input: MigrationDescriptor[] | MigrationRegistry): MigrationDescriptor[] {
  if (Array.isArray(input)) {
    return sortMigrations(input)
  }

  return input.list()
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
