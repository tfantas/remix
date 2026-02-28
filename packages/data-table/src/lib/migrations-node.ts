import { createHash } from 'node:crypto'
import { promises as fs } from 'node:fs'
import path from 'node:path'
import { pathToFileURL } from 'node:url'
import type { Migration, MigrationDescriptor } from './migrations.ts'
import { parseMigrationFilename } from './migrations.ts'

export async function loadMigrations(directory: string): Promise<MigrationDescriptor[]> {
  let allFiles = (await fs.readdir(directory, { withFileTypes: true }))
    .filter((entry) => entry.isFile())
    .map((entry) => entry.name)
    .sort((left, right) => left.localeCompare(right))
  let files: string[] = []

  for (let file of allFiles) {
    if (!/\.(?:m?ts|m?js|cts|cjs)$/.test(file)) {
      continue
    }

    parseMigrationFilename(file)
    files.push(file)
  }

  let migrations: MigrationDescriptor[] = []
  let seenIds = new Set<string>()

  for (let file of files) {
    let parsed = parseMigrationFilename(file)

    if (seenIds.has(parsed.id)) {
      throw new Error('Duplicate migration id "' + parsed.id + '" inferred from filename "' + file + '"')
    }

    seenIds.add(parsed.id)
    let fullPath = path.join(directory, file)
    let source = await fs.readFile(fullPath, 'utf8')
    let checksum = createHash('sha256').update(source).digest('hex')
    let module = (await import(pathToFileURL(fullPath).href)) as { default?: Migration }
    let migration = module.default

    if (!migration || typeof migration.up !== 'function' || typeof migration.down !== 'function') {
      throw new Error('Migration file "' + file + '" must default-export createMigration(...)')
    }

    migrations.push({
      id: parsed.id,
      name: parsed.name,
      path: fullPath,
      checksum,
      migration,
    })
  }

  return migrations
}
