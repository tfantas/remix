import { column as c, createMigration } from 'remix/data-table/migrations'

export default createMigration({
  async up({ db }) {
    await db.createTable('books', (table) => {
      table.addColumn('id', c.integer().primaryKey().autoIncrement())
      table.addColumn('slug', c.text().notNull().unique())
      table.addColumn('title', c.text().notNull())
      table.addColumn('author', c.text().notNull())
      table.addColumn('description', c.text().notNull())
      table.addColumn('price', c.decimal(10, 2).notNull())
      table.addColumn('genre', c.text().notNull())
      table.addColumn('image_urls', c.text().notNull())
      table.addColumn('cover_url', c.text().notNull())
      table.addColumn('isbn', c.text().notNull())
      table.addColumn('published_year', c.integer().notNull())
      table.addColumn('in_stock', c.boolean().notNull())
    })

    await db.createTable('users', (table) => {
      table.addColumn('id', c.integer().primaryKey().autoIncrement())
      table.addColumn('email', c.text().notNull().unique())
      table.addColumn('password', c.text().notNull())
      table.addColumn('name', c.text().notNull())
      table.addColumn('role', c.text().notNull())
      table.addColumn('created_at', c.integer().notNull())
    })

    await db.createTable('orders', (table) => {
      table.addColumn('id', c.integer().primaryKey().autoIncrement())
      table.addColumn(
        'user_id',
        c.integer().notNull().references('users', 'id', 'orders_user_id_fk').onDelete('restrict'),
      )
      table.addColumn('total', c.decimal(10, 2).notNull())
      table.addColumn('status', c.text().notNull())
      table.addColumn('shipping_address_json', c.text().notNull())
      table.addColumn('created_at', c.integer().notNull())
      table.addIndex('orders_user_id_idx', 'user_id')
    })

    await db.createTable('order_items', (table) => {
      table.addColumn(
        'order_id',
        c.integer().notNull().references('orders', 'id', 'order_items_order_id_fk').onDelete('cascade'),
      )
      table.addColumn(
        'book_id',
        c.integer().notNull().references('books', 'id', 'order_items_book_id_fk').onDelete('restrict'),
      )
      table.addColumn('title', c.text().notNull())
      table.addColumn('unit_price', c.decimal(10, 2).notNull())
      table.addColumn('quantity', c.integer().notNull())
      table.addPrimaryKey('order_items_pk', ['order_id', 'book_id'])
      table.addIndex('order_items_order_id_idx', 'order_id')
      table.addIndex('order_items_book_id_idx', 'book_id')
    })

    await db.createTable('password_reset_tokens', (table) => {
      table.addColumn('token', c.text().primaryKey())
      table.addColumn(
        'user_id',
        c.integer()
          .notNull()
          .references('users', 'id', 'password_reset_tokens_user_id_fk')
          .onDelete('cascade'),
      )
      table.addColumn('expires_at', c.integer().notNull())
      table.addIndex('password_reset_tokens_user_id_idx', 'user_id')
    })
  },
  async down({ db }) {
    await db.dropTable('password_reset_tokens', { ifExists: true })
    await db.dropTable('order_items', { ifExists: true })
    await db.dropTable('orders', { ifExists: true })
    await db.dropTable('users', { ifExists: true })
    await db.dropTable('books', { ifExists: true })
  },
})
