import { column as c, createMigration } from 'remix/data-table/migrations'
import { table } from 'remix/data-table'

export default createMigration({
  async up({ db }) {
    let books = table({
      name: 'books',
      columns: {
        id: c.integer().primaryKey().autoIncrement(),
        slug: c.text().notNull().unique(),
        title: c.text().notNull(),
        author: c.text().notNull(),
        description: c.text().notNull(),
        price: c.decimal(10, 2).notNull(),
        genre: c.text().notNull(),
        image_urls: c.text().notNull(),
        cover_url: c.text().notNull(),
        isbn: c.text().notNull(),
        published_year: c.integer().notNull(),
        in_stock: c.boolean().notNull(),
      },
    })
    await db.createTable(books)

    let users = table({
      name: 'users',
      columns: {
        id: c.integer().primaryKey().autoIncrement(),
        email: c.text().notNull().unique(),
        password: c.text().notNull(),
        name: c.text().notNull(),
        role: c.text().notNull(),
        created_at: c.integer().notNull(),
      },
    })
    await db.createTable(users)

    let orders = table({
      name: 'orders',
      columns: {
        id: c.integer().primaryKey().autoIncrement(),
        user_id: c
          .integer()
          .notNull()
          .references('users', 'id', 'orders_user_id_fk')
          .onDelete('restrict'),
        total: c.decimal(10, 2).notNull(),
        status: c.text().notNull(),
        shipping_address_json: c.text().notNull(),
        created_at: c.integer().notNull(),
      },
    })
    await db.createTable(orders)
    await db.createIndex('orders', 'orders_user_id_idx', 'user_id')

    let orderItems = table({
      name: 'order_items',
      primaryKey: ['order_id', 'book_id'],
      columns: {
        order_id: c
          .integer()
          .notNull()
          .references('orders', 'id', 'order_items_order_id_fk')
          .onDelete('cascade'),
        book_id: c
          .integer()
          .notNull()
          .references('books', 'id', 'order_items_book_id_fk')
          .onDelete('restrict'),
        title: c.text().notNull(),
        unit_price: c.decimal(10, 2).notNull(),
        quantity: c.integer().notNull(),
      },
    })
    await db.createTable(orderItems)
    await db.createIndex('order_items', 'order_items_order_id_idx', 'order_id')
    await db.createIndex('order_items', 'order_items_book_id_idx', 'book_id')

    let passwordResetTokens = table({
      name: 'password_reset_tokens',
      primaryKey: ['token'],
      columns: {
        token: c.text().primaryKey(),
        user_id: c
          .integer()
          .notNull()
          .references('users', 'id', 'password_reset_tokens_user_id_fk')
          .onDelete('cascade'),
        expires_at: c.integer().notNull(),
      },
    })
    await db.createTable(passwordResetTokens)
    await db.createIndex('password_reset_tokens', 'password_reset_tokens_user_id_idx', 'user_id')
  },
  async down({ db }) {
    await db.dropTable('password_reset_tokens', { ifExists: true })
    await db.dropTable('order_items', { ifExists: true })
    await db.dropTable('orders', { ifExists: true })
    await db.dropTable('users', { ifExists: true })
    await db.dropTable('books', { ifExists: true })
  },
})
