import { belongsTo, column as c, table, hasMany } from 'remix/data-table'
import type { TableRow, TableRowWith } from 'remix/data-table'

export const books = table({
  name: 'books',
  columns: {
    id: c.integer(),
    slug: c.text(),
    title: c.text(),
    author: c.text(),
    description: c.text(),
    price: c.decimal(10, 2),
    genre: c.text(),
    image_urls: c.text(),
    cover_url: c.text(),
    isbn: c.text(),
    published_year: c.integer(),
    in_stock: c.boolean(),
  },
})

export const users = table({
  name: 'users',
  columns: {
    id: c.integer(),
    email: c.text(),
    password: c.text(),
    name: c.text(),
    role: c.enum(['customer', 'admin']),
    created_at: c.integer(),
  },
})

export const orders = table({
  name: 'orders',
  columns: {
    id: c.integer(),
    user_id: c.integer(),
    total: c.decimal(10, 2),
    status: c.enum(['pending', 'processing', 'shipped', 'delivered']),
    shipping_address_json: c.text(),
    created_at: c.integer(),
  },
})

export const orderItems = table({
  name: 'order_items',
  primaryKey: ['order_id', 'book_id'],
  columns: {
    order_id: c.integer(),
    book_id: c.integer(),
    title: c.text(),
    unit_price: c.decimal(10, 2),
    quantity: c.integer(),
  },
})

export const itemsByOrder = hasMany(orders, orderItems)
export const bookForOrderItem = belongsTo(orderItems, books)
export const orderItemsWithBook = itemsByOrder
  .orderBy('book_id', 'asc')
  .with({ book: bookForOrderItem })

export const passwordResetTokens = table({
  name: 'password_reset_tokens',
  primaryKey: ['token'],
  columns: {
    token: c.text(),
    user_id: c.integer(),
    expires_at: c.integer(),
  },
})

export type Book = TableRow<typeof books>
export type User = TableRow<typeof users>
export type Order = TableRowWith<typeof orders, { items: OrderItem[] }>
export type OrderItem = TableRowWith<
  typeof itemsByOrder.targetTable,
  { book: TableRow<typeof bookForOrderItem.targetTable> | null }
>
