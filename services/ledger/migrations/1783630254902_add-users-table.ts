import { MigrationBuilder } from "node-pg-migrate"

/**
 * @type {import('node-pg-migrate').ColumnDefinitions | undefined}
 */
export const shorthands = undefined

/**
 * @param pgm {import('node-pg-migrate').MigrationBuilder}
 * @param run {() => void | undefined}
 * @returns {Promise<void> | void}
 */
export const up = (pgm: MigrationBuilder) => {
  pgm.createTable("users", {
    id: "id",
    name: { type: "varchar(100)", notNull: true },
    email: { type: "varchar(255)", notNull: true, unique: true },
    created_at: { type: "timestamp", notNull: true, default: "now()" },
  })
}

/**
 * @param pgm {import('node-pg-migrate').MigrationBuilder}
 * @param run {() => void | undefined}
 * @returns {Promise<void> | void}
 */
export const down = (pgm: MigrationBuilder) => {
  pgm.dropTable("users")
}
