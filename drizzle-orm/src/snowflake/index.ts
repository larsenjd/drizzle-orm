/**
 * Snowflake driver for Drizzle ORM.
 * 
 * Provides integration with the `snowflake-sdk` npm package.
 * 
 * @example
 * ```ts
 * import snowflake from 'snowflake-sdk';
 * import { drizzle } from 'drizzle-orm/snowflake';
 * import { snowflakeTable, varchar, integer, timestamp } from 'drizzle-orm/snowflake-core';
 * 
 * // Define your schema
 * const users = snowflakeTable('users', {
 *   id: integer('id').primaryKey(),
 *   name: varchar('name', { length: 255 }),
 *   email: varchar('email', { length: 255 }).notNull(),
 *   createdAt: timestamp('created_at').defaultNow(),
 * });
 * 
 * // Create connection
 * const connection = snowflake.createConnection({
 *   account: 'your-account',
 *   username: 'your-username',
 *   password: 'your-password',
 *   database: 'your-database',
 *   warehouse: 'your-warehouse',
 * });
 * 
 * // Connect and create db instance
 * await new Promise<void>((resolve, reject) => {
 *   connection.connect((err) => {
 *     if (err) reject(err);
 *     else resolve();
 *   });
 * });
 * 
 * const db = drizzle(connection);
 * 
 * // Execute queries
 * const allUsers = await db.select().from(users);
 * ```
 * 
 * @packageDocumentation
 */

export * from './driver.ts';
export * from './session.ts';
