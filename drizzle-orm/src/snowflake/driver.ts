import { entityKind } from '~/entity.ts';
import type { Logger } from '~/logger.ts';
import { DefaultLogger } from '~/logger.ts';
import type { ExtractTablesWithRelations, TablesRelationalConfig } from '~/relations.ts';
import { createTableRelationsHelpers, extractTablesRelationalConfig } from '~/relations.ts';
import { SnowflakeDatabase } from '~/snowflake-core/db.ts';
import { SnowflakeDialect } from '~/snowflake-core/dialect.ts';
import {
	SnowflakeSdkSession,
	type SnowflakeConnection,
	type SnowflakeDriverConfig,
	type SnowflakeSdkQueryResultHKT,
} from './session.ts';

/**
 * Configuration options for the Snowflake drizzle driver.
 */
export interface DrizzleSnowflakeConfig<
	TSchema extends Record<string, unknown> = Record<string, never>,
> {
	/**
	 * Schema for relational queries support (optional).
	 * Pass your schema object to enable relational queries.
	 */
	schema?: TSchema;
	/**
	 * Logger instance for query logging.
	 * Set to `true` to use the default logger, or provide a custom Logger instance.
	 */
	logger?: boolean | Logger;
}

/**
 * Snowflake connection options for creating a new connection.
 * Supports password auth and browser-based SSO (EXTERNALBROWSER).
 */
export interface SnowflakeConnectionOptions {
	/**
	 * Your Snowflake account identifier.
	 * Format: <account>.<region>.snowflakecomputing.com or just <account>
	 */
	account: string;
	/**
	 * Snowflake username.
	 */
	username: string;
	/**
	 * Snowflake password (for password authentication).
	 */
	password?: string;
	/**
	 * Authentication method.
	 * - 'SNOWFLAKE' (default): Password-based authentication.
	 * - 'EXTERNALBROWSER': Opens a browser for SSO authentication.
	 */
	authenticator?: 'SNOWFLAKE' | 'EXTERNALBROWSER';
	/**
	 * Database name.
	 */
	database?: string;
	/**
	 * Schema name.
	 */
	schema?: string;
	/**
	 * Warehouse name.
	 */
	warehouse?: string;
	/**
	 * Role name.
	 */
	role?: string;
}

export class SnowflakeSdkDriver {
	static readonly [entityKind]: string = 'SnowflakeSdkDriver';

	constructor(
		private connection: SnowflakeConnection,
		private options: SnowflakeDriverConfig = {},
	) {}

	createSession<
		TFullSchema extends Record<string, unknown> = Record<string, never>,
		TSchema extends TablesRelationalConfig = Record<string, never>,
	>(
		schema: {
			fullSchema: Record<string, unknown>;
			schema: TSchema;
			tableNamesMap: Record<string, string>;
		} | undefined,
	): SnowflakeSdkSession<TFullSchema, TSchema> {
		return new SnowflakeSdkSession(
			this.connection,
			new SnowflakeDialect(),
			schema,
			this.options,
		);
	}
}

export { SnowflakeDatabase } from '~/snowflake-core/db.ts';

/**
 * Create a Drizzle database instance from an existing Snowflake SDK connection.
 * 
 * @example
 * ```ts
 * import snowflake from 'snowflake-sdk';
 * import { drizzle } from 'drizzle-orm/snowflake';
 * 
 * // Password authentication
 * const connection = snowflake.createConnection({
 *   account: 'your-account',
 *   username: 'your-username',
 *   password: 'your-password',
 *   database: 'your-database',
 *   warehouse: 'your-warehouse',
 * });
 * 
 * // Connect
 * await new Promise<void>((resolve, reject) => {
 *   connection.connect((err, conn) => {
 *     if (err) reject(err);
 *     else resolve();
 *   });
 * });
 * 
 * const db = drizzle(connection);
 * 
 * // Browser-based SSO authentication (EXTERNALBROWSER)
 * const ssoConnection = snowflake.createConnection({
 *   account: 'your-account',
 *   username: 'your-email@company.com',
 *   authenticator: 'EXTERNALBROWSER',
 *   database: 'your-database',
 *   warehouse: 'your-warehouse',
 * });
 * 
 * await new Promise<void>((resolve, reject) => {
 *   ssoConnection.connect((err, conn) => {
 *     if (err) reject(err);
 *     else resolve();
 *   });
 * });
 * 
 * const ssoDb = drizzle(ssoConnection);
 * ```
 * 
 * @param connection - An established snowflake-sdk Connection.
 * @param config - Optional configuration for schema and logging.
 * @returns A SnowflakeDatabase instance for executing queries.
 */
export function drizzle<
	TSchema extends Record<string, unknown> = Record<string, never>,
>(
	connection: SnowflakeConnection,
	config: DrizzleSnowflakeConfig<TSchema> = {},
): SnowflakeDatabase<SnowflakeSdkQueryResultHKT, TSchema & Record<string, never>, ExtractTablesWithRelations<TSchema>> {
	const dialect = new SnowflakeDialect();
	
	let logger: Logger | undefined;
	if (config.logger === true) {
		logger = new DefaultLogger();
	} else if (config.logger !== false && config.logger !== undefined) {
		logger = config.logger;
	}

	let schema: {
		fullSchema: Record<string, unknown>;
		schema: TablesRelationalConfig;
		tableNamesMap: Record<string, string>;
	} | undefined;

	if (config.schema) {
		const tablesConfig = extractTablesRelationalConfig(
			config.schema,
			createTableRelationsHelpers,
		);
		schema = {
			fullSchema: config.schema as Record<string, unknown>,
			schema: tablesConfig.tables,
			tableNamesMap: tablesConfig.tableNamesMap,
		};
	}

	const driver = new SnowflakeSdkDriver(connection, { logger });
	const session = driver.createSession(schema);

	return new SnowflakeDatabase(
		dialect,
		session as SnowflakeSdkSession<TSchema & Record<string, never>, ExtractTablesWithRelations<TSchema>>,
		schema as {
			fullSchema: TSchema & Record<string, never>;
			schema: ExtractTablesWithRelations<TSchema>;
			tableNamesMap: Record<string, string>;
		} | undefined,
	) as SnowflakeDatabase<SnowflakeSdkQueryResultHKT, TSchema & Record<string, never>, ExtractTablesWithRelations<TSchema>>;
}
