import { entityKind } from '~/entity.ts';
import type { ExtractTablesWithRelations, RelationalSchemaConfig, TablesRelationalConfig } from '~/relations.ts';
import type { SQL } from '~/sql/sql.ts';
import type { DrizzleTypeError } from '~/utils.ts';

// Forward declare types to avoid circular dependency
export interface SnowflakeQueryResultHKT {
	readonly $brand: 'SnowflakeQueryResultHKT';
	readonly row: unknown;
	readonly type: unknown;
}

export interface SnowflakeTransactionConfig {
	isolationLevel?: 'read committed';
}

// These will be properly typed in session.ts
export interface SnowflakeSessionBase<
	TQueryResult extends SnowflakeQueryResultHKT = SnowflakeQueryResultHKT,
	TFullSchema extends Record<string, unknown> = Record<string, never>,
	TSchema extends TablesRelationalConfig = Record<string, never>,
> {
	execute<T>(query: SQL): Promise<T>;
	transaction<T>(
		transaction: (tx: any) => Promise<T>,
		config?: SnowflakeTransactionConfig,
	): Promise<T>;
}

export class SnowflakeDatabase<
	TQueryResult extends SnowflakeQueryResultHKT,
	TFullSchema extends Record<string, unknown> = Record<string, never>,
	TSchema extends TablesRelationalConfig = ExtractTablesWithRelations<TFullSchema>,
> {
	static readonly [entityKind]: string = 'SnowflakeDatabase';

	declare readonly _: {
		readonly schema: TSchema | undefined;
		readonly fullSchema: TFullSchema;
		readonly tableNamesMap: Record<string, string>;
		readonly session: SnowflakeSessionBase<TQueryResult, TFullSchema, TSchema>;
	};

	// TODO: Add relational query support
	query: TFullSchema extends Record<string, never>
		? DrizzleTypeError<'Seems like the schema generic is missing - did you forget to add it to your DB type?'>
		: Record<string, never>;

	constructor(
		/** @internal */
		readonly dialect: any, // SnowflakeDialect - typed as any to break circular dep
		/** @internal */
		readonly session: SnowflakeSessionBase<any, any, any>,
		schema: RelationalSchemaConfig<TSchema> | undefined,
	) {
		this._ = schema
			? {
				schema: schema.schema,
				fullSchema: schema.fullSchema as TFullSchema,
				tableNamesMap: schema.tableNamesMap,
				session,
			}
			: {
				schema: undefined,
				fullSchema: {} as TFullSchema,
				tableNamesMap: {},
				session,
			};
		this.query = {} as typeof this['query'];
	}

	/**
	 * Execute a raw SQL query.
	 * 
	 * @param query The SQL query to execute
	 * @returns Promise with the query result
	 * 
	 * @example
	 * ```ts
	 * const result = await db.execute(sql`SELECT * FROM users WHERE id = ${userId}`);
	 * ```
	 */
	execute<T = unknown>(query: SQL): Promise<T> {
		return this.session.execute(query);
	}

	/**
	 * Execute a transaction.
	 * 
	 * Note: Snowflake uses scoped transactions. Nested transaction calls will
	 * execute within the same transaction scope (no savepoints).
	 * 
	 * Warning: DDL statements (CREATE, ALTER, DROP) auto-commit any active transaction.
	 * 
	 * @param transaction The transaction function
	 * @param config Optional transaction configuration
	 * 
	 * @example
	 * ```ts
	 * await db.transaction(async (tx) => {
	 *   await tx.execute(sql`INSERT INTO users (name) VALUES ('Alice')`);
	 *   await tx.execute(sql`INSERT INTO orders (user_id) VALUES (1)`);
	 * });
	 * ```
	 */
	transaction<T>(
		transaction: (tx: SnowflakeDatabase<TQueryResult, TFullSchema, TSchema>) => Promise<T>,
		config?: SnowflakeTransactionConfig,
	): Promise<T> {
		return this.session.transaction(transaction as any, config);
	}
}
