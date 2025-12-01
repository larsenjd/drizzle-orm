import { entityKind } from '~/entity.ts';
import type { Logger } from '~/logger.ts';
import { NoopLogger } from '~/logger.ts';
import type { TablesRelationalConfig } from '~/relations.ts';
import type { Query } from '~/sql/sql.ts';
import {
	type PreparedQueryConfig,
	SnowflakePreparedQuery,
	SnowflakeSession,
	SnowflakeTransaction,
	type SnowflakeQueryResultHKT,
	type SnowflakeTransactionConfig,
} from '~/snowflake-core/session.ts';
import type { SelectedFieldsOrdered } from '~/snowflake-core/query-builders/select.types.ts';
import type { SnowflakeDialect } from '~/snowflake-core/dialect.ts';

// Types for snowflake-sdk - these will be used when the user has snowflake-sdk installed
export interface SnowflakeConnection {
	execute(options: {
		sqlText: string;
		binds?: unknown[];
		complete: (err: Error | undefined, stmt: unknown, rows: unknown[] | undefined) => void;
	}): void;
}

export interface SnowflakeDriverConfig {
	logger?: Logger;
}

export class SnowflakeSdkPreparedQuery<T extends PreparedQueryConfig> extends SnowflakePreparedQuery<T> {
	static override readonly [entityKind]: string = 'SnowflakeSdkPreparedQuery';

	private rawQuery: { sql: string; params: unknown[] };

	constructor(
		private connection: SnowflakeConnection,
		query: Query,
		private logger: Logger,
		private fields: SelectedFieldsOrdered | undefined,
		name: string | undefined,
		private _isResponseInArrayMode: boolean,
		private customResultMapper?: (rows: unknown[][]) => T['execute'],
	) {
		super(query);
		this.rawQuery = {
			sql: query.sql,
			params: query.params as unknown[],
		};
	}

	async execute(_placeholderValues?: Record<string, unknown>): Promise<T['execute']> {
		const params = this.rawQuery.params;
		this.logger.logQuery(this.rawQuery.sql, params);

		return new Promise<T['execute']>((resolve, reject) => {
			this.connection.execute({
				sqlText: this.rawQuery.sql,
				binds: params,
				complete: (err: Error | undefined, stmt: unknown, rows: unknown[] | undefined) => {
					if (err) {
						reject(err);
						return;
					}
					
					if (this.customResultMapper) {
						resolve(this.customResultMapper(rows as unknown[][]));
					} else {
						resolve(rows as T['execute']);
					}
				},
			});
		});
	}

	async all(_placeholderValues?: Record<string, unknown>): Promise<T['all']> {
		const params = this.rawQuery.params;
		this.logger.logQuery(this.rawQuery.sql, params);

		return new Promise<T['all']>((resolve, reject) => {
			this.connection.execute({
				sqlText: this.rawQuery.sql,
				binds: params,
				complete: (err: Error | undefined, stmt: unknown, rows: unknown[] | undefined) => {
					if (err) {
						reject(err);
						return;
					}
					resolve(rows as T['all']);
				},
			});
		});
	}

	isResponseInArrayMode(): boolean {
		return this._isResponseInArrayMode;
	}
}

export interface SnowflakeSdkQueryResultHKT extends SnowflakeQueryResultHKT {
	type: unknown[];
}

export class SnowflakeSdkSession<
	TFullSchema extends Record<string, unknown> = Record<string, never>,
	TSchema extends TablesRelationalConfig = Record<string, never>,
> extends SnowflakeSession<SnowflakeSdkQueryResultHKT, TFullSchema, TSchema> {
	static override readonly [entityKind]: string = 'SnowflakeSdkSession';

	private logger: Logger;

	constructor(
		private connection: SnowflakeConnection,
		dialect: SnowflakeDialect,
		private schema: {
			fullSchema: Record<string, unknown>;
			schema: TSchema;
			tableNamesMap: Record<string, string>;
		} | undefined,
		private options: SnowflakeDriverConfig = {},
	) {
		super(dialect);
		this.logger = options.logger ?? new NoopLogger();
	}

	prepareQuery<T extends PreparedQueryConfig = PreparedQueryConfig>(
		query: Query,
		fields: SelectedFieldsOrdered | undefined,
		name: string | undefined,
		isResponseInArrayMode: boolean,
		customResultMapper?: (rows: unknown[][]) => T['execute'],
	): SnowflakePreparedQuery<T> {
		return new SnowflakeSdkPreparedQuery(
			this.connection,
			query,
			this.logger,
			fields,
			name,
			isResponseInArrayMode,
			customResultMapper,
		);
	}

	/**
	 * Execute a transaction.
	 * 
	 * Uses Snowflake's BEGIN TRANSACTION / COMMIT / ROLLBACK.
	 * Note: Nested transaction calls execute within the same transaction scope (scoped transactions).
	 * Warning: DDL statements auto-commit any active transaction.
	 */
	async transaction<T>(
		transaction: (tx: SnowflakeSdkTransaction<TFullSchema, TSchema>) => Promise<T>,
		_config?: SnowflakeTransactionConfig,
	): Promise<T> {
		const tx = new SnowflakeSdkTransaction(
			this.connection,
			this.dialect,
			this,
			this.schema,
			this.options,
			0,
		);

		// Start transaction
		await new Promise<void>((resolve, reject) => {
			this.connection.execute({
				sqlText: 'BEGIN TRANSACTION',
				complete: (err: Error | undefined) => {
					if (err) reject(err);
					else resolve();
				},
			});
		});

		try {
			const result = await transaction(tx);
			
			// Commit transaction
			await new Promise<void>((resolve, reject) => {
				this.connection.execute({
					sqlText: 'COMMIT',
					complete: (err: Error | undefined) => {
						if (err) reject(err);
						else resolve();
					},
				});
			});
			
			return result;
		} catch (error) {
			// Rollback transaction
			await new Promise<void>((resolve, reject) => {
				this.connection.execute({
					sqlText: 'ROLLBACK',
					complete: (err: Error | undefined) => {
						if (err) reject(err);
						else resolve();
					},
				});
			});
			throw error;
		}
	}
}

export class SnowflakeSdkTransaction<
	TFullSchema extends Record<string, unknown> = Record<string, never>,
	TSchema extends TablesRelationalConfig = Record<string, never>,
> extends SnowflakeTransaction<SnowflakeSdkQueryResultHKT, TFullSchema, TSchema> {
	static override readonly [entityKind]: string = 'SnowflakeSdkTransaction';

	constructor(
		private connection: SnowflakeConnection,
		dialect: SnowflakeDialect,
		session: SnowflakeSdkSession<TFullSchema, TSchema>,
		schema: {
			fullSchema: Record<string, unknown>;
			schema: TSchema;
			tableNamesMap: Record<string, string>;
		} | undefined,
		private options: SnowflakeDriverConfig,
		nestedIndex = 0,
	) {
		super(dialect, session, schema, nestedIndex);
	}

	/**
	 * Nested transactions execute within the same transaction scope.
	 * No savepoints are used - all operations are part of the parent transaction.
	 */
	override async transaction<T>(
		transaction: (tx: SnowflakeSdkTransaction<TFullSchema, TSchema>) => Promise<T>,
	): Promise<T> {
		const tx = new SnowflakeSdkTransaction(
			this.connection,
			this.dialect,
			this.session as SnowflakeSdkSession<TFullSchema, TSchema>,
			this.schema,
			this.options,
			this.nestedIndex + 1,
		);
		return transaction(tx);
	}
}
