import { entityKind } from '~/entity.ts';
import { TransactionRollbackError } from '~/errors.ts';
import type { TablesRelationalConfig } from '~/relations.ts';
import type { PreparedQuery } from '~/session.ts';
import type { Query, SQL } from '~/sql/index.ts';
import { tracer } from '~/tracing.ts';
import { SnowflakeDatabase, type SnowflakeQueryResultHKT, type SnowflakeTransactionConfig } from './db.ts';
import type { SnowflakeDialect } from './dialect.ts';
import type { SelectedFieldsOrdered } from './query-builders/select.types.ts';

export type { SnowflakeQueryResultHKT, SnowflakeTransactionConfig } from './db.ts';

export interface PreparedQueryConfig {
	execute: unknown;
	all: unknown;
	values: unknown;
}

export abstract class SnowflakePreparedQuery<T extends PreparedQueryConfig> implements PreparedQuery {
	static readonly [entityKind]: string = 'SnowflakePreparedQuery';

	/** @internal */
	joinsNotNullableMap?: Record<string, boolean>;

	constructor(protected query: Query) {}

	getQuery(): Query {
		return this.query;
	}

	mapResult(response: unknown, _isFromBatch?: boolean): unknown {
		return response;
	}

	abstract execute(placeholderValues?: Record<string, unknown>): Promise<T['execute']>;

	abstract all(placeholderValues?: Record<string, unknown>): Promise<T['all']>;

	abstract isResponseInArrayMode(): boolean;
}

export abstract class SnowflakeSession<
	TQueryResult extends SnowflakeQueryResultHKT = SnowflakeQueryResultHKT,
	TFullSchema extends Record<string, unknown> = Record<string, never>,
	TSchema extends TablesRelationalConfig = Record<string, never>,
> {
	static readonly [entityKind]: string = 'SnowflakeSession';

	constructor(protected dialect: SnowflakeDialect) {}

	abstract prepareQuery<T extends PreparedQueryConfig = PreparedQueryConfig>(
		query: Query,
		fields: SelectedFieldsOrdered | undefined,
		name: string | undefined,
		isResponseInArrayMode: boolean,
		customResultMapper?: (rows: unknown[][], mapColumnValue?: (value: unknown) => unknown) => T['execute'],
	): SnowflakePreparedQuery<T>;

	execute<T>(query: SQL): Promise<T> {
		return tracer.startActiveSpan('drizzle.operation', () => {
			const prepared = tracer.startActiveSpan('drizzle.prepareQuery', () => {
				return this.prepareQuery<PreparedQueryConfig & { execute: T }>(
					this.dialect.sqlToQuery(query),
					undefined,
					undefined,
					false,
				);
			});

			return prepared.execute();
		});
	}

	all<T = unknown>(query: SQL): Promise<T[]> {
		return this.prepareQuery<PreparedQueryConfig & { all: T[] }>(
			this.dialect.sqlToQuery(query),
			undefined,
			undefined,
			false,
		).all();
	}

	async count(sql: SQL): Promise<number> {
		const res = await this.execute<[{ count: string }]>(sql);

		return Number(
			res[0]['count'],
		);
	}

	/**
	 * Execute a transaction.
	 * 
	 * Note: Snowflake uses scoped transactions. Nested transaction calls will
	 * execute within the same transaction scope (no savepoints).
	 * 
	 * Warning: DDL statements (CREATE, ALTER, DROP) auto-commit any active transaction.
	 * TODO: Consider adding ALTER SESSION SET AUTOCOMMIT = FALSE option.
	 */
	abstract transaction<T>(
		transaction: (tx: SnowflakeTransaction<TQueryResult, TFullSchema, TSchema>) => Promise<T>,
		config?: SnowflakeTransactionConfig,
	): Promise<T>;
}

export abstract class SnowflakeTransaction<
	TQueryResult extends SnowflakeQueryResultHKT,
	TFullSchema extends Record<string, unknown> = Record<string, never>,
	TSchema extends TablesRelationalConfig = Record<string, never>,
> extends SnowflakeDatabase<TQueryResult, TFullSchema, TSchema> {
	static override readonly [entityKind]: string = 'SnowflakeTransaction';

	constructor(
		dialect: SnowflakeDialect,
		session: SnowflakeSession<any, any, any>,
		protected schema: {
			fullSchema: Record<string, unknown>;
			schema: TSchema;
			tableNamesMap: Record<string, string>;
		} | undefined,
		protected readonly nestedIndex = 0,
	) {
		super(dialect, session, schema);
	}

	rollback(): never {
		throw new TransactionRollbackError();
	}

	/**
	 * Nested transactions in Snowflake execute within the same transaction scope.
	 * No savepoints are used - all operations are part of the parent transaction.
	 */
	abstract override transaction<T>(
		transaction: (tx: SnowflakeTransaction<TQueryResult, TFullSchema, TSchema>) => Promise<T>,
	): Promise<T>;
}

export type SnowflakeQueryResultKind<TKind extends SnowflakeQueryResultHKT, TRow> = (TKind & {
	readonly row: TRow;
})['type'];

