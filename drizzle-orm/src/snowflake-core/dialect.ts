import { CasingCache } from '~/casing.ts';
import { Column } from '~/column.ts';
import { entityKind, is } from '~/entity.ts';
import type { MigrationConfig, MigrationMeta } from '~/migrator.ts';
import {
	SnowflakeColumn,
	SnowflakeTimestamp,
	SnowflakeTimestampString,
	SnowflakeDate,
	SnowflakeDateString,
} from '~/snowflake-core/columns/index.ts';
import type {
	SnowflakeDeleteConfig,
	SnowflakeInsertConfig,
	SnowflakeSelectJoinConfig,
	SnowflakeUpdateConfig,
} from '~/snowflake-core/query-builders/index.ts';
import type { SelectedFieldsOrdered, SnowflakeSelectConfig } from '~/snowflake-core/query-builders/select.types.ts';
import { SnowflakeTable } from '~/snowflake-core/table.ts';
import { View } from '~/sql/index.ts';
import {
	type DriverValueEncoder,
	type Name,
	Param,
	type QueryTypingsValue,
	type QueryWithTypings,
	SQL,
	sql,
	type SQLChunk,
} from '~/sql/sql.ts';
import { Subquery } from '~/subquery.ts';
import { getTableName, Table } from '~/table.ts';
import { type Casing, orderSelectedFields, type UpdateSet } from '~/utils.ts';
import { ViewBaseConfig } from '~/view-common.ts';
import type { SnowflakeSession } from './session.ts';

export interface SnowflakeDialectConfig {
	casing?: Casing;
}

export class SnowflakeDialect {
	static readonly [entityKind]: string = 'SnowflakeDialect';

	/** @internal */
	readonly casing: CasingCache;

	constructor(config?: SnowflakeDialectConfig) {
		this.casing = new CasingCache(config?.casing);
	}

	/**
	 * Migrate function for Snowflake.
	 * 
	 * TODO: DDL statements in Snowflake auto-commit any active transaction.
	 * Consider using ALTER SESSION SET AUTOCOMMIT = FALSE before running migrations,
	 * or running each migration statement individually outside of a transaction.
	 */
	async migrate(migrations: MigrationMeta[], session: SnowflakeSession, config: string | MigrationConfig): Promise<void> {
		const migrationsTable = typeof config === 'string'
			? '__drizzle_migrations'
			: config.migrationsTable ?? '__drizzle_migrations';
		const migrationsSchema = typeof config === 'string' ? 'drizzle' : config.migrationsSchema ?? 'drizzle';
		
		// Create schema if it doesn't exist
		await session.execute(sql`CREATE SCHEMA IF NOT EXISTS ${sql.identifier(migrationsSchema)}`);
		
		// Create migrations table if it doesn't exist
		const migrationTableCreate = sql`
			CREATE TABLE IF NOT EXISTS ${sql.identifier(migrationsSchema)}.${sql.identifier(migrationsTable)} (
				id INTEGER AUTOINCREMENT PRIMARY KEY,
				hash VARCHAR NOT NULL,
				created_at BIGINT
			)
		`;
		await session.execute(migrationTableCreate);

		const dbMigrations = await session.all<{ id: number; hash: string; created_at: string }>(
			sql`SELECT id, hash, created_at FROM ${sql.identifier(migrationsSchema)}.${
				sql.identifier(migrationsTable)
			} ORDER BY created_at DESC LIMIT 1`,
		);

		const lastDbMigration = dbMigrations[0];
		
		// Note: DDL statements auto-commit in Snowflake, so we run migrations individually
		// TODO: Add option to use ALTER SESSION SET AUTOCOMMIT = FALSE for migration bundling
		for await (const migration of migrations) {
			if (
				!lastDbMigration
				|| Number(lastDbMigration.created_at) < migration.folderMillis
			) {
				for (const stmt of migration.sql) {
					await session.execute(sql.raw(stmt));
				}
				await session.execute(
					sql`INSERT INTO ${sql.identifier(migrationsSchema)}.${
						sql.identifier(migrationsTable)
					} (hash, created_at) VALUES(${migration.hash}, ${migration.folderMillis})`,
				);
			}
		}
	}

	escapeName(name: string): string {
		return `"${name}"`;
	}

	escapeParam(_num: number): string {
		return `?`;
	}

	escapeString(str: string): string {
		return `'${str.replace(/'/g, "''")}'`;
	}

	private buildWithCTE(queries: Subquery[] | undefined): SQL | undefined {
		if (!queries?.length) return undefined;

		const withSqlChunks = [sql`WITH `];
		for (const [i, w] of queries.entries()) {
			withSqlChunks.push(sql`${sql.identifier(w._.alias)} AS (${w._.sql})`);
			if (i < queries.length - 1) {
				withSqlChunks.push(sql`, `);
			}
		}
		withSqlChunks.push(sql` `);
		return sql.join(withSqlChunks);
	}

	buildDeleteQuery({ table, where, returning: _returning, withList }: SnowflakeDeleteConfig): SQL {
		const withSql = this.buildWithCTE(withList);

		const whereSql = where ? sql` WHERE ${where}` : undefined;

		// Note: Snowflake doesn't support RETURNING in DELETE statements
		// The returning parameter is kept for API compatibility but ignored
		return sql`${withSql}DELETE FROM ${table}${whereSql}`;
	}

	buildUpdateSet(table: SnowflakeTable, set: UpdateSet): SQL {
		const tableColumns = table[Table.Symbol.Columns];

		const columnNames = Object.keys(tableColumns).filter((colName) =>
			set[colName] !== undefined || tableColumns[colName]?.onUpdateFn !== undefined
		);

		const setSize = columnNames.length;
		return sql.join(columnNames.flatMap((colName, i) => {
			const col = tableColumns[colName]!;

			const onUpdateFnResult = col.onUpdateFn?.();
			const value = set[colName] ?? (is(onUpdateFnResult, SQL) ? onUpdateFnResult : sql.param(onUpdateFnResult, col));
			const res = sql`${sql.identifier(this.casing.getColumnCasing(col))} = ${value}`;

			if (i < setSize - 1) {
				return [res, sql.raw(', ')];
			}
			return [res];
		}));
	}

	buildUpdateQuery({ table, set, where, returning: _returning, withList }: SnowflakeUpdateConfig): SQL {
		const withSql = this.buildWithCTE(withList);

		const tableName = table[SnowflakeTable.Symbol.Name];
		const tableSchema = table[SnowflakeTable.Symbol.Schema];
		const origTableName = table[SnowflakeTable.Symbol.OriginalName];
		const alias = tableName === origTableName ? undefined : tableName;
		const tableSql = sql`${tableSchema ? sql`${sql.identifier(tableSchema)}.` : undefined}${
			sql.identifier(origTableName)
		}${alias && sql` ${sql.identifier(alias)}`}`;

		const setSql = this.buildUpdateSet(table, set);

		// Note: Snowflake doesn't support RETURNING in UPDATE statements
		// The returning parameter is kept for API compatibility but ignored
		const whereSql = where ? sql` WHERE ${where}` : undefined;

		return sql`${withSql}UPDATE ${tableSql} SET ${setSql}${whereSql}`;
	}

	/**
	 * Builds selection SQL with provided fields/expressions
	 *
	 * Examples:
	 *
	 * `SELECT <selection> FROM`
	 *
	 * If `isSingleTable` is true, then columns won't be prefixed with table name
	 */
	private buildSelection(
		fields: SelectedFieldsOrdered,
		{ isSingleTable = false }: { isSingleTable?: boolean } = {},
	): SQL {
		const columnsLen = fields.length;

		const chunks = fields
			.flatMap(({ field }, i) => {
				const chunk: SQLChunk[] = [];

				if (is(field, SQL.Aliased) && field.isSelectionField) {
					chunk.push(sql.identifier(field.fieldAlias));
				} else if (is(field, SQL.Aliased) || is(field, SQL)) {
					const query = is(field, SQL.Aliased) ? field.sql : field;

					if (isSingleTable) {
						chunk.push(
							new SQL(
								query.queryChunks.map((c) => {
									if (is(c, SnowflakeColumn)) {
										return sql.identifier(this.casing.getColumnCasing(c));
									}
									return c;
								}),
							),
						);
					} else {
						chunk.push(query);
					}

					if (is(field, SQL.Aliased)) {
						chunk.push(sql` AS ${sql.identifier(field.fieldAlias)}`);
					}
				} else if (is(field, Column)) {
					if (isSingleTable) {
						chunk.push(sql.identifier(this.casing.getColumnCasing(field)));
					} else {
						chunk.push(field);
					}
				} else if (is(field, Subquery)) {
					const entries = Object.entries(field._.selectedFields) as [string, SQL.Aliased | Column | SQL][];

					if (entries.length === 1) {
						const entry = entries[0]![1];

						const fieldDecoder = is(entry, SQL)
							? entry.decoder
							: is(entry, Column)
							? { mapFromDriverValue: (v: any) => entry.mapFromDriverValue(v) }
							: entry.sql.decoder;

						if (fieldDecoder) {
							field._.sql.decoder = fieldDecoder;
						}
					}
					chunk.push(field);
				}

				if (i < columnsLen - 1) {
					chunk.push(sql`, `);
				}

				return chunk;
			});

		return sql.join(chunks);
	}

	private buildJoins(joins: SnowflakeSelectJoinConfig[] | undefined): SQL | undefined {
		if (!joins || joins.length === 0) {
			return undefined;
		}

		const joinsArray: SQL[] = [];

		for (const [index, joinMeta] of joins.entries()) {
			if (index === 0) {
				joinsArray.push(sql` `);
			}
			const table = joinMeta.table;
			const onSql = joinMeta.on ? sql` ON ${joinMeta.on}` : undefined;

			if (is(table, SnowflakeTable)) {
				const tableName = table[SnowflakeTable.Symbol.Name];
				const tableSchema = table[SnowflakeTable.Symbol.Schema];
				const origTableName = table[SnowflakeTable.Symbol.OriginalName];
				const alias = tableName === origTableName ? undefined : joinMeta.alias;
				joinsArray.push(
					sql`${sql.raw(joinMeta.joinType)} JOIN ${
						tableSchema ? sql`${sql.identifier(tableSchema)}.` : undefined
					}${sql.identifier(origTableName)}${alias && sql` ${sql.identifier(alias)}`}${onSql}`,
				);
			} else if (is(table, View)) {
				const viewName = table[ViewBaseConfig].name;
				const viewSchema = table[ViewBaseConfig].schema;
				const origViewName = table[ViewBaseConfig].originalName;
				const alias = viewName === origViewName ? undefined : joinMeta.alias;
				joinsArray.push(
					sql`${sql.raw(joinMeta.joinType)} JOIN ${
						viewSchema ? sql`${sql.identifier(viewSchema)}.` : undefined
					}${sql.identifier(origViewName)}${alias && sql` ${sql.identifier(alias)}`}${onSql}`,
				);
			} else {
				joinsArray.push(
					sql`${sql.raw(joinMeta.joinType)} JOIN ${table}${onSql}`,
				);
			}
			if (index < joins.length - 1) {
				joinsArray.push(sql` `);
			}
		}

		return sql.join(joinsArray);
	}

	private buildFromTable(
		table: SQL | Subquery | SnowflakeTable | undefined,
	): SQL | Subquery | SnowflakeTable | undefined {
		if (is(table, Table) && table[Table.Symbol.IsAlias]) {
			let fullName = sql`${sql.identifier(table[Table.Symbol.OriginalName])}`;
			if (table[Table.Symbol.Schema]) {
				fullName = sql`${sql.identifier(table[Table.Symbol.Schema]!)}.${fullName}`;
			}
			return sql`${fullName} ${sql.identifier(table[Table.Symbol.Name])}`;
		}

		return table;
	}

	buildSelectQuery(
		{
			withList,
			fields,
			fieldsFlat,
			where,
			having,
			table,
			joins,
			orderBy,
			groupBy,
			limit,
			offset,
			distinct,
			setOperators,
		}: SnowflakeSelectConfig,
	): SQL {
		const fieldsList = fieldsFlat ?? orderSelectedFields<SnowflakeColumn>(fields);
		for (const f of fieldsList) {
			if (
				is(f.field, Column)
				&& getTableName(f.field.table)
					!== (is(table, Subquery)
						? table._.alias
						: is(table, SQL)
						? undefined
						: getTableName(table))
				&& !((table) =>
					joins?.some(({ alias }) =>
						alias === (table[Table.Symbol.IsAlias] ? getTableName(table) : table[Table.Symbol.BaseName])
					))(f.field.table)
			) {
				const tableName = getTableName(f.field.table);
				throw new Error(
					`Your "${
						f.path.join('->')
					}" field references a column "${tableName}"."${f.field.name}", but the table "${tableName}" is not part of the query! Did you forget to join it?`,
				);
			}
		}

		const isSingleTable = !joins || joins.length === 0;

		const withSql = this.buildWithCTE(withList);

		let distinctSql: SQL | undefined;
		if (distinct) {
			distinctSql = sql` DISTINCT`;
		}

		const selection = this.buildSelection(fieldsList, { isSingleTable });

		const tableSql = this.buildFromTable(table);

		const joinsSql = this.buildJoins(joins);

		const whereSql = where ? sql` WHERE ${where}` : undefined;

		const havingSql = having ? sql` HAVING ${having}` : undefined;

		let orderBySql;
		if (orderBy && orderBy.length > 0) {
			orderBySql = sql` ORDER BY ${sql.join(orderBy, sql`, `)}`;
		}

		let groupBySql;
		if (groupBy && groupBy.length > 0) {
			groupBySql = sql` GROUP BY ${sql.join(groupBy, sql`, `)}`;
		}

		const limitSql = typeof limit === 'object' || (typeof limit === 'number' && limit >= 0)
			? sql` LIMIT ${limit}`
			: undefined;

		const offsetSql = offset ? sql` OFFSET ${offset}` : undefined;

		const finalQuery =
			sql`${withSql}SELECT${distinctSql} ${selection} FROM ${tableSql}${joinsSql}${whereSql}${groupBySql}${havingSql}${orderBySql}${limitSql}${offsetSql}`;

		if (setOperators.length > 0) {
			return this.buildSetOperations(finalQuery, setOperators);
		}

		return finalQuery;
	}

	buildSetOperations(leftSelect: SQL, setOperators: SnowflakeSelectConfig['setOperators']): SQL {
		const [setOperator, ...rest] = setOperators;

		if (!setOperator) {
			throw new Error('Cannot pass undefined values to any set operator');
		}

		if (rest.length === 0) {
			return this.buildSetOperationQuery({ leftSelect, setOperator });
		}

		return this.buildSetOperations(
			this.buildSetOperationQuery({ leftSelect, setOperator }),
			rest,
		);
	}

	buildSetOperationQuery({
		leftSelect,
		setOperator: { type, isAll, rightSelect, limit, orderBy, offset },
	}: { leftSelect: SQL; setOperator: SnowflakeSelectConfig['setOperators'][number] }): SQL {
		const leftChunk = sql`(${leftSelect.getSQL()}) `;
		const rightChunk = sql`(${rightSelect.getSQL()})`;

		let orderBySql;
		if (orderBy && orderBy.length > 0) {
			const orderByValues: (SQL<unknown> | Name)[] = [];

			for (const singleOrderBy of orderBy) {
				if (is(singleOrderBy, SnowflakeColumn)) {
					orderByValues.push(sql.identifier(singleOrderBy.name));
				} else if (is(singleOrderBy, SQL)) {
					for (let i = 0; i < singleOrderBy.queryChunks.length; i++) {
						const chunk = singleOrderBy.queryChunks[i];

						if (is(chunk, SnowflakeColumn)) {
							singleOrderBy.queryChunks[i] = sql.identifier(chunk.name);
						}
					}

					orderByValues.push(sql`${singleOrderBy}`);
				} else {
					orderByValues.push(sql`${singleOrderBy}`);
				}
			}

			orderBySql = sql` ORDER BY ${sql.join(orderByValues, sql`, `)} `;
		}

		const limitSql = typeof limit === 'object' || (typeof limit === 'number' && limit >= 0)
			? sql` LIMIT ${limit}`
			: undefined;

		const operatorChunk = sql.raw(`${type} ${isAll ? 'ALL ' : ''}`);

		const offsetSql = offset ? sql` OFFSET ${offset}` : undefined;

		return sql`${leftChunk}${operatorChunk}${rightChunk}${orderBySql}${limitSql}${offsetSql}`;
	}

	buildInsertQuery(
		{ table, values: valuesOrSelect, onConflict: _onConflict, returning: _returning, withList, select }: SnowflakeInsertConfig,
	): SQL {
		const valuesSqlList: ((SQLChunk | SQL)[] | SQL)[] = [];
		const columns: Record<string, SnowflakeColumn> = table[Table.Symbol.Columns];

		const colEntries: [string, SnowflakeColumn][] = Object.entries(columns).filter(([_, col]) => !col.shouldDisableInsert());

		const insertOrder = colEntries.map(
			([, column]) => sql.identifier(this.casing.getColumnCasing(column)),
		);

		if (select) {
			const select = valuesOrSelect as SQL;

			if (is(select, SQL)) {
				valuesSqlList.push(select);
			} else {
				valuesSqlList.push((select as any).getSQL());
			}
		} else {
			const values = valuesOrSelect as Record<string, Param | SQL>[];
			valuesSqlList.push(sql.raw('VALUES '));

			for (const [valueIndex, value] of values.entries()) {
				const valueList: (SQLChunk | SQL)[] = [];
				for (const [fieldName, col] of colEntries) {
					const colValue = value[fieldName];
					if (colValue === undefined || (is(colValue, Param) && colValue.value === undefined)) {
						if (col.defaultFn !== undefined) {
							const defaultFnResult = col.defaultFn();
							const defaultValue = is(defaultFnResult, SQL) ? defaultFnResult : sql.param(defaultFnResult, col);
							valueList.push(defaultValue);
						} else if (!col.default && col.onUpdateFn !== undefined) {
							const onUpdateFnResult = col.onUpdateFn();
							const newValue = is(onUpdateFnResult, SQL) ? onUpdateFnResult : sql.param(onUpdateFnResult, col);
							valueList.push(newValue);
						} else {
							valueList.push(sql`DEFAULT`);
						}
					} else {
						valueList.push(colValue);
					}
				}

				valuesSqlList.push(valueList);
				if (valueIndex < values.length - 1) {
					valuesSqlList.push(sql`, `);
				}
			}
		}

		const withSql = this.buildWithCTE(withList);

		const valuesSql = sql.join(valuesSqlList);

		// Note: Snowflake doesn't support RETURNING in INSERT statements
		// The returning parameter is kept for API compatibility but ignored

		// Note: Snowflake doesn't support ON CONFLICT in INSERT statements
		// Use MERGE statement for upsert functionality instead
		// The onConflict parameter is kept for API compatibility but ignored

		return sql`${withSql}INSERT INTO ${table} ${insertOrder} ${valuesSql}`;
	}

	prepareTyping(encoder: DriverValueEncoder<unknown, unknown>): QueryTypingsValue {
		if (is(encoder, SnowflakeTimestamp) || is(encoder, SnowflakeTimestampString)) {
			return 'timestamp';
		} else if (is(encoder, SnowflakeDate) || is(encoder, SnowflakeDateString)) {
			return 'date';
		} else {
			return 'none';
		}
	}

	sqlToQuery(sql: SQL, invokeSource?: 'indexes' | undefined): QueryWithTypings {
		return sql.toQuery({
			casing: this.casing,
			escapeName: this.escapeName,
			escapeParam: this.escapeParam,
			escapeString: this.escapeString,
			prepareTyping: this.prepareTyping,
			invokeSource,
		});
	}
}
