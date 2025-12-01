import { entityKind } from '~/entity.ts';
import type { SnowflakeColumn } from './columns/common.ts';
import { SnowflakeTable } from './table.ts';

export function primaryKeyName(table: SnowflakeTable, columns: string[]): string {
	return `${table[SnowflakeTable.Symbol.Name]}_${columns.join('_')}_pk`;
}

export interface PrimaryKeyConfig {
	name?: string;
	columns: SnowflakeColumn[];
}

export class PrimaryKeyBuilder {
	static readonly [entityKind]: string = 'SnowflakePrimaryKeyBuilder';

	/** @internal */
	columns: SnowflakeColumn[];

	/** @internal */
	name?: string;

	constructor(
		columns: SnowflakeColumn[],
		name?: string,
	) {
		this.columns = columns;
		this.name = name;
	}

	/** @internal */
	build(table: SnowflakeTable): PrimaryKey {
		return new PrimaryKey(table, this.columns, this.name);
	}
}

export class PrimaryKey {
	static readonly [entityKind]: string = 'SnowflakePrimaryKey';

	readonly columns: SnowflakeColumn[];
	readonly name?: string;

	constructor(
		readonly table: SnowflakeTable,
		columns: SnowflakeColumn[],
		name?: string,
	) {
		this.columns = columns;
		this.name = name ?? primaryKeyName(table, columns.map((c) => c.name));
	}

	getName(): string {
		return this.name!;
	}
}

export function primaryKey<TColumns extends [SnowflakeColumn, ...SnowflakeColumn[]]>(
	config: { name?: string; columns: TColumns },
): PrimaryKeyBuilder;
export function primaryKey<TColumns extends SnowflakeColumn[]>(
	...columns: TColumns
): PrimaryKeyBuilder;
export function primaryKey(...args: unknown[]): PrimaryKeyBuilder {
	if (args.length === 1 && typeof args[0] === 'object' && 'columns' in args[0]!) {
		const { columns, name } = args[0] as { columns: SnowflakeColumn[]; name?: string };
		return new PrimaryKeyBuilder(columns, name);
	}
	return new PrimaryKeyBuilder(args as SnowflakeColumn[]);
}
