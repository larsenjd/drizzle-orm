import { entityKind } from '~/entity.ts';
import type { SQL } from '~/sql/sql.ts';
import type { SnowflakeColumn } from './columns/common.ts';
import type { SnowflakeTable } from './table.ts';

export interface IndexConfig {
	name: string;
	columns: IndexedColumn[];
	unique: boolean;
	where?: SQL;
}

export type IndexColumn = SnowflakeColumn | SQL;

export interface IndexedColumn {
	column: IndexColumn;
	order?: 'asc' | 'desc';
	nulls?: 'first' | 'last';
}

export class IndexBuilderOn {
	static readonly [entityKind]: string = 'SnowflakeIndexBuilderOn';

	constructor(
		private name: string,
		private unique: boolean,
	) {}

	on(...columns: [IndexColumn, ...IndexColumn[]]): IndexBuilder {
		return new IndexBuilder(this.name, columns, this.unique);
	}
}

export class IndexBuilder {
	static readonly [entityKind]: string = 'SnowflakeIndexBuilder';

	/** @internal */
	config: IndexConfig;

	constructor(
		name: string,
		columns: IndexColumn[],
		unique: boolean,
	) {
		this.config = {
			name,
			columns: columns.map((column) => ({
				column,
				order: 'asc',
				nulls: 'last',
			})),
			unique,
		};
	}

	asc(): this {
		for (const column of this.config.columns) {
			column.order = 'asc';
		}
		return this;
	}

	desc(): this {
		for (const column of this.config.columns) {
			column.order = 'desc';
		}
		return this;
	}

	nullsFirst(): this {
		for (const column of this.config.columns) {
			column.nulls = 'first';
		}
		return this;
	}

	nullsLast(): this {
		for (const column of this.config.columns) {
			column.nulls = 'last';
		}
		return this;
	}

	where(condition: SQL): this {
		this.config.where = condition;
		return this;
	}

	/** @internal */
	build(table: SnowflakeTable): Index {
		return new Index(this.config, table);
	}
}

export class Index {
	static readonly [entityKind]: string = 'SnowflakeIndex';

	readonly config: IndexConfig & { table: SnowflakeTable };

	constructor(
		config: IndexConfig,
		table: SnowflakeTable,
	) {
		this.config = { ...config, table };
	}
}

export type AnyIndexBuilder = IndexBuilder;

export function index(name: string): IndexBuilderOn {
	return new IndexBuilderOn(name, false);
}

export function uniqueIndex(name: string): IndexBuilderOn {
	return new IndexBuilderOn(name, true);
}
