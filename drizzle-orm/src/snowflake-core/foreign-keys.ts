import { entityKind } from '~/entity.ts';
import { SnowflakeTable } from './table.ts';

export type UpdateDeleteAction = 'cascade' | 'restrict' | 'no action' | 'set null' | 'set default';

export interface ForeignKeyBuilderConfig {
	columns: () => { columns: SnowflakeColumn[] };
	foreignColumns: () => { columns: SnowflakeColumn[] };
}

export type Reference = () => {
	readonly columns: SnowflakeColumn[];
	readonly foreignTable: SnowflakeTable;
	readonly foreignColumns: SnowflakeColumn[];
};

export class ForeignKeyBuilder {
	static readonly [entityKind]: string = 'SnowflakeForeignKeyBuilder';

	/** @internal */
	reference: Reference;

	/** @internal */
	_onUpdate: UpdateDeleteAction | undefined = 'no action';

	/** @internal */
	_onDelete: UpdateDeleteAction | undefined = 'no action';

	constructor(
		config: () => {
			columns: SnowflakeColumn[];
			foreignColumns: SnowflakeColumn[];
		},
		private actions?: {
			onUpdate?: UpdateDeleteAction;
			onDelete?: UpdateDeleteAction;
		} | undefined,
	) {
		this.reference = () => {
			const { columns, foreignColumns } = config();
			return { columns, foreignTable: foreignColumns[0]!.table as SnowflakeTable, foreignColumns };
		};
		if (actions) {
			this._onUpdate = actions.onUpdate;
			this._onDelete = actions.onDelete;
		}
	}

	onUpdate(action: UpdateDeleteAction): this {
		this._onUpdate = action;
		return this;
	}

	onDelete(action: UpdateDeleteAction): this {
		this._onDelete = action;
		return this;
	}

	/** @internal */
	build(table: SnowflakeTable): ForeignKey {
		return new ForeignKey(table, this);
	}
}

export type AnyForeignKeyBuilder = ForeignKeyBuilder;

export class ForeignKey {
	static readonly [entityKind]: string = 'SnowflakeForeignKey';

	readonly reference: Reference;
	readonly onUpdate: UpdateDeleteAction | undefined;
	readonly onDelete: UpdateDeleteAction | undefined;

	constructor(
		readonly table: SnowflakeTable,
		builder: ForeignKeyBuilder,
	) {
		this.reference = builder.reference;
		this.onUpdate = builder._onUpdate;
		this.onDelete = builder._onDelete;
	}

	getName(): string {
		const { columns, foreignColumns } = this.reference();
		const columnNames = columns.map((column) => column.name);
		const foreignColumnNames = foreignColumns.map((column) => column.name);
		const chunks = [
			this.table[SnowflakeTable.Symbol.Name],
			...columnNames,
			foreignColumns[0]!.table[SnowflakeTable.Symbol.Name],
			...foreignColumnNames,
		];
		return `${chunks.join('_')}_fk`;
	}
}

import type { SnowflakeColumn } from './columns/common.ts';

type ColumnsWithTable<
	_TTableName extends string,
	TColumns extends SnowflakeColumn[],
> = { [Key in keyof TColumns]: TColumns[Key] };

export type GetColumnsTable<TColumns extends SnowflakeColumn | SnowflakeColumn[]> = (
	TColumns extends SnowflakeColumn ? TColumns['table']['$inferSelect']
		: TColumns extends SnowflakeColumn[] ? TColumns[number]['table']['$inferSelect']
		: never
) extends infer TTable extends Record<string, unknown> ? TTable : never;

export function foreignKey<
	TTableName extends string,
	TColumns extends [SnowflakeColumn, ...SnowflakeColumn[]],
	TForeignColumns extends ColumnsWithTable<TTableName, TColumns>,
>(
	config: {
		name?: string;
		columns: TColumns;
		foreignColumns: TForeignColumns;
	},
): ForeignKeyBuilder {
	function mappedConfig(): {
		columns: SnowflakeColumn[];
		foreignColumns: SnowflakeColumn[];
	} {
		const { columns, foreignColumns } = config;
		return {
			columns: columns as SnowflakeColumn[],
			foreignColumns: foreignColumns as SnowflakeColumn[],
		};
	}

	return new ForeignKeyBuilder(mappedConfig);
}
