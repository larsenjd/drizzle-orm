import type { BuildColumns, BuildExtraConfigColumns } from '~/column-builder.ts';
import { entityKind } from '~/entity.ts';
import { Table, type TableConfig as TableConfigBase, type UpdateTableConfig } from '~/table.ts';
import type { CheckBuilder } from './checks.ts';
import type { SnowflakeColumn, SnowflakeColumnBuilder, SnowflakeColumnBuilderBase } from './columns/common.ts';
import type { ForeignKey, ForeignKeyBuilder } from './foreign-keys.ts';
import type { AnyIndexBuilder } from './indexes.ts';
import type { PrimaryKeyBuilder } from './primary-keys.ts';
import type { UniqueConstraintBuilder } from './unique-constraint.ts';

export type SnowflakeTableExtraConfigValue =
	| AnyIndexBuilder
	| CheckBuilder
	| ForeignKeyBuilder
	| PrimaryKeyBuilder
	| UniqueConstraintBuilder;

export type SnowflakeTableExtraConfig = Record<
	string,
	SnowflakeTableExtraConfigValue
>;

export type TableConfig = TableConfigBase<SnowflakeColumn>;

/** @internal */
export const InlineForeignKeys = Symbol.for('drizzle:SnowflakeInlineForeignKeys');

export class SnowflakeTable<T extends TableConfig = TableConfig> extends Table<T> {
	static override readonly [entityKind]: string = 'SnowflakeTable';

	/** @internal */
	static override readonly Symbol = Object.assign({}, Table.Symbol, {
		InlineForeignKeys: InlineForeignKeys as typeof InlineForeignKeys,
	});

	/**@internal */
	[InlineForeignKeys]: ForeignKey[] = [];

	/** @internal */
	override [Table.Symbol.ExtraConfigBuilder]:
		| ((self: Record<string, SnowflakeColumn>) => SnowflakeTableExtraConfig)
		| undefined = undefined;
}

export type AnySnowflakeTable<TPartial extends Partial<TableConfig> = {}> = SnowflakeTable<
	UpdateTableConfig<TableConfig, TPartial>
>;

export type SnowflakeTableWithColumns<T extends TableConfig> =
	& SnowflakeTable<T>
	& {
		[Key in keyof T['columns']]: T['columns'][Key];
	};

/** @internal */
export function snowflakeTableWithSchema<
	TTableName extends string,
	TSchemaName extends string | undefined,
	TColumnsMap extends Record<string, SnowflakeColumnBuilderBase>,
>(
	name: TTableName,
	columns: TColumnsMap,
	extraConfig:
		| ((
			self: BuildExtraConfigColumns<TTableName, TColumnsMap, 'snowflake'>,
		) => SnowflakeTableExtraConfig | SnowflakeTableExtraConfigValue[])
		| undefined,
	schema: TSchemaName,
	baseName = name,
): SnowflakeTableWithColumns<{
	name: TTableName;
	schema: TSchemaName;
	columns: BuildColumns<TTableName, TColumnsMap, 'snowflake'>;
	dialect: 'snowflake';
}> {
	const rawTable = new SnowflakeTable<{
		name: TTableName;
		schema: TSchemaName;
		columns: BuildColumns<TTableName, TColumnsMap, 'snowflake'>;
		dialect: 'snowflake';
	}>(name, schema, baseName);

	const builtColumns = Object.fromEntries(
		Object.entries(columns).map(([name, colBuilderBase]) => {
			const colBuilder = colBuilderBase as SnowflakeColumnBuilder;
			colBuilder.setName(name);
			const column = colBuilder.build(rawTable);
			rawTable[InlineForeignKeys].push(...colBuilder.buildForeignKeys(column, rawTable));
			return [name, column];
		}),
	) as unknown as BuildColumns<TTableName, TColumnsMap, 'snowflake'>;

	const table = Object.assign(rawTable, builtColumns);

	table[Table.Symbol.Columns] = builtColumns;
	table[Table.Symbol.ExtraConfigColumns] = builtColumns as unknown as BuildExtraConfigColumns<TTableName, TColumnsMap, 'snowflake'>;

	if (extraConfig) {
		table[SnowflakeTable.Symbol.ExtraConfigBuilder] = extraConfig as unknown as (self: Record<string, SnowflakeColumn>) => SnowflakeTableExtraConfig;
	}

	return table as SnowflakeTableWithColumns<{
		name: TTableName;
		schema: TSchemaName;
		columns: BuildColumns<TTableName, TColumnsMap, 'snowflake'>;
		dialect: 'snowflake';
	}>;
}

export interface SnowflakeTableFn<TSchema extends string | undefined = undefined> {
	<
		TTableName extends string,
		TColumnsMap extends Record<string, SnowflakeColumnBuilderBase>,
	>(
		name: TTableName,
		columns: TColumnsMap,
		extraConfig?: (
			self: BuildExtraConfigColumns<TTableName, TColumnsMap, 'snowflake'>,
		) => SnowflakeTableExtraConfigValue[],
	): SnowflakeTableWithColumns<{
		name: TTableName;
		schema: TSchema;
		columns: BuildColumns<TTableName, TColumnsMap, 'snowflake'>;
		dialect: 'snowflake';
	}>;
}

export const snowflakeTable: SnowflakeTableFn = (name, columns, extraConfig) => {
	return snowflakeTableWithSchema(name, columns, extraConfig, undefined);
};

export function snowflakeTableCreator(customizeTableName: (name: string) => string): SnowflakeTableFn {
	return (name, columns, extraConfig) => {
		return snowflakeTableWithSchema(customizeTableName(name) as typeof name, columns, extraConfig, undefined, name);
	};
}
