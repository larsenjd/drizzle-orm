import type { ColumnBuilderBaseConfig, ColumnBuilderRuntimeConfig, MakeColumnConfig } from '~/column-builder.ts';
import type { ColumnBaseConfig } from '~/column.ts';
import { entityKind } from '~/entity.ts';
import type { AnySnowflakeTable } from '~/snowflake-core/table.ts';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export type SnowflakeIntegerBuilderInitial<TName extends string> = SnowflakeIntegerBuilder<{
	name: TName;
	dataType: 'number';
	columnType: 'SnowflakeInteger';
	data: number;
	driverParam: number;
	enumValues: undefined;
}>;

export class SnowflakeIntegerBuilder<T extends ColumnBuilderBaseConfig<'number', 'SnowflakeInteger'>>
	extends SnowflakeColumnBuilder<T>
{
	static override readonly [entityKind]: string = 'SnowflakeIntegerBuilder';

	constructor(name: T['name']) {
		super(name, 'number', 'SnowflakeInteger');
	}

	/** @internal */
	override build<TTableName extends string>(
		table: AnySnowflakeTable<{ name: TTableName }>,
	): SnowflakeInteger<MakeColumnConfig<T, TTableName>> {
		return new SnowflakeInteger<MakeColumnConfig<T, TTableName>>(
			table,
			this.config as ColumnBuilderRuntimeConfig<any, any>,
		);
	}
}

export class SnowflakeInteger<T extends ColumnBaseConfig<'number', 'SnowflakeInteger'>>
	extends SnowflakeColumn<T>
{
	static override readonly [entityKind]: string = 'SnowflakeInteger';

	getSQLType(): string {
		return 'integer';
	}
}

export function integer(): SnowflakeIntegerBuilderInitial<''>;
export function integer<TName extends string>(name: TName): SnowflakeIntegerBuilderInitial<TName>;
export function integer<TName extends string>(name?: TName): SnowflakeIntegerBuilderInitial<TName> {
	return new SnowflakeIntegerBuilder(name ?? '' as TName);
}
