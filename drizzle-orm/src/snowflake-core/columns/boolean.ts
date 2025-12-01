import type { ColumnBuilderBaseConfig, ColumnBuilderRuntimeConfig, MakeColumnConfig } from '~/column-builder.ts';
import type { ColumnBaseConfig } from '~/column.ts';
import { entityKind } from '~/entity.ts';
import type { AnySnowflakeTable } from '~/snowflake-core/table.ts';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export type SnowflakeBooleanBuilderInitial<TName extends string> = SnowflakeBooleanBuilder<{
	name: TName;
	dataType: 'boolean';
	columnType: 'SnowflakeBoolean';
	data: boolean;
	driverParam: boolean;
	enumValues: undefined;
}>;

export class SnowflakeBooleanBuilder<T extends ColumnBuilderBaseConfig<'boolean', 'SnowflakeBoolean'>>
	extends SnowflakeColumnBuilder<T>
{
	static override readonly [entityKind]: string = 'SnowflakeBooleanBuilder';

	constructor(name: T['name']) {
		super(name, 'boolean', 'SnowflakeBoolean');
	}

	/** @internal */
	override build<TTableName extends string>(
		table: AnySnowflakeTable<{ name: TTableName }>,
	): SnowflakeBoolean<MakeColumnConfig<T, TTableName>> {
		return new SnowflakeBoolean<MakeColumnConfig<T, TTableName>>(
			table,
			this.config as ColumnBuilderRuntimeConfig<any, any>,
		);
	}
}

export class SnowflakeBoolean<T extends ColumnBaseConfig<'boolean', 'SnowflakeBoolean'>>
	extends SnowflakeColumn<T>
{
	static override readonly [entityKind]: string = 'SnowflakeBoolean';

	getSQLType(): string {
		return 'boolean';
	}
}

export function boolean(): SnowflakeBooleanBuilderInitial<''>;
export function boolean<TName extends string>(name: TName): SnowflakeBooleanBuilderInitial<TName>;
export function boolean<TName extends string>(name?: TName): SnowflakeBooleanBuilderInitial<TName> {
	return new SnowflakeBooleanBuilder(name ?? '' as TName);
}
