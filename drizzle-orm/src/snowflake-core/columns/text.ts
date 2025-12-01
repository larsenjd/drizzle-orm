import type { ColumnBuilderBaseConfig, ColumnBuilderRuntimeConfig, MakeColumnConfig } from '~/column-builder.ts';
import type { ColumnBaseConfig } from '~/column.ts';
import { entityKind } from '~/entity.ts';
import type { AnySnowflakeTable } from '~/snowflake-core/table.ts';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export type SnowflakeTextBuilderInitial<TName extends string> = SnowflakeTextBuilder<{
	name: TName;
	dataType: 'string';
	columnType: 'SnowflakeText';
	data: string;
	driverParam: string;
	enumValues: undefined;
}>;

export class SnowflakeTextBuilder<T extends ColumnBuilderBaseConfig<'string', 'SnowflakeText'>>
	extends SnowflakeColumnBuilder<T>
{
	static override readonly [entityKind]: string = 'SnowflakeTextBuilder';

	constructor(name: T['name']) {
		super(name, 'string', 'SnowflakeText');
	}

	/** @internal */
	override build<TTableName extends string>(
		table: AnySnowflakeTable<{ name: TTableName }>,
	): SnowflakeText<MakeColumnConfig<T, TTableName>> {
		return new SnowflakeText<MakeColumnConfig<T, TTableName>>(
			table,
			this.config as ColumnBuilderRuntimeConfig<any, any>,
		);
	}
}

export class SnowflakeText<T extends ColumnBaseConfig<'string', 'SnowflakeText'>>
	extends SnowflakeColumn<T>
{
	static override readonly [entityKind]: string = 'SnowflakeText';

	getSQLType(): string {
		return 'text';
	}
}

export function text(): SnowflakeTextBuilderInitial<''>;
export function text<TName extends string>(name: TName): SnowflakeTextBuilderInitial<TName>;
export function text<TName extends string>(name?: TName): SnowflakeTextBuilderInitial<TName> {
	return new SnowflakeTextBuilder(name ?? '' as TName);
}
