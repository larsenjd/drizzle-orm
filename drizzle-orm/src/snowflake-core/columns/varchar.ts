import type { ColumnBuilderBaseConfig, ColumnBuilderRuntimeConfig, MakeColumnConfig } from '~/column-builder.ts';
import type { ColumnBaseConfig } from '~/column.ts';
import { entityKind } from '~/entity.ts';
import type { AnySnowflakeTable } from '~/snowflake-core/table.ts';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export type SnowflakeVarcharBuilderInitial<TName extends string> = SnowflakeVarcharBuilder<{
	name: TName;
	dataType: 'string';
	columnType: 'SnowflakeVarchar';
	data: string;
	driverParam: string;
	enumValues: undefined;
}>;

export class SnowflakeVarcharBuilder<T extends ColumnBuilderBaseConfig<'string', 'SnowflakeVarchar'>>
	extends SnowflakeColumnBuilder<T, SnowflakeVarcharConfig>
{
	static override readonly [entityKind]: string = 'SnowflakeVarcharBuilder';

	constructor(name: T['name'], config: SnowflakeVarcharConfig) {
		super(name, 'string', 'SnowflakeVarchar');
		this.config.length = config.length;
	}

	/** @internal */
	override build<TTableName extends string>(
		table: AnySnowflakeTable<{ name: TTableName }>,
	): SnowflakeVarchar<MakeColumnConfig<T, TTableName>> {
		return new SnowflakeVarchar<MakeColumnConfig<T, TTableName>>(
			table,
			this.config as ColumnBuilderRuntimeConfig<any, any>,
		);
	}
}

export class SnowflakeVarchar<T extends ColumnBaseConfig<'string', 'SnowflakeVarchar'>>
	extends SnowflakeColumn<T, SnowflakeVarcharConfig>
{
	static override readonly [entityKind]: string = 'SnowflakeVarchar';

	readonly length: number | undefined = this.config.length;

	getSQLType(): string {
		return this.length === undefined ? `varchar` : `varchar(${this.length})`;
	}
}

export interface SnowflakeVarcharConfig {
	length?: number;
}

export function varchar(): SnowflakeVarcharBuilderInitial<''>;
export function varchar<TName extends string>(
	name: TName,
	config?: SnowflakeVarcharConfig,
): SnowflakeVarcharBuilderInitial<TName>;
export function varchar(config?: SnowflakeVarcharConfig): SnowflakeVarcharBuilderInitial<''>;
export function varchar<TName extends string>(
	a?: TName | SnowflakeVarcharConfig,
	b: SnowflakeVarcharConfig = {},
): SnowflakeVarcharBuilderInitial<TName> {
	const { name, config } = getSnowflakeColumnBuilderNameAndConfig<TName, SnowflakeVarcharConfig>(a, b);
	return new SnowflakeVarcharBuilder(name, config);
}

function getSnowflakeColumnBuilderNameAndConfig<TName extends string, TConfig extends object>(
	a?: TName | TConfig,
	b?: TConfig,
): { name: TName; config: TConfig } {
	if (typeof a === 'string') {
		return { name: a, config: b ?? {} as TConfig };
	}
	return { name: '' as TName, config: a ?? {} as TConfig };
}
