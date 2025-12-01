import type { ColumnBuilderBaseConfig, ColumnBuilderRuntimeConfig, MakeColumnConfig } from '~/column-builder.ts';
import type { ColumnBaseConfig } from '~/column.ts';
import { entityKind } from '~/entity.ts';
import type { AnySnowflakeTable } from '~/snowflake-core/table.ts';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export type SnowflakeNumberBuilderInitial<TName extends string> = SnowflakeNumberBuilder<{
	name: TName;
	dataType: 'number';
	columnType: 'SnowflakeNumber';
	data: number;
	driverParam: number | string;
	enumValues: undefined;
}>;

export class SnowflakeNumberBuilder<T extends ColumnBuilderBaseConfig<'number', 'SnowflakeNumber'>>
	extends SnowflakeColumnBuilder<T, SnowflakeNumberConfig>
{
	static override readonly [entityKind]: string = 'SnowflakeNumberBuilder';

	constructor(name: T['name'], config: SnowflakeNumberConfig = {}) {
		super(name, 'number', 'SnowflakeNumber');
		this.config.precision = config.precision;
		this.config.scale = config.scale;
	}

	/** @internal */
	override build<TTableName extends string>(
		table: AnySnowflakeTable<{ name: TTableName }>,
	): SnowflakeNumber<MakeColumnConfig<T, TTableName>> {
		return new SnowflakeNumber<MakeColumnConfig<T, TTableName>>(
			table,
			this.config as ColumnBuilderRuntimeConfig<any, any>,
		);
	}
}

export class SnowflakeNumber<T extends ColumnBaseConfig<'number', 'SnowflakeNumber'>>
	extends SnowflakeColumn<T, SnowflakeNumberConfig>
{
	static override readonly [entityKind]: string = 'SnowflakeNumber';

	readonly precision: number | undefined = this.config.precision;
	readonly scale: number | undefined = this.config.scale;

	getSQLType(): string {
		if (this.precision !== undefined && this.scale !== undefined) {
			return `number(${this.precision}, ${this.scale})`;
		}
		if (this.precision !== undefined) {
			return `number(${this.precision})`;
		}
		return 'number';
	}
}

export interface SnowflakeNumberConfig {
	precision?: number;
	scale?: number;
}

export function number(): SnowflakeNumberBuilderInitial<''>;
export function number<TName extends string>(
	name: TName,
	config?: SnowflakeNumberConfig,
): SnowflakeNumberBuilderInitial<TName>;
export function number(config?: SnowflakeNumberConfig): SnowflakeNumberBuilderInitial<''>;
export function number<TName extends string>(
	a?: TName | SnowflakeNumberConfig,
	b: SnowflakeNumberConfig = {},
): SnowflakeNumberBuilderInitial<TName> {
	const { name, config } = getSnowflakeColumnBuilderNameAndConfig<TName, SnowflakeNumberConfig>(a, b);
	return new SnowflakeNumberBuilder(name, config);
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
