import type { ColumnBuilderBaseConfig, ColumnBuilderRuntimeConfig, MakeColumnConfig } from '~/column-builder.ts';
import type { ColumnBaseConfig } from '~/column.ts';
import { entityKind } from '~/entity.ts';
import type { AnySnowflakeTable } from '~/snowflake-core/table.ts';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export type SnowflakeDateBuilderInitial<TName extends string> = SnowflakeDateBuilder<{
	name: TName;
	dataType: 'date';
	columnType: 'SnowflakeDate';
	data: Date;
	driverParam: string;
	enumValues: undefined;
}>;

export class SnowflakeDateBuilder<T extends ColumnBuilderBaseConfig<'date', 'SnowflakeDate'>>
	extends SnowflakeColumnBuilder<T, SnowflakeDateConfig>
{
	static override readonly [entityKind]: string = 'SnowflakeDateBuilder';

	constructor(name: T['name'], config: SnowflakeDateConfig = {}) {
		super(name, 'date', 'SnowflakeDate');
		this.config.mode = config.mode ?? 'date';
	}

	/** @internal */
	override build<TTableName extends string>(
		table: AnySnowflakeTable<{ name: TTableName }>,
	): SnowflakeDate<MakeColumnConfig<T, TTableName>> {
		return new SnowflakeDate<MakeColumnConfig<T, TTableName>>(
			table,
			this.config as ColumnBuilderRuntimeConfig<any, any>,
		);
	}
}

export class SnowflakeDate<T extends ColumnBaseConfig<'date', 'SnowflakeDate'>>
	extends SnowflakeColumn<T, SnowflakeDateConfig>
{
	static override readonly [entityKind]: string = 'SnowflakeDate';

	getSQLType(): string {
		return 'date';
	}

	override mapFromDriverValue(value: string): Date {
		return new Date(value);
	}

	override mapToDriverValue(value: Date): string {
		return value.toISOString().split('T')[0]!;
	}
}

export interface SnowflakeDateConfig {
	mode?: 'date' | 'string';
}

export type SnowflakeDateStringBuilderInitial<TName extends string> = SnowflakeDateStringBuilder<{
	name: TName;
	dataType: 'string';
	columnType: 'SnowflakeDateString';
	data: string;
	driverParam: string;
	enumValues: undefined;
}>;

export class SnowflakeDateStringBuilder<T extends ColumnBuilderBaseConfig<'string', 'SnowflakeDateString'>>
	extends SnowflakeColumnBuilder<T, SnowflakeDateConfig>
{
	static override readonly [entityKind]: string = 'SnowflakeDateStringBuilder';

	constructor(name: T['name']) {
		super(name, 'string', 'SnowflakeDateString');
	}

	/** @internal */
	override build<TTableName extends string>(
		table: AnySnowflakeTable<{ name: TTableName }>,
	): SnowflakeDateString<MakeColumnConfig<T, TTableName>> {
		return new SnowflakeDateString<MakeColumnConfig<T, TTableName>>(
			table,
			this.config as ColumnBuilderRuntimeConfig<any, any>,
		);
	}
}

export class SnowflakeDateString<T extends ColumnBaseConfig<'string', 'SnowflakeDateString'>>
	extends SnowflakeColumn<T, SnowflakeDateConfig>
{
	static override readonly [entityKind]: string = 'SnowflakeDateString';

	getSQLType(): string {
		return 'date';
	}
}

export function date(): SnowflakeDateBuilderInitial<''>;
export function date<TName extends string>(
	name: TName,
	config?: SnowflakeDateConfig & { mode?: 'date' },
): SnowflakeDateBuilderInitial<TName>;
export function date<TName extends string>(
	name: TName,
	config: SnowflakeDateConfig & { mode: 'string' },
): SnowflakeDateStringBuilderInitial<TName>;
export function date(
	config?: SnowflakeDateConfig & { mode?: 'date' },
): SnowflakeDateBuilderInitial<''>;
export function date(
	config: SnowflakeDateConfig & { mode: 'string' },
): SnowflakeDateStringBuilderInitial<''>;
export function date(
	a?: string | SnowflakeDateConfig,
	b: SnowflakeDateConfig = {},
) {
	const { name, config } = getSnowflakeColumnBuilderNameAndConfig<SnowflakeDateConfig | undefined>(a, b);
	if (config?.mode === 'string') {
		return new SnowflakeDateStringBuilder(name) as SnowflakeDateStringBuilderInitial<string>;
	}
	return new SnowflakeDateBuilder(name, config) as SnowflakeDateBuilderInitial<string>;
}

function getSnowflakeColumnBuilderNameAndConfig<TConfig extends object | undefined>(
	a?: string | TConfig,
	b?: TConfig,
): { name: string; config: TConfig } {
	if (typeof a === 'string') {
		return { name: a, config: b ?? {} as TConfig };
	}
	return { name: '', config: a ?? {} as TConfig };
}
