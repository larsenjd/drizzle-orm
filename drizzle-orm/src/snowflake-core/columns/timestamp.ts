import type { ColumnBuilderBaseConfig, ColumnBuilderRuntimeConfig, MakeColumnConfig } from '~/column-builder.ts';
import type { ColumnBaseConfig } from '~/column.ts';
import { entityKind } from '~/entity.ts';
import type { AnySnowflakeTable } from '~/snowflake-core/table.ts';
import { SnowflakeColumn, SnowflakeColumnBuilder } from './common.ts';

export type SnowflakeTimestampBuilderInitial<TName extends string> = SnowflakeTimestampBuilder<{
	name: TName;
	dataType: 'date';
	columnType: 'SnowflakeTimestamp';
	data: Date;
	driverParam: string;
	enumValues: undefined;
}>;

export class SnowflakeTimestampBuilder<T extends ColumnBuilderBaseConfig<'date', 'SnowflakeTimestamp'>>
	extends SnowflakeColumnBuilder<T, SnowflakeTimestampConfig>
{
	static override readonly [entityKind]: string = 'SnowflakeTimestampBuilder';

	constructor(name: T['name'], config: SnowflakeTimestampConfig = {}) {
		super(name, 'date', 'SnowflakeTimestamp');
		this.config.withTimezone = config.withTimezone ?? false;
		this.config.precision = config.precision;
		this.config.mode = config.mode ?? 'date';
	}

	/** @internal */
	override build<TTableName extends string>(
		table: AnySnowflakeTable<{ name: TTableName }>,
	): SnowflakeTimestamp<MakeColumnConfig<T, TTableName>> {
		return new SnowflakeTimestamp<MakeColumnConfig<T, TTableName>>(
			table,
			this.config as ColumnBuilderRuntimeConfig<any, any>,
		);
	}
}

export class SnowflakeTimestamp<T extends ColumnBaseConfig<'date', 'SnowflakeTimestamp'>>
	extends SnowflakeColumn<T, SnowflakeTimestampConfig>
{
	static override readonly [entityKind]: string = 'SnowflakeTimestamp';

	readonly withTimezone: boolean = this.config.withTimezone ?? false;
	readonly precision: number | undefined = this.config.precision;

	getSQLType(): string {
		const precision = this.precision === undefined ? '' : `(${this.precision})`;
		if (this.withTimezone) {
			return `timestamp_tz${precision}`;
		}
		return `timestamp_ntz${precision}`;
	}

	override mapFromDriverValue(value: string): Date {
		return new Date(value);
	}

	override mapToDriverValue(value: Date): string {
		return value.toISOString();
	}
}

export interface SnowflakeTimestampConfig {
	withTimezone?: boolean;
	precision?: number;
	mode?: 'date' | 'string';
}

export type SnowflakeTimestampStringBuilderInitial<TName extends string> = SnowflakeTimestampStringBuilder<{
	name: TName;
	dataType: 'string';
	columnType: 'SnowflakeTimestampString';
	data: string;
	driverParam: string;
	enumValues: undefined;
}>;

export class SnowflakeTimestampStringBuilder<T extends ColumnBuilderBaseConfig<'string', 'SnowflakeTimestampString'>>
	extends SnowflakeColumnBuilder<T, SnowflakeTimestampConfig>
{
	static override readonly [entityKind]: string = 'SnowflakeTimestampStringBuilder';

	constructor(name: T['name'], config: SnowflakeTimestampConfig = {}) {
		super(name, 'string', 'SnowflakeTimestampString');
		this.config.withTimezone = config.withTimezone ?? false;
		this.config.precision = config.precision;
	}

	/** @internal */
	override build<TTableName extends string>(
		table: AnySnowflakeTable<{ name: TTableName }>,
	): SnowflakeTimestampString<MakeColumnConfig<T, TTableName>> {
		return new SnowflakeTimestampString<MakeColumnConfig<T, TTableName>>(
			table,
			this.config as ColumnBuilderRuntimeConfig<any, any>,
		);
	}
}

export class SnowflakeTimestampString<T extends ColumnBaseConfig<'string', 'SnowflakeTimestampString'>>
	extends SnowflakeColumn<T, SnowflakeTimestampConfig>
{
	static override readonly [entityKind]: string = 'SnowflakeTimestampString';

	readonly withTimezone: boolean = this.config.withTimezone ?? false;
	readonly precision: number | undefined = this.config.precision;

	getSQLType(): string {
		const precision = this.precision === undefined ? '' : `(${this.precision})`;
		if (this.withTimezone) {
			return `timestamp_tz${precision}`;
		}
		return `timestamp_ntz${precision}`;
	}
}

export function timestamp(): SnowflakeTimestampBuilderInitial<''>;
export function timestamp<TName extends string>(
	name: TName,
	config?: SnowflakeTimestampConfig & { mode?: 'date' },
): SnowflakeTimestampBuilderInitial<TName>;
export function timestamp<TName extends string>(
	name: TName,
	config: SnowflakeTimestampConfig & { mode: 'string' },
): SnowflakeTimestampStringBuilderInitial<TName>;
export function timestamp(
	config?: SnowflakeTimestampConfig & { mode?: 'date' },
): SnowflakeTimestampBuilderInitial<''>;
export function timestamp(
	config: SnowflakeTimestampConfig & { mode: 'string' },
): SnowflakeTimestampStringBuilderInitial<''>;
export function timestamp(
	a?: string | SnowflakeTimestampConfig,
	b: SnowflakeTimestampConfig = {},
) {
	const { name, config } = getSnowflakeColumnBuilderNameAndConfig<SnowflakeTimestampConfig | undefined>(a, b);
	if (config?.mode === 'string') {
		return new SnowflakeTimestampStringBuilder(name, config) as SnowflakeTimestampStringBuilderInitial<string>;
	}
	return new SnowflakeTimestampBuilder(name, config) as SnowflakeTimestampBuilderInitial<string>;
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
