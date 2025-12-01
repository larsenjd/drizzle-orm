import { entityKind, is } from '~/entity.ts';
import { SnowflakeTable, snowflakeTableWithSchema, type SnowflakeTableFn } from './table.ts';

export class SnowflakeSchema<TName extends string = string> {
	static readonly [entityKind]: string = 'SnowflakeSchema';

	constructor(
		public readonly schemaName: TName,
	) {}

	table: SnowflakeTableFn<TName> = (name, columns, extraConfig) => {
		return snowflakeTableWithSchema(name, columns, extraConfig, this.schemaName);
	};
}

export function snowflakeSchema<TName extends string>(name: TName): SnowflakeSchema<TName> {
	return new SnowflakeSchema(name);
}

export function isSnowflakeSchema(obj: unknown): obj is SnowflakeSchema {
	return is(obj, SnowflakeSchema);
}
