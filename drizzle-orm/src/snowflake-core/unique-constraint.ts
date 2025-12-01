import { entityKind } from '~/entity.ts';
import type { SnowflakeColumn } from './columns/common.ts';
import { SnowflakeTable } from './table.ts';

export function uniqueKeyName(table: SnowflakeTable, columns: string[]): string {
	return `${table[SnowflakeTable.Symbol.Name]}_${columns.join('_')}_unique`;
}

export interface UniqueConstraintBuilderConfig {
	name: string;
	nullsNotDistinct: boolean;
	columns: SnowflakeColumn[];
}

export class UniqueConstraintBuilder {
	static readonly [entityKind]: string = 'SnowflakeUniqueConstraintBuilder';

	/** @internal */
	columns: SnowflakeColumn[];

	/** @internal */
	nullsNotDistinct = false;

	/** @internal */
	name?: string;

	constructor(
		columns: SnowflakeColumn[],
		name?: string,
	) {
		this.name = name;
		this.columns = columns;
	}

	nullsNotDistinctConfig(): this {
		this.nullsNotDistinct = true;
		return this;
	}

	/** @internal */
	build(table: SnowflakeTable): UniqueConstraint {
		return new UniqueConstraint(table, this.columns, this.nullsNotDistinct, this.name);
	}
}

export class UniqueConstraint {
	static readonly [entityKind]: string = 'SnowflakeUniqueConstraint';

	readonly columns: SnowflakeColumn[];
	readonly name?: string;
	readonly nullsNotDistinct: boolean;

	constructor(
		readonly table: SnowflakeTable,
		columns: SnowflakeColumn[],
		nullsNotDistinct: boolean,
		name?: string,
	) {
		this.columns = columns;
		this.name = name ?? uniqueKeyName(table, columns.map((c) => c.name));
		this.nullsNotDistinct = nullsNotDistinct;
	}

	getName(): string {
		return this.name!;
	}
}

export type AnyUniqueConstraintBuilder = UniqueConstraintBuilder;

export function unique(name?: string): UniqueOnConstraintBuilder {
	return new UniqueOnConstraintBuilder(name);
}

export class UniqueOnConstraintBuilder {
	static readonly [entityKind]: string = 'SnowflakeUniqueOnConstraintBuilder';

	/** @internal */
	name?: string;

	constructor(name?: string) {
		this.name = name;
	}

	on(...columns: [SnowflakeColumn, ...SnowflakeColumn[]]): UniqueConstraintBuilder {
		return new UniqueConstraintBuilder(columns, this.name);
	}
}
