import type {
	ColumnBuilderBase,
	ColumnBuilderBaseConfig,
	ColumnBuilderExtraConfig,
	ColumnBuilderRuntimeConfig,
	ColumnDataType,
	HasGenerated,
	MakeColumnConfig,
} from '~/column-builder.ts';
import { ColumnBuilder } from '~/column-builder.ts';
import type { ColumnBaseConfig } from '~/column.ts';
import { Column } from '~/column.ts';
import { entityKind } from '~/entity.ts';
import type { Update } from '~/utils.ts';

import type { ForeignKey, UpdateDeleteAction } from '~/snowflake-core/foreign-keys.ts';
import { ForeignKeyBuilder } from '~/snowflake-core/foreign-keys.ts';
import type { AnySnowflakeTable, SnowflakeTable } from '~/snowflake-core/table.ts';
import type { SQL } from '~/sql/sql.ts';
import { iife } from '~/tracing-utils.ts';
import { uniqueKeyName } from '../unique-constraint.ts';

export interface ReferenceConfig {
	ref: () => SnowflakeColumn;
	actions: {
		onUpdate?: UpdateDeleteAction;
		onDelete?: UpdateDeleteAction;
	};
}

export interface SnowflakeColumnBuilderBase<
	T extends ColumnBuilderBaseConfig<ColumnDataType, string> = ColumnBuilderBaseConfig<ColumnDataType, string>,
	TTypeConfig extends object = object,
> extends ColumnBuilderBase<T, TTypeConfig & { dialect: 'snowflake' }> {}

export abstract class SnowflakeColumnBuilder<
	T extends ColumnBuilderBaseConfig<ColumnDataType, string> = ColumnBuilderBaseConfig<ColumnDataType, string>,
	TRuntimeConfig extends object = object,
	TTypeConfig extends object = object,
	TExtraConfig extends ColumnBuilderExtraConfig = ColumnBuilderExtraConfig,
> extends ColumnBuilder<T, TRuntimeConfig, TTypeConfig & { dialect: 'snowflake' }, TExtraConfig>
	implements SnowflakeColumnBuilderBase<T, TTypeConfig>
{
	private foreignKeyConfigs: ReferenceConfig[] = [];

	static override readonly [entityKind]: string = 'SnowflakeColumnBuilder';

	references(
		ref: ReferenceConfig['ref'],
		actions: ReferenceConfig['actions'] = {},
	): this {
		this.foreignKeyConfigs.push({ ref, actions });
		return this;
	}

	unique(
		name?: string,
		config?: { nulls: 'distinct' | 'not distinct' },
	): this {
		this.config.isUnique = true;
		this.config.uniqueName = name;
		this.config.uniqueType = config?.nulls;
		return this;
	}

	generatedAlwaysAs(as: SQL | T['data'] | (() => SQL)): HasGenerated<this, {
		type: 'always';
	}> {
		this.config.generated = {
			as,
			type: 'always',
			mode: 'stored',
		};
		return this as HasGenerated<this, {
			type: 'always';
		}>;
	}

	/** @internal */
	buildForeignKeys(column: SnowflakeColumn, table: SnowflakeTable): ForeignKey[] {
		return this.foreignKeyConfigs.map(({ ref, actions }) => {
			return iife(
				(ref, actions) => {
					const builder = new ForeignKeyBuilder(() => {
						const foreignColumn = ref();
						return { columns: [column], foreignColumns: [foreignColumn] };
					});
					if (actions.onUpdate) {
						builder.onUpdate(actions.onUpdate);
					}
					if (actions.onDelete) {
						builder.onDelete(actions.onDelete);
					}
					return builder.build(table);
				},
				ref,
				actions,
			);
		});
	}

	/** @internal */
	abstract build<TTableName extends string>(
		table: AnySnowflakeTable<{ name: TTableName }>,
	): SnowflakeColumn<MakeColumnConfig<T, TTableName>>;
}

export abstract class SnowflakeColumn<
	T extends ColumnBaseConfig<ColumnDataType, string> = ColumnBaseConfig<ColumnDataType, string>,
	TRuntimeConfig extends object = {},
	TTypeConfig extends object = {},
> extends Column<T, TRuntimeConfig, TTypeConfig & { dialect: 'snowflake' }> {
	static override readonly [entityKind]: string = 'SnowflakeColumn';

	constructor(
		override readonly table: SnowflakeTable,
		config: ColumnBuilderRuntimeConfig<T['data'], TRuntimeConfig>,
	) {
		if (!config.uniqueName) {
			config.uniqueName = uniqueKeyName(table, [config.name]);
		}
		super(table, config);
	}
}

export type AnySnowflakeColumn<TPartial extends Partial<ColumnBaseConfig<ColumnDataType, string>> = {}> = SnowflakeColumn<
	Required<Update<ColumnBaseConfig<ColumnDataType, string>, TPartial>>
>;
