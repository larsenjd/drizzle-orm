import type {
	SelectedFields as SelectedFieldsBase,
	SelectedFieldsFlat as SelectedFieldsFlatBase,
	SelectedFieldsOrdered as SelectedFieldsOrderedBase,
} from '~/operations.ts';
import type { SnowflakeColumn } from '~/snowflake-core/columns/index.ts';
import type { SnowflakeTable, SnowflakeTableWithColumns } from '~/snowflake-core/table.ts';
import type { TypedQueryBuilder } from '~/query-builders/query-builder.ts';
import type {
	BuildSubquerySelection,
	JoinNullability,
	JoinType,
	MapColumnsToTableAlias,
	SelectMode,
	SelectResult,
	SetOperator,
} from '~/query-builders/select.types.ts';
import type { ColumnsSelection, Placeholder, SQL, View } from '~/sql/sql.ts';
import type { Subquery } from '~/subquery.ts';
import type { Table, UpdateTableConfig } from '~/table.ts';


export interface SnowflakeSelectJoinConfig {
	on: SQL | undefined;
	table: SnowflakeTable | Subquery | SQL;
	alias: string | undefined;
	joinType: JoinType;
}

export type BuildAliasTable<TTable extends SnowflakeTable | View, TAlias extends string> = TTable extends Table
	? SnowflakeTableWithColumns<
		UpdateTableConfig<TTable['_']['config'], {
			name: TAlias;
			columns: MapColumnsToTableAlias<TTable['_']['columns'], TAlias, 'snowflake'>;
		}>
	>
	: never;

export interface SnowflakeSelectConfig {
	withList?: Subquery[];
	fields: Record<string, unknown>;
	fieldsFlat?: SelectedFieldsOrdered;
	where?: SQL;
	having?: SQL;
	table: SnowflakeTable | Subquery | SQL;
	limit?: number | Placeholder;
	offset?: number | Placeholder;
	joins?: SnowflakeSelectJoinConfig[];
	orderBy?: (SnowflakeColumn | SQL | SQL.Aliased)[];
	groupBy?: (SnowflakeColumn | SQL | SQL.Aliased)[];
	distinct?: boolean;
	setOperators: {
		rightSelect: TypedQueryBuilder<any, any>;
		type: SetOperator;
		isAll: boolean;
		orderBy?: (SnowflakeColumn | SQL | SQL.Aliased)[];
		limit?: number | Placeholder;
		offset?: number | Placeholder;
	}[];
}

export type SelectedFieldsFlat = SelectedFieldsFlatBase<SnowflakeColumn>;

export type SelectedFields = SelectedFieldsBase<SnowflakeColumn, SnowflakeTable>;

export type SelectedFieldsOrdered = SelectedFieldsOrderedBase<SnowflakeColumn>;

export interface SnowflakeSelectHKTBase {
	tableName: string | undefined;
	selection: unknown;
	selectMode: SelectMode;
	nullabilityMap: unknown;
	dynamic: boolean;
	excludedMethods: string;
	result: unknown;
	selectedFields: unknown;
	_type: unknown;
}

export type SnowflakeSelectKind<
	T extends SnowflakeSelectHKTBase,
	TTableName extends string | undefined,
	TSelection extends ColumnsSelection,
	TSelectMode extends SelectMode,
	TNullabilityMap extends Record<string, JoinNullability>,
	TDynamic extends boolean,
	TExcludedMethods extends string,
	TResult = SelectResult<TSelection, TSelectMode, TNullabilityMap>[],
	TSelectedFields = BuildSubquerySelection<TSelection, TNullabilityMap>,
> = (T & {
	tableName: TTableName;
	selection: TSelection;
	selectMode: TSelectMode;
	nullabilityMap: TNullabilityMap;
	dynamic: TDynamic;
	excludedMethods: TExcludedMethods;
	result: TResult;
	selectedFields: TSelectedFields;
})['_type'];

export type SnowflakeSetOperatorExcludedMethods =
	| 'leftJoin'
	| 'rightJoin'
	| 'innerJoin'
	| 'fullJoin'
	| 'where'
	| 'having'
	| 'groupBy';
