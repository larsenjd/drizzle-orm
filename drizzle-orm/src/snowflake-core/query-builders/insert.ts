import type { SQL } from '~/sql/sql.ts';
import type { Subquery } from '~/subquery.ts';
import type { SnowflakeTable } from '~/snowflake-core/table.ts';
import type { SelectedFieldsOrdered } from './select.types.ts';

export interface SnowflakeInsertConfig {
	table: SnowflakeTable;
	values: Record<string, unknown>[] | SQL;
	onConflict?: SQL;
	returning?: SelectedFieldsOrdered;
	withList?: Subquery[];
	select?: boolean;
}
