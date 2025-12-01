import type { SQL } from '~/sql/sql.ts';
import type { Subquery } from '~/subquery.ts';
import type { SnowflakeTable } from '~/snowflake-core/table.ts';
import type { SelectedFieldsOrdered } from './select.types.ts';

export interface SnowflakeDeleteConfig {
	table: SnowflakeTable;
	where?: SQL;
	returning?: SelectedFieldsOrdered;
	withList?: Subquery[];
}
