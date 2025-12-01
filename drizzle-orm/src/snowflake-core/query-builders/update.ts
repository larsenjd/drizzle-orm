import type { SQL } from '~/sql/sql.ts';
import type { Subquery } from '~/subquery.ts';
import type { SnowflakeTable } from '~/snowflake-core/table.ts';
import type { UpdateSet } from '~/utils.ts';
import type { SelectedFieldsOrdered } from './select.types.ts';

export interface SnowflakeUpdateConfig {
	table: SnowflakeTable;
	set: UpdateSet;
	where?: SQL;
	returning?: SelectedFieldsOrdered;
	withList?: Subquery[];
}
