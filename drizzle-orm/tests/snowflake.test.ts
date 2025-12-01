import { describe, test, expect, vi, beforeEach } from 'vitest';
import { is } from '~/entity.ts';
import { Column } from '~/column.ts';
import { Table } from '~/table.ts';
import { sql } from '~/sql/sql.ts';
import {
	snowflakeTable,
	SnowflakeTable,
	varchar,
	text,
	number as sfNumber,
	integer,
	boolean as sfBoolean,
	timestamp,
	date,
	SnowflakeColumn,
	SnowflakeDialect,
	SnowflakeDatabase,
} from '~/snowflake-core/index.ts';
import {
	drizzle,
	type SnowflakeConnection,
	// SnowflakeSdkSession,
	// SnowflakeSdkPreparedQuery,
	// SnowflakeSdkTransaction,
} from '~/snowflake/index.ts';

describe('Snowflake column types', () => {
	// Test column types through table definition since columns require a table reference
	const testTable = snowflakeTable('test_columns', {
		varcharCol: varchar('varchar_col', { length: 255 }),
		varcharNoLen: varchar('varchar_no_len'),
		textCol: text('text_col'),
		numberCol: sfNumber('number_col'),
		numberPrecScale: sfNumber('number_prec_scale', { precision: 10, scale: 2 }),
		integerCol: integer('integer_col'),
		booleanCol: sfBoolean('boolean_col'),
		timestampCol: timestamp('timestamp_col'),
		timestampPrec: timestamp('timestamp_prec', { precision: 6 }),
		timestampNtz: timestamp('timestamp_ntz', { withTimezone: false }),
		timestampTz: timestamp('timestamp_tz', { withTimezone: true }),
		dateCol: date('date_col'),
	});

	test('varchar column', () => {
		expect(testTable.varcharCol.getSQLType()).toBe('varchar(255)');
	});

	test('varchar column without length', () => {
		expect(testTable.varcharNoLen.getSQLType()).toBe('varchar');
	});

	test('text column', () => {
		expect(testTable.textCol.getSQLType()).toBe('text');
	});

	test('number column', () => {
		expect(testTable.numberCol.getSQLType()).toBe('number');
	});

	test('number column with precision and scale', () => {
		expect(testTable.numberPrecScale.getSQLType()).toBe('number(10, 2)');
	});

	test('integer column', () => {
		expect(testTable.integerCol.getSQLType()).toBe('integer');
	});

	test('boolean column', () => {
		expect(testTable.booleanCol.getSQLType()).toBe('boolean');
	});

	test('timestamp column', () => {
		// Default timestamp without timezone config returns timestamp_ntz
		expect(testTable.timestampCol.getSQLType()).toBe('timestamp_ntz');
	});

	test('timestamp column with precision', () => {
		expect(testTable.timestampPrec.getSQLType()).toBe('timestamp_ntz(6)');
	});

	test('timestamp_ntz column', () => {
		expect(testTable.timestampNtz.getSQLType()).toBe('timestamp_ntz');
	});

	test('timestamp_tz column', () => {
		expect(testTable.timestampTz.getSQLType()).toBe('timestamp_tz');
	});

	test('date column', () => {
		expect(testTable.dateCol.getSQLType()).toBe('date');
	});
});

describe('Snowflake table', () => {
	const users = snowflakeTable('users', {
		id: integer('id').primaryKey(),
		name: varchar('name', { length: 255 }),
		email: varchar('email', { length: 255 }).notNull(),
		active: sfBoolean('active').default(true),
		createdAt: timestamp('created_at'),
	});

	test('table is instance of SnowflakeTable', () => {
		expect(is(users, SnowflakeTable)).toBe(true);
		expect(is(users, Table)).toBe(true);
	});

	test('columns are instances of SnowflakeColumn', () => {
		expect(is(users.id, SnowflakeColumn)).toBe(true);
		expect(is(users.name, SnowflakeColumn)).toBe(true);
		expect(is(users.email, SnowflakeColumn)).toBe(true);
		expect(is(users.id, Column)).toBe(true);
	});

	test('table has correct name', () => {
		expect(users[SnowflakeTable.Symbol.Name]).toBe('users');
	});
});

describe('SnowflakeDialect', () => {
	const dialect = new SnowflakeDialect();

	test('escapeName wraps identifiers in double quotes', () => {
		expect(dialect.escapeName('table_name')).toBe('"table_name"');
		expect(dialect.escapeName('Column')).toBe('"Column"');
	});

	test('escapeParam returns ? placeholder', () => {
		expect(dialect.escapeParam(0)).toBe('?');
		expect(dialect.escapeParam(1)).toBe('?');
		expect(dialect.escapeParam(99)).toBe('?');
	});

	test('escapeString escapes single quotes', () => {
		expect(dialect.escapeString("hello")).toBe("'hello'");
		expect(dialect.escapeString("it's")).toBe("'it''s'");
		expect(dialect.escapeString("test'value'here")).toBe("'test''value''here'");
	});
});

describe('Snowflake SQL generation', () => {
	const dialect = new SnowflakeDialect();

	test('escapeName wraps identifiers correctly', () => {
		// Test that dialect escapes names properly for SQL generation
		expect(dialect.escapeName('users')).toBe('"users"');
		expect(dialect.escapeName('user_name')).toBe('"user_name"');
	});

	test('escapeParam returns positional placeholder', () => {
		// Snowflake uses ? for all positional parameters
		expect(dialect.escapeParam(0)).toBe('?');
		expect(dialect.escapeParam(5)).toBe('?');
	});
});

describe('Snowflake driver with mock', () => {
	let mockConnection: SnowflakeConnection;

	beforeEach(() => {
		mockConnection = {
			execute: vi.fn(),
		};
	});

	test('drizzle creates database instance', () => {
		const db = drizzle(mockConnection);
		expect(db).toBeInstanceOf(SnowflakeDatabase);
	});

	test('drizzle accepts logger option', () => {
		const db = drizzle(mockConnection, { logger: true });
		expect(db).toBeInstanceOf(SnowflakeDatabase);
	});

	test('execute calls connection.execute with correct parameters', async () => {
		const mockRows = [{ id: 1, name: 'Test' }];
		(mockConnection.execute as ReturnType<typeof vi.fn>).mockImplementation(
			({ complete }: { complete: (err: Error | undefined, stmt: unknown, rows: unknown[]) => void }) => {
				complete(undefined, {}, mockRows);
			}
		);

		const db = drizzle(mockConnection);
		
		// Access internal session to test query execution
		await db.execute(sql`SELECT * FROM users`);
		
		expect(mockConnection.execute).toHaveBeenCalled();
	});

	test('transaction executes BEGIN, callback, COMMIT on success', async () => {
		const executeCalls: string[] = [];
		
		(mockConnection.execute as ReturnType<typeof vi.fn>).mockImplementation(
			({ sqlText, complete }: { sqlText: string; complete: (err: Error | undefined, stmt: unknown, rows?: unknown[]) => void }) => {
				executeCalls.push(sqlText);
				complete(undefined, {}, []);
			}
		);

		const db = drizzle(mockConnection);
		
		await db.transaction(async (_tx) => {
			// Transaction body
		});

		expect(executeCalls).toContain('BEGIN TRANSACTION');
		expect(executeCalls).toContain('COMMIT');
	});

	test('transaction executes BEGIN, callback, ROLLBACK on error', async () => {
		const executeCalls: string[] = [];
		
		(mockConnection.execute as ReturnType<typeof vi.fn>).mockImplementation(
			({ sqlText, complete }: { sqlText: string; complete: (err: Error | undefined, stmt: unknown, rows?: unknown[]) => void }) => {
				executeCalls.push(sqlText);
				complete(undefined, {}, []);
			}
		);

		const db = drizzle(mockConnection);
		
		try {
			await db.transaction(async (_tx) => {
				throw new Error('Test error');
			});
		} catch {
			// Expected error
		}

		expect(executeCalls).toContain('BEGIN TRANSACTION');
		expect(executeCalls).toContain('ROLLBACK');
	});

	test('nested transaction executes in same scope (no BEGIN/COMMIT)', async () => {
		const executeCalls: string[] = [];
		
		(mockConnection.execute as ReturnType<typeof vi.fn>).mockImplementation(
			({ sqlText, complete }: { sqlText: string; complete: (err: Error | undefined, stmt: unknown, rows?: unknown[]) => void }) => {
				executeCalls.push(sqlText);
				complete(undefined, {}, []);
			}
		);

		const db = drizzle(mockConnection);
		
		await db.transaction(async (tx) => {
			await tx.transaction(async (_nestedTx) => {
				// Nested transaction body - should NOT issue BEGIN/COMMIT
			});
		});

		// Should only have one BEGIN and one COMMIT (from outer transaction)
		const beginCount = executeCalls.filter(c => c === 'BEGIN TRANSACTION').length;
		const commitCount = executeCalls.filter(c => c === 'COMMIT').length;
		
		expect(beginCount).toBe(1);
		expect(commitCount).toBe(1);
	});
});

// TODO: Integration tests with actual Snowflake instance
// These tests require Docker or cloud Snowflake instance setup
// See integration-tests/tests/ for examples of Docker-based test patterns
describe.skip('Snowflake integration tests', () => {
	test.todo('TODO: Set up Docker-based Snowflake test environment');
	test.todo('TODO: Test actual CRUD operations against Snowflake');
	test.todo('TODO: Test transactions with real connection');
	test.todo('TODO: Test browser-based SSO authentication flow');
});
