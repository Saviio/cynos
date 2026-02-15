/**
 * Cynos Database - End-to-End Tests
 *
 * These tests cover the full lifecycle of database operations including:
 * - Database creation and table management
 * - CRUD operations (Create, Read, Update, Delete)
 * - Query builders with filtering, sorting, pagination
 * - Transactions with commit and rollback
 * - Reactive queries and change streams
 * - JSONB operations
 */

import { describe, it, expect, beforeAll, beforeEach } from 'vitest';
import init, {
  Database,
  JsTableBuilder,
  JsDataType,
  JsSortOrder,
  ColumnOptions,
  col,
  
} from '../wasm/cynos_database.js';

// Initialize WASM before all tests
beforeAll(async () => {
  await init();
  
});

describe('Database Lifecycle', () => {
  it('should create a new database', () => {
    const db = new Database('test_db');
    expect(db.name).toBe('test_db');
  });

  it('should create and register tables', () => {
    const db = new Database('test_db');

    const builder = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('age', JsDataType.Int32, null);

    db.registerTable(builder);

    expect(db.tableCount()).toBe(1);
    expect(db.tableNames()).toContain('users');
  });

  it('should drop tables', () => {
    const db = new Database('test_db');

    const builder = db.createTable('temp')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true));
    db.registerTable(builder);

    expect(db.tableCount()).toBe(1);

    db.dropTable('temp');
    expect(db.tableCount()).toBe(0);
  });

  it('should get table reference', () => {
    const db = new Database('test_db');

    const builder = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(builder);

    const table = db.table('users');
    expect(table).toBeDefined();
    expect(table!.name).toBe('users');
    expect(table!.columnCount()).toBe(2);
  });
});

describe('CRUD Operations', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database('crud_test');
    const builder = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('age', JsDataType.Int32, null)
      .column('email', JsDataType.String, new ColumnOptions().setNullable(true));
    db.registerTable(builder);
  });

  it('should insert single row', async () => {
    const count = await db.insert('users')
      .values([{ id: 1, name: 'Alice', age: 25, email: 'alice@test.com' }])
      .exec();

    expect(count).toBe(1);
    expect(db.totalRowCount()).toBe(1);
  });

  it('should insert multiple rows', async () => {
    const count = await db.insert('users')
      .values([
        { id: 1, name: 'Alice', age: 25, email: 'alice@test.com' },
        { id: 2, name: 'Bob', age: 30, email: 'bob@test.com' },
        { id: 3, name: 'Charlie', age: 35, email: null },
      ])
      .exec();

    expect(count).toBe(3);
    expect(db.totalRowCount()).toBe(3);
  });

  it('should select all rows', async () => {
    await db.insert('users')
      .values([
        { id: 1, name: 'Alice', age: 25, email: 'alice@test.com' },
        { id: 2, name: 'Bob', age: 30, email: 'bob@test.com' },
      ])
      .exec();

    const results = await db.select('*').from('users').exec();
    expect(results).toHaveLength(2);
  });

  it('should select with WHERE clause', async () => {
    await db.insert('users')
      .values([
        { id: 1, name: 'Alice', age: 25, email: 'alice@test.com' },
        { id: 2, name: 'Bob', age: 30, email: 'bob@test.com' },
        { id: 3, name: 'Charlie', age: 35, email: null },
      ])
      .exec();

    const results = await db.select('*')
      .from('users')
      .where(col('age').gt(28))
      .exec();

    expect(results).toHaveLength(2);
  });

  it('should update rows', async () => {
    await db.insert('users')
      .values([{ id: 1, name: 'Alice', age: 25, email: 'alice@test.com' }])
      .exec();

    const updateCount = await db.update('users')
      .set('age', 26)
      .where(col('id').eq(1))
      .exec();

    expect(updateCount).toBe(1);

    const results = await db.select('*').from('users').exec();
    expect(results[0].age).toBe(26);
  });

  it('should delete rows', async () => {
    await db.insert('users')
      .values([
        { id: 1, name: 'Alice', age: 25, email: 'alice@test.com' },
        { id: 2, name: 'Bob', age: 30, email: 'bob@test.com' },
      ])
      .exec();

    const deleteCount = await db.delete('users')
      .where(col('id').eq(1))
      .exec();

    expect(deleteCount).toBe(1);
    expect(db.totalRowCount()).toBe(1);
  });
});

describe('Query Builder Features', () => {
  let db: Database;

  beforeEach(async () => {
    db = new Database('query_test');
    const builder = db.createTable('products')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('price', JsDataType.Float64, null)
      .column('category', JsDataType.String, null);
    db.registerTable(builder);

    await db.insert('products')
      .values([
        { id: 1, name: 'Apple', price: 1.50, category: 'fruit' },
        { id: 2, name: 'Banana', price: 0.75, category: 'fruit' },
        { id: 3, name: 'Carrot', price: 0.50, category: 'vegetable' },
        { id: 4, name: 'Milk', price: 2.00, category: 'dairy' },
        { id: 5, name: 'Bread', price: 2.50, category: 'bakery' },
      ])
      .exec();
  });

  it('should order by ascending', async () => {
    const results = await db.select('*')
      .from('products')
      .orderBy('price', JsSortOrder.Asc)
      .exec();

    expect(results[0].name).toBe('Carrot');
    expect(results[4].name).toBe('Bread');
  });

  it('should order by descending', async () => {
    const results = await db.select('*')
      .from('products')
      .orderBy('price', JsSortOrder.Desc)
      .exec();

    expect(results[0].name).toBe('Bread');
    expect(results[4].name).toBe('Carrot');
  });

  it('should limit results', async () => {
    const results = await db.select('*')
      .from('products')
      .limit(3)
      .exec();

    expect(results).toHaveLength(3);
  });

  it('should offset results', async () => {
    const results = await db.select('*')
      .from('products')
      .orderBy('id', JsSortOrder.Asc)
      .offset(2)
      .exec();

    expect(results).toHaveLength(3);
    expect(results[0].id).toBe(3);
  });

  it('should combine limit and offset', async () => {
    const results = await db.select('*')
      .from('products')
      .orderBy('id', JsSortOrder.Asc)
      .offset(1)
      .limit(2)
      .exec();

    expect(results).toHaveLength(2);
    expect(results[0].id).toBe(2);
    expect(results[1].id).toBe(3);
  });

  it('should filter with multiple conditions (AND)', async () => {
    const results = await db.select('*')
      .from('products')
      .where(col('category').eq('fruit').and(col('price').lt(1.0)))
      .exec();

    expect(results).toHaveLength(1);
    expect(results[0].name).toBe('Banana');
  });

  it('should filter with OR conditions', async () => {
    const results = await db.select('*')
      .from('products')
      .where(col('category').eq('dairy').or(col('category').eq('bakery')))
      .exec();

    expect(results).toHaveLength(2);
  });

  it('should filter with BETWEEN', async () => {
    const results = await db.select('*')
      .from('products')
      .where(col('price').between(1.0, 2.0))
      .exec();

    expect(results).toHaveLength(2); // Apple (1.50) and Milk (2.00)
  });

  it('should filter with LIKE', async () => {
    const results = await db.select('*')
      .from('products')
      .where(col('name').like('B%'))
      .exec();

    expect(results).toHaveLength(2); // Banana and Bread
  });
});

describe('Transactions', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database('tx_test');
    const builder = db.createTable('accounts')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('balance', JsDataType.Float64, null);
    db.registerTable(builder);
  });

  it('should commit transaction', async () => {
    const tx = db.transaction();

    tx.insert('accounts', [
      { id: 1, name: 'Alice', balance: 100.0 },
      { id: 2, name: 'Bob', balance: 200.0 },
    ]);

    expect(tx.active).toBe(true);
    expect(tx.state).toBe('active');

    tx.commit();

    expect(tx.active).toBe(false);
    expect(db.totalRowCount()).toBe(2);
  });

  it('should rollback transaction', async () => {
    // First insert some data
    await db.insert('accounts')
      .values([{ id: 1, name: 'Alice', balance: 100.0 }])
      .exec();

    expect(db.totalRowCount()).toBe(1);

    // Start transaction and insert more
    const tx = db.transaction();
    tx.insert('accounts', [{ id: 2, name: 'Bob', balance: 200.0 }]);

    // Rollback
    tx.rollback();

    // Should only have the original row
    expect(db.totalRowCount()).toBe(1);
  });

  it('should handle multiple operations in transaction', async () => {
    await db.insert('accounts')
      .values([
        { id: 1, name: 'Alice', balance: 100.0 },
        { id: 2, name: 'Bob', balance: 200.0 },
      ])
      .exec();

    const tx = db.transaction();

    // Transfer money: Alice -> Bob
    tx.update('accounts', { balance: 50.0 }, col('id').eq(1));
    tx.update('accounts', { balance: 250.0 }, col('id').eq(2));

    tx.commit();

    const results = await db.select('*')
      .from('accounts')
      .orderBy('id', JsSortOrder.Asc)
      .exec();

    expect(results[0].balance).toBe(50.0);
    expect(results[1].balance).toBe(250.0);
  });
});

describe('Reactive Queries', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database('reactive_test');
    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('quantity', JsDataType.Int32, null);
    db.registerTable(builder);
  });

  it('should create observable query', async () => {
    await db.insert('items')
      .values([
        { id: 1, name: 'Item A', quantity: 10 },
        { id: 2, name: 'Item B', quantity: 20 },
      ])
      .exec();

    const observable = db.select('*').from('items').observe();

    expect(observable.length).toBe(2);
    expect(observable.isEmpty()).toBe(false);
  });

  it('should get current result from observable', async () => {
    await db.insert('items')
      .values([
        { id: 1, name: 'Item A', quantity: 10 },
        { id: 2, name: 'Item B', quantity: 20 },
      ])
      .exec();

    const observable = db.select('*').from('items').observe();
    const result = observable.getResult();

    expect(result).toHaveLength(2);
  });

  it('should subscribe to changes', async () => {
    // First insert some data
    await db.insert('items')
      .values([{ id: 1, name: 'Item A', quantity: 10 }])
      .exec();

    // Create observable AFTER initial insert
    const observable = db.select('*').from('items').observe();

    // Verify initial state
    expect(observable.length).toBe(1);

    let changeCount = 0;
    let lastData: any[] = [];
    const unsubscribe = observable.subscribe((data: any[]) => {
      changeCount++;
      lastData = data;
    });

    // Insert new item - this should trigger the subscription
    await db.insert('items')
      .values([{ id: 2, name: 'Item B', quantity: 20 }])
      .exec();

    // The callback should have been called synchronously
    expect(changeCount).toBe(1);
    expect(lastData.length).toBe(2);

    unsubscribe();
  });

  it('should create changes stream', async () => {
    await db.insert('items')
      .values([{ id: 1, name: 'Item A', quantity: 10 }])
      .exec();

    const stream = db.select('*').from('items').changes();

    let initialReceived = false;
    const unsubscribe = stream.subscribe((data: any[]) => {
      // Now returns full data array instead of {added, removed, modified}
      if (Array.isArray(data) && data.length > 0) {
        initialReceived = true;
      }
    });

    // Give time for initial emission
    await new Promise(resolve => setTimeout(resolve, 10));

    expect(initialReceived).toBe(true);

    unsubscribe();
  });
});

describe('Expression Builders', () => {
  let db: Database;

  beforeEach(async () => {
    db = new Database('expr_test');
    const builder = db.createTable('data')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('value', JsDataType.Int32, null)
      .column('text', JsDataType.String, new ColumnOptions().setNullable(true));
    db.registerTable(builder);

    await db.insert('data')
      .values([
        { id: 1, value: 10, text: 'hello' },
        { id: 2, value: 20, text: 'world' },
        { id: 3, value: 30, text: null },
        { id: 4, value: 40, text: 'hello world' },
      ])
      .exec();
  });

  it('should filter with eq', async () => {
    const results = await db.select('*')
      .from('data')
      .where(col('value').eq(20))
      .exec();

    expect(results).toHaveLength(1);
    expect(results[0].id).toBe(2);
  });

  it('should filter with ne', async () => {
    const results = await db.select('*')
      .from('data')
      .where(col('value').ne(20))
      .exec();

    expect(results).toHaveLength(3);
  });

  it('should filter with gt', async () => {
    const results = await db.select('*')
      .from('data')
      .where(col('value').gt(20))
      .exec();

    expect(results).toHaveLength(2);
  });

  it('should filter with gte', async () => {
    const results = await db.select('*')
      .from('data')
      .where(col('value').gte(20))
      .exec();

    expect(results).toHaveLength(3);
  });

  it('should filter with lt', async () => {
    const results = await db.select('*')
      .from('data')
      .where(col('value').lt(30))
      .exec();

    expect(results).toHaveLength(2);
  });

  it('should filter with lte', async () => {
    const results = await db.select('*')
      .from('data')
      .where(col('value').lte(30))
      .exec();

    expect(results).toHaveLength(3);
  });

  it('should filter with isNull', async () => {
    const results = await db.select('*')
      .from('data')
      .where(col('text').isNull())
      .exec();

    expect(results).toHaveLength(1);
    expect(results[0].id).toBe(3);
  });

  it('should filter with isNotNull', async () => {
    const results = await db.select('*')
      .from('data')
      .where(col('text').isNotNull())
      .exec();

    expect(results).toHaveLength(3);
  });

  it('should filter with in', async () => {
    const results = await db.select('*')
      .from('data')
      .where(col('value').in([10, 30]))
      .exec();

    expect(results).toHaveLength(2);
  });

  it('should combine with NOT', async () => {
    const results = await db.select('*')
      .from('data')
      .where(col('value').gt(20).not())
      .exec();

    expect(results).toHaveLength(2); // value <= 20
  });
});

describe('Table Schema', () => {
  it('should create table with all data types', () => {
    const db = new Database('schema_test');

    const builder = db.createTable('all_types')
      .column('bool_col', JsDataType.Boolean, null)
      .column('int32_col', JsDataType.Int32, null)
      .column('int64_col', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('float_col', JsDataType.Float64, null)
      .column('string_col', JsDataType.String, null)
      .column('bytes_col', JsDataType.Bytes, null)
      .column('datetime_col', JsDataType.DateTime, null)
      .column('jsonb_col', JsDataType.Jsonb, null);

    db.registerTable(builder);

    const table = db.table('all_types');
    expect(table!.columnCount()).toBe(8);
  });

  it('should create table with indices', () => {
    const db = new Database('index_test');

    const builder = db.createTable('indexed')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('email', JsDataType.String, null)
      .column('category', JsDataType.String, null)
      .uniqueIndex('idx_email', 'email')
      .index('idx_category', 'category');

    db.registerTable(builder);

    const table = db.table('indexed');
    expect(table).toBeDefined();
  });

  it('should get column information', () => {
    const db = new Database('col_info_test');

    const builder = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('bio', JsDataType.String, new ColumnOptions().setNullable(true));

    db.registerTable(builder);

    const table = db.table('users')!;

    expect(table.columnNames()).toContain('id');
    expect(table.columnNames()).toContain('name');
    expect(table.columnNames()).toContain('bio');

    expect(table.getColumnType('id')).toBe(JsDataType.Int64);
    expect(table.getColumnType('name')).toBe(JsDataType.String);

    expect(table.isColumnNullable('bio')).toBe(true);
    expect(table.isColumnNullable('name')).toBe(false);

    expect(table.primaryKeyColumns()).toContain('id');
  });
});

describe('Error Handling', () => {
  it('should throw on duplicate primary key', async () => {
    const db = new Database('error_test');
    const builder = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(builder);

    await db.insert('users')
      .values([{ id: 1, name: 'Alice' }])
      .exec();

    await expect(
      db.insert('users')
        .values([{ id: 1, name: 'Bob' }])
        .exec()
    ).rejects.toThrow();
  });

  it('should throw on non-existent table', async () => {
    const db = new Database('error_test');

    await expect(
      db.select('*').from('nonexistent').exec()
    ).rejects.toThrow();
  });
});

describe('Projection (Column Selection)', () => {
  let db: Database;

  beforeEach(async () => {
    db = new Database('projection_test');
    const builder = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('age', JsDataType.Int32, null)
      .column('email', JsDataType.String, new ColumnOptions().setNullable(true));
    db.registerTable(builder);

    await db.insert('users')
      .values([
        { id: 1, name: 'Alice', age: 25, email: 'alice@test.com' },
        { id: 2, name: 'Bob', age: 30, email: 'bob@test.com' },
        { id: 3, name: 'Charlie', age: 35, email: null },
      ])
      .exec();
  });

  it('should select specific columns with array syntax', async () => {
    const results = await db.select(['name', 'age']).from('users').exec();

    expect(results).toHaveLength(3);
    // Should only have name and age columns
    expect(results[0]).toHaveProperty('name');
    expect(results[0]).toHaveProperty('age');
    expect(results[0]).not.toHaveProperty('id');
    expect(results[0]).not.toHaveProperty('email');
  });

  it('should select single column with string syntax', async () => {
    const results = await db.select('name').from('users').exec();

    expect(results).toHaveLength(3);
    expect(results[0]).toHaveProperty('name');
    expect(results[0]).not.toHaveProperty('id');
    expect(results[0]).not.toHaveProperty('age');
    expect(results[0]).not.toHaveProperty('email');
  });

  it('should select all columns with * syntax', async () => {
    const results = await db.select('*').from('users').exec();

    expect(results).toHaveLength(3);
    expect(results[0]).toHaveProperty('id');
    expect(results[0]).toHaveProperty('name');
    expect(results[0]).toHaveProperty('age');
    expect(results[0]).toHaveProperty('email');
  });

  it('should combine projection with WHERE clause', async () => {
    const results = await db.select(['name', 'age'])
      .from('users')
      .where(col('age').gt(28))
      .exec();

    expect(results).toHaveLength(2);
    expect(results[0]).toHaveProperty('name');
    expect(results[0]).toHaveProperty('age');
    expect(results[0]).not.toHaveProperty('id');
  });

  it('should combine projection with ORDER BY', async () => {
    const results = await db.select(['name', 'age'])
      .from('users')
      .orderBy('age', JsSortOrder.Desc)
      .exec();

    expect(results).toHaveLength(3);
    expect(results[0].name).toBe('Charlie');
    expect(results[0].age).toBe(35);
    expect(results[0]).not.toHaveProperty('id');
  });

  it('should combine projection with LIMIT', async () => {
    const results = await db.select(['name'])
      .from('users')
      .limit(2)
      .exec();

    expect(results).toHaveLength(2);
    expect(results[0]).toHaveProperty('name');
    expect(results[0]).not.toHaveProperty('id');
    expect(results[0]).not.toHaveProperty('age');
  });

  it('should work with observable queries', async () => {
    const observable = db.select(['name', 'age']).from('users').observe();

    const result = observable.getResult();
    expect(result).toHaveLength(3);
    expect(result[0]).toHaveProperty('name');
    expect(result[0]).toHaveProperty('age');
    expect(result[0]).not.toHaveProperty('id');
    expect(result[0]).not.toHaveProperty('email');
  });

  it('should work with changes stream', async () => {
    const stream = db.select(['name']).from('users').changes();

    let receivedData: any[] = [];
    const unsubscribe = stream.subscribe((data: any[]) => {
      receivedData = data;
    });

    // Give time for initial emission
    await new Promise(resolve => setTimeout(resolve, 10));

    expect(receivedData).toHaveLength(3);
    expect(receivedData[0]).toHaveProperty('name');
    expect(receivedData[0]).not.toHaveProperty('id');

    unsubscribe();
  });
});
