import { describe, it, expect, beforeAll } from 'vitest';
import init, { Database, JsDataType, ColumnOptions, col } from '../wasm/cynos_database.js';

beforeAll(async () => {
  await init();
});

describe('Primary Key Index Test', () => {
  it('should use IndexGet for primary key lookup', async () => {
    const db = new Database('test_pk');
    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(builder);
    
    // Check table info
    const table = db.table('items');
    console.log('Table name:', table.name);
    console.log('Primary key columns:', table.primaryKeyColumns());
    
    // Insert some data
    await db.insert('items').values([
      { id: 1, name: 'a' },
      { id: 2, name: 'b' },
      { id: 3, name: 'c' },
    ]).exec();
    
    // Check query plan
    const plan = db.select('*').from('items').where(col('id').eq(2)).explain();
    console.log('Query plan for id = 2:');
    console.log(JSON.stringify(plan, null, 2));
    
    // Should use IndexGet, not Filter(Scan)
    expect(plan.optimized).toContain('IndexGet');
  });
});
