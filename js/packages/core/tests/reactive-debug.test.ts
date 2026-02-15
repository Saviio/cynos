/**
 * Debug test for reactive system
 */

import { describe, it, expect, beforeAll } from 'vitest';
import init, {
  Database,
  JsDataType,
  ColumnOptions,
} from '../wasm/cynos_database.js';

beforeAll(async () => {
  await init();
});

describe('Reactive System Debug', () => {
  it('should notify subscribers when data changes', async () => {
    const db = new Database('reactive_debug');

    // Create table
    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(builder);

    // Insert initial data
    await db.insert('items').values([{ id: 1, name: 'Item 1' }]).exec();

    // Create observable
    const observable = db.select('*').from('items').observe();

    // Track subscription calls
    let callCount = 0;
    let lastData: any[] = [];

    const unsubscribe = observable.subscribe((data: any[]) => {
      callCount++;
      lastData = data;
    });

    // Insert new data
    await db.insert('items').values([{ id: 2, name: 'Item 2' }]).exec();

    // Verify
    expect(callCount).toBe(1);
    expect(lastData.length).toBe(2);

    unsubscribe();
  });

  it('should work with changes() stream', async () => {
    const db = new Database('changes_debug');

    // Create table
    const builder = db.createTable('items')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(builder);

    // Insert initial data
    await db.insert('items').values([{ id: 1, name: 'Item 1' }]).exec();

    // Create changes stream
    const stream = db.select('*').from('items').changes();

    // Track subscription calls
    let callCount = 0;
    let lastData: any[] = [];

    const unsubscribe = stream.subscribe((data: any[]) => {
      callCount++;
      lastData = data;
    });

    // Insert new data
    await db.insert('items').values([{ id: 2, name: 'Item 2' }]).exec();

    // changes() should emit initial data immediately, then updates
    expect(callCount).toBeGreaterThanOrEqual(1);

    unsubscribe();
  });

  it('should handle bulk insert then single insert', async () => {
    const db = new Database('bulk_then_single');

    // Create table
    const builder = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(builder);

    // Generate 100 users
    const users = Array.from({ length: 100 }, (_, i) => ({
      id: i + 1,
      name: `User ${i + 1}`,
    }));

    // Insert 100 users in bulk
    await db.insert('users').values(users).exec();

    // Query to verify
    const allUsers = await db.select('*').from('users').exec();
    expect(allUsers.length).toBe(100);

    // Insert one more user
    await db.insert('users').values([{ id: 999, name: 'New User' }]).exec();

    const allUsers2 = await db.select('*').from('users').exec();
    expect(allUsers2.length).toBe(101);
  });
});
