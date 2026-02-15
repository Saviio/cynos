import { describe, it, expect, beforeAll } from 'vitest';
import init, { Database, Column, JsDataType, ColumnOptions, col } from '../wasm/cynos_database.js';

beforeAll(async () => {
  await init();
});

describe('Join Live Query', () => {
  it('should create a join live query between two tables', async () => {
    const db = new Database('join_test_1');

    // Create employees table
    const employeesBuilder = db.createTable('employees')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('dept_id', JsDataType.Int64, null);
    db.registerTable(employeesBuilder);

    // Create departments table
    const departmentsBuilder = db.createTable('departments')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(departmentsBuilder);

    // Insert departments
    await db.insert('departments').values([
      { id: 1, name: 'Engineering' },
      { id: 2, name: 'Sales' },
    ]).exec();

    // Insert employees
    await db.insert('employees').values([
      { id: 1, name: 'Alice', dept_id: 1 },
      { id: 2, name: 'Bob', dept_id: 1 },
      { id: 3, name: 'Charlie', dept_id: 2 },
    ]).exec();

    // Create join live query
    const deptIdCol = col('dept_id');
    const joinCondition = deptIdCol.eq('id'); // employees.dept_id = departments.id

    const observable = db.select(['*'])
      .from('employees')
      .innerJoin('departments', joinCondition)
      .observe();

    // Get initial result
    const initialResult = observable.getResult();
    expect(initialResult.length).toBe(3);

    // Track changes
    let changeCount = 0;
    let lastResult: any[] = [];
    const unsubscribe = observable.subscribe((data: any[]) => {
      changeCount++;
      lastResult = data;
    });

    // Insert new employee in Engineering
    await db.insert('employees').values([
      { id: 4, name: 'Diana', dept_id: 1 },
    ]).exec();

    // Should have received update
    expect(changeCount).toBe(1);
    expect(lastResult.length).toBe(4);

    // Insert new department
    await db.insert('departments').values([
      { id: 3, name: 'Marketing' },
    ]).exec();

    // No new employees in Marketing, so no change to join result
    // (unless there are employees with dept_id=3)

    // Insert employee in new department
    await db.insert('employees').values([
      { id: 5, name: 'Eve', dept_id: 3 },
    ]).exec();

    expect(changeCount).toBe(2);
    expect(lastResult.length).toBe(5);

    unsubscribe();
  });

  it('should handle delete propagation in join live query', async () => {
    const db = new Database('join_test_2');

    // Create tables
    const employeesBuilder = db.createTable('employees')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('dept_id', JsDataType.Int64, null);
    db.registerTable(employeesBuilder);

    const departmentsBuilder = db.createTable('departments')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(departmentsBuilder);

    // Insert data
    await db.insert('departments').values([
      { id: 1, name: 'Engineering' },
    ]).exec();

    await db.insert('employees').values([
      { id: 1, name: 'Alice', dept_id: 1 },
      { id: 2, name: 'Bob', dept_id: 1 },
    ]).exec();

    // Create join live query
    const deptIdCol = col('dept_id');
    const observable = db.select(['*'])
      .from('employees')
      .innerJoin('departments', deptIdCol.eq('id'))
      .observe();

    expect(observable.getResult().length).toBe(2);

    let lastResult: any[] = [];
    const unsubscribe = observable.subscribe((data: any[]) => {
      lastResult = data;
    });

    // Delete an employee
    await db.delete('employees').where(col('id').eq(1)).exec();

    expect(lastResult.length).toBe(1);

    unsubscribe();
  });

  it('should handle update propagation in join live query', async () => {
    const db = new Database('join_test_3');

    // Create tables
    const employeesBuilder = db.createTable('employees')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null)
      .column('dept_id', JsDataType.Int64, null);
    db.registerTable(employeesBuilder);

    const departmentsBuilder = db.createTable('departments')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(departmentsBuilder);

    // Insert data
    await db.insert('departments').values([
      { id: 1, name: 'Engineering' },
      { id: 2, name: 'Sales' },
    ]).exec();

    await db.insert('employees').values([
      { id: 1, name: 'Alice', dept_id: 1 },
    ]).exec();

    // Create join live query
    const deptIdCol = col('dept_id');
    const observable = db.select(['*'])
      .from('employees')
      .innerJoin('departments', deptIdCol.eq('id'))
      .observe();

    expect(observable.getResult().length).toBe(1);

    let changeCount = 0;
    let lastResult: any[] = [];
    const unsubscribe = observable.subscribe((data: any[]) => {
      changeCount++;
      lastResult = data;
    });

    // Update employee's department
    await db.update('employees')
      .set('dept_id', 2)
      .where(col('id').eq(1))
      .exec();

    // Should have received update (delete old join + insert new join)
    expect(changeCount).toBeGreaterThanOrEqual(1);
    expect(lastResult.length).toBe(1);

    unsubscribe();
  });
});
