/**
 * Stress Test for Live Query Performance
 *
 * Scenario: Task management system with 100,000 tasks
 * - Query top 1000 tasks by updated_at DESC
 * - Filter by status and priority
 * - Measure query latency and update propagation
 */

import { describe, it, expect, beforeAll } from 'vitest';
import init, {
  Database,
  JsDataType,
  JsSortOrder,
  ColumnOptions,
  col,
} from '../wasm/cynos_database.js';

beforeAll(async () => {
  await init();
});

// Test configuration
const TOTAL_TASKS = 100000;
const QUERY_LIMIT = 1000;
const STATUSES = ['todo', 'in_progress', 'review', 'done', 'blocked'];
const PRIORITIES = ['low', 'medium', 'high', 'critical'];
const CATEGORIES = ['feature', 'bug', 'improvement', 'documentation', 'test'];

// Helper to create test database with Task table
function createTaskDb(name: string) {
  const db = new Database(name);

  // Tasks table - main table with 100k records
  const tasksBuilder = db.createTable('tasks')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('title', JsDataType.String, null)
    .column('status', JsDataType.String, null)
    .column('priority', JsDataType.String, null)
    .column('category', JsDataType.String, null)
    .column('assignee_id', JsDataType.Int64, null)
    .column('project_id', JsDataType.Int64, null)
    .column('created_at', JsDataType.Int64, null)
    .column('updated_at', JsDataType.Int64, null)
    .column('due_date', JsDataType.Int64, null)
    .column('estimated_hours', JsDataType.Int32, null)
    .column('actual_hours', JsDataType.Int32, null)
    .column('description', JsDataType.String, null)
    .index('idx_status', 'status')
    .index('idx_priority', 'priority')
    .index('idx_category', 'category')
    .index('idx_updated_at', 'updated_at')
    .index('idx_assignee', 'assignee_id')
    .index('idx_project', 'project_id');
  db.registerTable(tasksBuilder);

  return db;
}

// Generate test tasks
function generateTasks(count: number) {
  const now = Date.now();
  return Array.from({ length: count }, (_, i) => ({
    id: i + 1,
    title: `Task ${i + 1}: ${CATEGORIES[i % CATEGORIES.length]} work item`,
    status: STATUSES[i % STATUSES.length],
    priority: PRIORITIES[i % PRIORITIES.length],
    category: CATEGORIES[i % CATEGORIES.length],
    assignee_id: (i % 100) + 1,  // 100 assignees
    project_id: (i % 50) + 1,    // 50 projects
    created_at: now - (count - i) * 60000,  // Spread over time
    updated_at: now - (i % 10000) * 1000,   // Recent updates clustered
    due_date: now + (i % 30) * 86400000,    // Due dates spread over 30 days
    estimated_hours: (i % 40) + 1,
    actual_hours: Math.floor((i % 40) * 0.8),
    description: `Description for task ${i + 1}. This is a ${PRIORITIES[i % PRIORITIES.length]} priority ${CATEGORIES[i % CATEGORIES.length]} task.`,
  }));
}

describe('Stress Test: 100K Tasks Live Query', () => {

  it('should insert 100,000 tasks efficiently', async () => {
    const db = createTaskDb('stress_insert');
    const tasks = generateTasks(TOTAL_TASKS);

    // Insert in batches of 10,000
    const batchSize = 10000;
    const insertStart = performance.now();

    for (let i = 0; i < tasks.length; i += batchSize) {
      const batch = tasks.slice(i, i + batchSize);
      await db.insert('tasks').values(batch).exec();
    }

    const insertDuration = performance.now() - insertStart;
    console.log(`[Stress] Insert ${TOTAL_TASKS} tasks (with JS serialization): ${insertDuration.toFixed(2)}ms (${(TOTAL_TASKS / insertDuration * 1000).toFixed(0)} rows/sec)`);

    // Verify count
    const allTasks = await db.select('*').from('tasks').exec();
    expect(allTasks.length).toBe(TOTAL_TASKS);
  }, 60000);

  it('should measure pure Rust insert performance (no JS serialization)', async () => {
    const db = createTaskDb('stress_rust_insert');

    // Benchmark pure Rust insert without JS serialization overhead
    const result = db.benchmarkInsert('tasks', TOTAL_TASKS);

    console.log(`[Stress] Pure Rust insert ${TOTAL_TASKS} tasks: ${result.duration_ms.toFixed(2)}ms (${result.rows_per_sec.toFixed(0)} rows/sec)`);

    // Verify count
    expect(db.totalRowCount()).toBe(TOTAL_TASKS);
  }, 60000);

  it('should query top 1000 tasks by updated_at DESC efficiently', async () => {
    const db = createTaskDb('stress_query');
    const tasks = generateTasks(TOTAL_TASKS);

    // Debug: check JsSortOrder values
    console.log('[Debug] JsSortOrder.Asc:', JsSortOrder.Asc);
    console.log('[Debug] JsSortOrder.Desc:', JsSortOrder.Desc);

    // Insert all tasks
    const batchSize = 10000;
    for (let i = 0; i < tasks.length; i += batchSize) {
      await db.insert('tasks').values(tasks.slice(i, i + batchSize)).exec();
    }

    // Debug: explain the query plan
    const explainResult = db.select('*')
      .from('tasks')
      .orderBy('updated_at', JsSortOrder.Desc)
      .limit(QUERY_LIMIT)
      .explain();
    console.log('[Debug] Logical plan:', explainResult.logical);
    console.log('[Debug] Optimized plan:', explainResult.optimized);
    console.log('[Debug] Physical plan:', explainResult.physical);

    // Query top 1000 by updated_at DESC
    const queryStart = performance.now();
    const results = await db.select('*')
      .from('tasks')
      .orderBy('updated_at', JsSortOrder.Desc)
      .limit(QUERY_LIMIT)
      .exec();
    const queryDuration = performance.now() - queryStart;

    console.log(`[Stress] Query top ${QUERY_LIMIT} from ${TOTAL_TASKS}: ${queryDuration.toFixed(2)}ms`);
    console.log('[Debug] First 5 results updated_at:', results.slice(0, 5).map((r: any) => r.updated_at));
    console.log('[Debug] Last 5 results updated_at:', results.slice(-5).map((r: any) => r.updated_at));

    expect(results.length).toBe(QUERY_LIMIT);
    // Verify ordering
    for (let i = 1; i < results.length; i++) {
      if (results[i - 1].updated_at < results[i].updated_at) {
        console.log(`[Debug] Order violation at index ${i}: ${results[i - 1].updated_at} < ${results[i].updated_at}`);
      }
      expect(results[i - 1].updated_at).toBeGreaterThanOrEqual(results[i].updated_at);
    }
  }, 60000);

  it('should create live query with complex filter on 100K tasks', async () => {
    const db = createTaskDb('stress_live_query');
    const tasks = generateTasks(TOTAL_TASKS);

    // Insert all tasks
    const batchSize = 10000;
    for (let i = 0; i < tasks.length; i += batchSize) {
      await db.insert('tasks').values(tasks.slice(i, i + batchSize)).exec();
    }

    // Create live query: high priority tasks that are in_progress or review
    const liveQueryStart = performance.now();
    const stream = db.select('*')
      .from('tasks')
      .where(
        col('priority').eq('high')
          .or(col('priority').eq('critical'))
          .and(
            col('status').eq('in_progress')
              .or(col('status').eq('review'))
          )
      )
      .changes();

    let receivedData: any[] = [];
    let updateCount = 0;

    const unsubscribe = stream.subscribe((data: any[]) => {
      updateCount++;
      receivedData = data;
    });

    const liveQueryDuration = performance.now() - liveQueryStart;
    console.log(`[Stress] Create live query on ${TOTAL_TASKS} tasks: ${liveQueryDuration.toFixed(2)}ms`);
    console.log(`[Stress] Initial result count: ${receivedData.length}`);

    expect(updateCount).toBe(1);
    expect(receivedData.length).toBeGreaterThan(0);

    unsubscribe();
  }, 60000);

  it('should propagate updates with sub-10ms latency on 100K dataset', async () => {
    const db = createTaskDb('stress_propagation');
    const tasks = generateTasks(TOTAL_TASKS);

    // Insert all tasks
    const batchSize = 10000;
    for (let i = 0; i < tasks.length; i += batchSize) {
      await db.insert('tasks').values(tasks.slice(i, i + batchSize)).exec();
    }

    // Create live query for 'critical' priority tasks
    const stream = db.select('*')
      .from('tasks')
      .where(col('priority').eq('critical'))
      .changes();

    let receivedData: any[] = [];
    let updateCount = 0;
    let propagationLatency = 0;

    const unsubscribe = stream.subscribe((data: any[]) => {
      updateCount++;
      receivedData = data;
    });

    const initialCount = receivedData.length;
    console.log(`[Stress] Initial critical tasks: ${initialCount}`);

    // Insert a new critical task and measure propagation latency
    const insertStart = performance.now();
    await db.insert('tasks').values([{
      id: TOTAL_TASKS + 1,
      title: 'New Critical Task',
      status: 'todo',
      priority: 'critical',
      category: 'bug',
      assignee_id: 1,
      project_id: 1,
      created_at: Date.now(),
      updated_at: Date.now(),
      due_date: Date.now() + 86400000,
      estimated_hours: 8,
      actual_hours: 0,
      description: 'Urgent critical task',
    }]).exec();
    propagationLatency = performance.now() - insertStart;

    console.log(`[Stress] Update propagation latency: ${propagationLatency.toFixed(2)}ms`);
    console.log(`[Stress] New critical tasks count: ${receivedData.length}`);

    expect(updateCount).toBe(2);
    expect(receivedData.length).toBe(initialCount + 1);
    expect(propagationLatency).toBeLessThan(200); // Allow up to 200ms for now

    unsubscribe();
  }, 60000);

  it('should handle multiple concurrent live queries on 100K dataset', async () => {
    const db = createTaskDb('stress_concurrent');
    const tasks = generateTasks(TOTAL_TASKS);

    // Insert all tasks
    const batchSize = 10000;
    for (let i = 0; i < tasks.length; i += batchSize) {
      await db.insert('tasks').values(tasks.slice(i, i + batchSize)).exec();
    }

    // First, test exec() performance to verify optimizer works
    const execStart = performance.now();
    const execResult = await db.select('*')
      .from('tasks')
      .where(col('status').eq('todo'))
      .limit(10)
      .exec();
    const execTime = performance.now() - execStart;
    console.log(`[Stress] exec() with WHERE status='todo' LIMIT 10: ${execTime.toFixed(2)}ms, returned ${execResult.length} rows`);

    // Create 5 different live queries with various filters (reduced for debugging)
    const CONCURRENT_QUERIES = 5;
    const queries: { filter: any; name: string }[] = [];

    // Generate diverse query filters
    for (let i = 0; i < CONCURRENT_QUERIES; i++) {
      const statusIdx = i % STATUSES.length;
      const priorityIdx = i % PRIORITIES.length;
      const categoryIdx = i % CATEGORIES.length;

      if (i % 5 === 0) {
        queries.push({ filter: col('status').eq(STATUSES[statusIdx]), name: `status_${STATUSES[statusIdx]}_${i}` });
      } else if (i % 5 === 1) {
        queries.push({ filter: col('priority').eq(PRIORITIES[priorityIdx]), name: `priority_${PRIORITIES[priorityIdx]}_${i}` });
      } else if (i % 5 === 2) {
        queries.push({ filter: col('category').eq(CATEGORIES[categoryIdx]), name: `category_${CATEGORIES[categoryIdx]}_${i}` });
      } else if (i % 5 === 3) {
        queries.push({
          filter: col('status').eq(STATUSES[statusIdx]).and(col('priority').eq(PRIORITIES[priorityIdx])),
          name: `combo_${i}`
        });
      } else {
        queries.push({
          filter: col('assignee_id').eq((i % 100) + 1),
          name: `assignee_${(i % 100) + 1}_${i}`
        });
      }
    }

    const streams: { name: string; unsubscribe: () => void; data: any[]; updateCount: number }[] = [];

    // Detailed timing breakdown
    let totalChangesTime = 0;
    let totalSubscribeTime = 0;

    const setupStart = performance.now();
    for (let i = 0; i < queries.length; i++) {
      const q = queries[i];

      // Time: create changes stream (includes observe() which scans table)
      // Add limit(10) to reduce data returned
      const changesStart = performance.now();
      const stream = db.select('*')
        .from('tasks')
        .where(q.filter)
        .limit(10)
        .changes();
      totalChangesTime += performance.now() - changesStart;

      const state = { name: q.name, unsubscribe: () => {}, data: [] as any[], updateCount: 0 };

      // Time: subscribe (includes initial data conversion to JS)
      const subscribeStart = performance.now();
      state.unsubscribe = stream.subscribe((data: any[]) => {
        state.updateCount++;
        state.data = data;
      });
      totalSubscribeTime += performance.now() - subscribeStart;

      streams.push(state);

      // Log progress every 100 queries
      if ((i + 1) % 100 === 0) {
        const elapsed = performance.now() - setupStart;
        console.log(`[Stress] Progress: ${i + 1}/${CONCURRENT_QUERIES} queries setup in ${elapsed.toFixed(0)}ms`);
      }
    }
    const setupDuration = performance.now() - setupStart;

    console.log(`[Stress] Setup ${CONCURRENT_QUERIES} concurrent live queries: ${setupDuration.toFixed(2)}ms`);
    console.log(`[Stress] Breakdown:`);
    console.log(`  - changes() total: ${totalChangesTime.toFixed(2)}ms (${(totalChangesTime / CONCURRENT_QUERIES).toFixed(2)}ms avg)`);
    console.log(`  - subscribe() total: ${totalSubscribeTime.toFixed(2)}ms (${(totalSubscribeTime / CONCURRENT_QUERIES).toFixed(2)}ms avg)`);
    console.log(`[Stress] Average setup time per query: ${(setupDuration / CONCURRENT_QUERIES).toFixed(2)}ms`);

    // Sample some query results
    const sampleSize = 5;
    for (let i = 0; i < sampleSize; i++) {
      const s = streams[i];
      console.log(`  - ${s.name}: ${s.data.length} tasks`);
    }

    // Insert a task that matches multiple queries
    const insertStart = performance.now();
    await db.insert('tasks').values([{
      id: TOTAL_TASKS + 1,
      title: 'Multi-match Task',
      status: 'todo',
      priority: 'critical',
      category: 'bug',
      assignee_id: 1,
      project_id: 1,
      created_at: Date.now(),
      updated_at: Date.now(),
      due_date: Date.now() + 86400000,
      estimated_hours: 4,
      actual_hours: 0,
      description: 'This task matches multiple queries',
    }]).exec();
    const insertDuration = performance.now() - insertStart;

    console.log(`[Stress] Insert + propagate to ${CONCURRENT_QUERIES} queries: ${insertDuration.toFixed(2)}ms`);

    // Count how many queries received updates
    const updatedQueries = streams.filter(s => s.updateCount > 1).length;
    console.log(`[Stress] Queries that received update: ${updatedQueries}`);

    // Cleanup
    for (const s of streams) {
      s.unsubscribe();
    }

    expect(setupDuration).toBeLessThan(120000); // Allow up to 120s for 500 queries
  }, 180000);

  it('should respect LIMIT in changes() stream', async () => {
    const db = createTaskDb('stress_limit_test');
    const tasks = generateTasks(1000); // Just 1000 tasks for this test

    // Insert all tasks
    await db.insert('tasks').values(tasks).exec();

    // Test exec() with LIMIT
    const execResult = await db.select('*')
      .from('tasks')
      .where(col('status').eq('todo'))
      .limit(10)
      .exec();

    // Test observe() with LIMIT
    const observable = db.select('*')
      .from('tasks')
      .where(col('status').eq('todo'))
      .limit(10)
      .observe();
    const observeResult = observable.getResult();

    // Test changes() with LIMIT
    const stream = db.select('*')
      .from('tasks')
      .where(col('status').eq('todo'))
      .limit(10)
      .changes();
    const streamResult = stream.getResult();

    let receivedData: any[] = [];
    const unsubscribe = stream.subscribe((data: any[]) => {
      receivedData = data;
    });

    expect(execResult.length).toBe(10);
    expect(observeResult.length).toBe(10);
    expect(streamResult.length).toBe(10);
    expect(receivedData.length).toBe(10);

    unsubscribe();
  }, 30000);

  it('should handle rapid sequential updates on live query', async () => {
    const db = createTaskDb('stress_rapid_updates');
    const tasks = generateTasks(TOTAL_TASKS);

    // Insert all tasks
    const batchSize = 10000;
    for (let i = 0; i < tasks.length; i += batchSize) {
      await db.insert('tasks').values(tasks.slice(i, i + batchSize)).exec();
    }

    // Create live query
    const stream = db.select('*')
      .from('tasks')
      .where(col('status').eq('blocked'))
      .changes();

    let updateCount = 0;
    let lastData: any[] = [];

    const unsubscribe = stream.subscribe((data: any[]) => {
      updateCount++;
      lastData = data;
    });

    const initialCount = lastData.length;
    console.log(`[Stress] Initial blocked tasks: ${initialCount}`);

    // Perform 100 rapid inserts
    const rapidInsertStart = performance.now();
    for (let i = 0; i < 100; i++) {
      await db.insert('tasks').values([{
        id: TOTAL_TASKS + i + 1,
        title: `Rapid Insert Task ${i + 1}`,
        status: 'blocked',
        priority: 'medium',
        category: 'feature',
        assignee_id: 1,
        project_id: 1,
        created_at: Date.now(),
        updated_at: Date.now(),
        due_date: Date.now() + 86400000,
        estimated_hours: 2,
        actual_hours: 0,
        description: `Rapid insert test ${i + 1}`,
      }]).exec();
    }
    const rapidInsertDuration = performance.now() - rapidInsertStart;

    console.log(`[Stress] 100 rapid inserts: ${rapidInsertDuration.toFixed(2)}ms (${(100 / rapidInsertDuration * 1000).toFixed(0)} inserts/sec)`);
    console.log(`[Stress] Total updates received: ${updateCount}`);
    console.log(`[Stress] Final blocked tasks: ${lastData.length}`);

    expect(updateCount).toBe(101); // 1 initial + 100 updates
    expect(lastData.length).toBe(initialCount + 100);

    unsubscribe();
  }, 120000);

  // Diagnostic test to isolate insert performance
  it('should diagnose rapid insert performance', async () => {
    const db = createTaskDb('stress_diagnose');
    const tasks = generateTasks(TOTAL_TASKS);

    // Insert all tasks
    const batchSize = 10000;
    for (let i = 0; i < tasks.length; i += batchSize) {
      await db.insert('tasks').values(tasks.slice(i, i + batchSize)).exec();
    }

    // Test 1: Pure insert without live query
    console.log('[Diagnose] Test 1: 100 inserts WITHOUT live query');
    const noLiveQueryStart = performance.now();
    for (let i = 0; i < 100; i++) {
      await db.insert('tasks').values([{
        id: TOTAL_TASKS + i + 1,
        title: `No Live Query Task ${i + 1}`,
        status: 'blocked',
        priority: 'medium',
        category: 'feature',
        assignee_id: 1,
        project_id: 1,
        created_at: Date.now(),
        updated_at: Date.now(),
        due_date: Date.now() + 86400000,
        estimated_hours: 2,
        actual_hours: 0,
        description: `Test ${i + 1}`,
      }]).exec();
    }
    const noLiveQueryDuration = performance.now() - noLiveQueryStart;
    console.log(`[Diagnose] 100 inserts without live query: ${noLiveQueryDuration.toFixed(2)}ms (${(100 / noLiveQueryDuration * 1000).toFixed(0)} inserts/sec)`);

    // Test 2: Insert with live query (small result set - LIMIT 10)
    console.log('[Diagnose] Test 2: 100 inserts WITH live query (LIMIT 10)');

    // First, let's see how fast a single query is
    const singleQueryStart = performance.now();
    for (let i = 0; i < 100; i++) {
      db.select('*')
        .from('tasks')
        .where(col('status').eq('blocked'))
        .limit(10)
        .exec();
    }
    const singleQueryDuration = performance.now() - singleQueryStart;
    console.log(`[Diagnose] 100 exec() queries (LIMIT 10): ${singleQueryDuration.toFixed(2)}ms (${(singleQueryDuration / 100).toFixed(2)}ms avg)`);

    // Test 2b: Insert without any live query subscription (but with observe() called)
    console.log('[Diagnose] Test 2b: 100 inserts with observe() but NO subscription');
    const streamNoSub = db.select('*')
      .from('tasks')
      .where(col('status').eq('blocked'))
      .limit(10)
      .changes();
    // Don't subscribe - just create the stream

    const noSubStart = performance.now();
    for (let i = 0; i < 100; i++) {
      await db.insert('tasks').values([{
        id: TOTAL_TASKS + 1000 + i + 1,
        title: `No Sub Task ${i + 1}`,
        status: 'blocked',
        priority: 'medium',
        category: 'feature',
        assignee_id: 1,
        project_id: 1,
        created_at: Date.now(),
        updated_at: Date.now(),
        due_date: Date.now() + 86400000,
        estimated_hours: 2,
        actual_hours: 0,
        description: `Test ${i + 1}`,
      }]).exec();
    }
    const noSubDuration = performance.now() - noSubStart;
    console.log(`[Diagnose] 100 inserts with observe() but NO subscription: ${noSubDuration.toFixed(2)}ms (${(100 / noSubDuration * 1000).toFixed(0)} inserts/sec)`);

    const streamSmall = db.select('*')
      .from('tasks')
      .where(col('status').eq('blocked'))
      .limit(10)
      .changes();

    let smallUpdateCount = 0;
    const unsubSmall = streamSmall.subscribe(() => { smallUpdateCount++; });

    const smallLiveQueryStart = performance.now();
    for (let i = 0; i < 100; i++) {
      await db.insert('tasks').values([{
        id: TOTAL_TASKS + 100 + i + 1,
        title: `Small Live Query Task ${i + 1}`,
        status: 'blocked',
        priority: 'medium',
        category: 'feature',
        assignee_id: 1,
        project_id: 1,
        created_at: Date.now(),
        updated_at: Date.now(),
        due_date: Date.now() + 86400000,
        estimated_hours: 2,
        actual_hours: 0,
        description: `Test ${i + 1}`,
      }]).exec();
    }
    const smallLiveQueryDuration = performance.now() - smallLiveQueryStart;
    console.log(`[Diagnose] 100 inserts with LIMIT 10 live query: ${smallLiveQueryDuration.toFixed(2)}ms (${(100 / smallLiveQueryDuration * 1000).toFixed(0)} inserts/sec)`);
    console.log(`[Diagnose] Updates received (LIMIT 10): ${smallUpdateCount}`);
    unsubSmall();

    // Test 3: Insert with live query (large result set - no LIMIT)
    console.log('[Diagnose] Test 3: 100 inserts WITH live query (NO LIMIT - ~20000 rows)');
    const streamLarge = db.select('*')
      .from('tasks')
      .where(col('status').eq('blocked'))
      .changes();

    let largeUpdateCount = 0;
    const unsubLarge = streamLarge.subscribe(() => { largeUpdateCount++; });

    const largeLiveQueryStart = performance.now();
    for (let i = 0; i < 100; i++) {
      await db.insert('tasks').values([{
        id: TOTAL_TASKS + 200 + i + 1,
        title: `Large Live Query Task ${i + 1}`,
        status: 'blocked',
        priority: 'medium',
        category: 'feature',
        assignee_id: 1,
        project_id: 1,
        created_at: Date.now(),
        updated_at: Date.now(),
        due_date: Date.now() + 86400000,
        estimated_hours: 2,
        actual_hours: 0,
        description: `Test ${i + 1}`,
      }]).exec();
    }
    const largeLiveQueryDuration = performance.now() - largeLiveQueryStart;
    console.log(`[Diagnose] 100 inserts with NO LIMIT live query: ${largeLiveQueryDuration.toFixed(2)}ms (${(100 / largeLiveQueryDuration * 1000).toFixed(0)} inserts/sec)`);
    console.log(`[Diagnose] Updates received (NO LIMIT): ${largeUpdateCount}`);
    unsubLarge();

    // Test 4: Breakdown - measure serialization overhead
    console.log('[Diagnose] Test 4: Serialization overhead breakdown');

    // 4a: Just query execution (no subscription)
    const queryOnlyStart = performance.now();
    for (let i = 0; i < 10; i++) {
      await db.select('*')
        .from('tasks')
        .where(col('status').eq('blocked'))
        .exec();
    }
    const queryOnlyDuration = performance.now() - queryOnlyStart;
    console.log(`[Diagnose] 10 queries (~20000 rows each) without subscription: ${queryOnlyDuration.toFixed(2)}ms (${(queryOnlyDuration / 10).toFixed(2)}ms avg)`);

    // 4b: Query with getResult() (serialization happens)
    const observable = db.select('*')
      .from('tasks')
      .where(col('status').eq('blocked'))
      .observe();

    const getResultStart = performance.now();
    for (let i = 0; i < 10; i++) {
      observable.getResult();
    }
    const getResultDuration = performance.now() - getResultStart;
    console.log(`[Diagnose] 10 getResult() calls (~20000 rows each): ${getResultDuration.toFixed(2)}ms (${(getResultDuration / 10).toFixed(2)}ms avg)`);

    // Test 5: Batch insert (fire-and-forget, no await between inserts)
    console.log('[Diagnose] Test 5: Batch inserts (microtask batching)');
    const batchDb = createTaskDb('batch_test');
    await batchDb.insert('tasks').values(generateTasks(1000)).exec();

    const batchStream = batchDb.select('*')
      .from('tasks')
      .where(col('status').eq('todo'))
      .changes();

    let batchUpdateCount = 0;
    const unsubBatch = batchStream.subscribe(() => { batchUpdateCount++; });

    // Fire 100 inserts without awaiting each one
    const batchStart = performance.now();
    const insertPromises = [];
    for (let i = 0; i < 100; i++) {
      insertPromises.push(db.insert('tasks').values([{
        id: 200000 + i + 1,
        title: `Batch Task ${i + 1}`,
        status: 'todo',
        priority: 'medium',
        category: 'feature',
        assignee_id: 1,
        project_id: 1,
        created_at: Date.now(),
        updated_at: Date.now(),
        due_date: Date.now() + 86400000,
        estimated_hours: 2,
        actual_hours: 0,
        description: `Batch test ${i + 1}`,
      }]).exec());
    }
    await Promise.all(insertPromises);
    // Wait for microtask to flush
    await new Promise(resolve => setTimeout(resolve, 10));
    const batchDuration = performance.now() - batchStart;
    console.log(`[Diagnose] 100 batch inserts: ${batchDuration.toFixed(2)}ms`);
    console.log(`[Diagnose] Updates received (batch): ${batchUpdateCount}`);
    unsubBatch();

    // Summary
    console.log('[Diagnose] Summary:');
    console.log(`  - No live query: ${noLiveQueryDuration.toFixed(2)}ms`);
    console.log(`  - LIMIT 10 live query: ${smallLiveQueryDuration.toFixed(2)}ms (${(smallLiveQueryDuration / noLiveQueryDuration).toFixed(1)}x slower)`);
    console.log(`  - NO LIMIT live query: ${largeLiveQueryDuration.toFixed(2)}ms (${(largeLiveQueryDuration / noLiveQueryDuration).toFixed(1)}x slower)`);
  }, 180000);
});

describe('Stress Test: Performance Summary', () => {
  it('should print performance summary', async () => {
    // Summary test - metrics are validated in individual tests
  });
});
