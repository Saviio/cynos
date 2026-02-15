/**
 * 全链路性能追踪测试
 *
 * 50 万行主表 + 维度表 LEFT JOIN
 * 逐环节计时，定位瓶颈
 */

import { describe, it, expect, beforeAll } from 'vitest';
import init, {
  Database,
  JsDataType,
  ColumnOptions,
  col,
} from '../wasm/cynos_database.js';
import { ResultSet } from '../src/result-set.js';

beforeAll(async () => {
  await init();
});

// =============================================================================
// 配置
// =============================================================================

const MAIN_TABLE_SIZE = 500_000;
const CATEGORY_COUNT = 50;
const REGION_COUNT = 20;
const STATUS_COUNT = 5;

// =============================================================================
// 计时工具
// =============================================================================

interface TimingEntry {
  phase: string;
  ms: number;
  detail?: string;
}

function timer() {
  const entries: TimingEntry[] = [];
  let last = performance.now();

  return {
    mark(phase: string, detail?: string) {
      const now = performance.now();
      entries.push({ phase, ms: now - last, detail });
      last = now;
    },
    reset() {
      last = performance.now();
    },
    entries,
    print() {
      const total = entries.reduce((s, e) => s + e.ms, 0);
      console.log('');
      console.log('┌─────────────────────────────────────────────────────────┐');
      console.log('│ PIPELINE TRACE                                          │');
      console.log('├──────────────────────────────┬──────────┬───────────────┤');
      console.log('│ Phase                        │   ms     │   % of total  │');
      console.log('├──────────────────────────────┼──────────┼───────────────┤');
      for (const e of entries) {
        const pct = ((e.ms / total) * 100).toFixed(1);
        const name = (e.detail ? `${e.phase} (${e.detail})` : e.phase).padEnd(28);
        const ms = e.ms.toFixed(2).padStart(8);
        const pctStr = (pct + '%').padStart(13);
        console.log(`│ ${name} │ ${ms} │ ${pctStr} │`);
      }
      console.log('├──────────────────────────────┼──────────┼───────────────┤');
      const totalStr = total.toFixed(2).padStart(8);
      console.log(`│ ${'TOTAL'.padEnd(28)} │ ${totalStr} │ ${'100.0%'.padStart(13)} │`);
      console.log('└──────────────────────────────┴──────────┴───────────────┘');
      console.log('');
    },
  };
}

// =============================================================================
// 测试
// =============================================================================

describe('Pipeline Trace (500K rows)', () => {
  let db: InstanceType<typeof Database>;

  it('setup: create tables and insert 500K rows', () => {
    const t = timer();

    db = new Database('pipeline_trace');

    // 主表: orders (500K)
    const ordersBuilder = db.createTable('orders')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('customer_id', JsDataType.Int64, null)
      .column('category_id', JsDataType.Int32, null)
      .column('region_id', JsDataType.Int32, null)
      .column('status_id', JsDataType.Int32, null)
      .column('amount', JsDataType.Int64, null)
      .column('quantity', JsDataType.Int32, null)
      .column('note', JsDataType.String, new ColumnOptions().setNullable(true))
      .index('idx_category', 'category_id')
      .index('idx_region', 'region_id')
      .index('idx_status', 'status_id')
      .index('idx_amount', 'amount');
    db.registerTable(ordersBuilder);
    t.mark('create orders table');

    // 维度表: categories (50)
    const catBuilder = db.createTable('categories')
      .column('id', JsDataType.Int32, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(catBuilder);

    // 维度表: regions (20)
    const regBuilder = db.createTable('regions')
      .column('id', JsDataType.Int32, new ColumnOptions().primaryKey(true))
      .column('name', JsDataType.String, null);
    db.registerTable(regBuilder);

    // 维度表: statuses (5)
    const statusBuilder = db.createTable('statuses')
      .column('id', JsDataType.Int32, new ColumnOptions().primaryKey(true))
      .column('label', JsDataType.String, null);
    db.registerTable(statusBuilder);
    t.mark('create dimension tables');

    // 插入维度数据
    const categories = Array.from({ length: CATEGORY_COUNT }, (_, i) => ({
      id: i, name: `Category_${i}`,
    }));
    const regions = Array.from({ length: REGION_COUNT }, (_, i) => ({
      id: i, name: `Region_${i}`,
    }));
    const statuses = [
      { id: 0, label: 'pending' },
      { id: 1, label: 'processing' },
      { id: 2, label: 'shipped' },
      { id: 3, label: 'delivered' },
      { id: 4, label: 'cancelled' },
    ];

    db.insert('categories').values(categories).exec();
    db.insert('regions').values(regions).exec();
    db.insert('statuses').values(statuses).exec();
    t.mark('insert dimension data');

    // 插入 50 万行主表数据 (分批)
    const BATCH = 50_000;
    for (let offset = 0; offset < MAIN_TABLE_SIZE; offset += BATCH) {
      const batch = Array.from({ length: BATCH }, (_, i) => {
        const idx = offset + i;
        return {
          id: idx + 1,
          customer_id: 1000 + (idx % 10000),
          category_id: idx % CATEGORY_COUNT,
          region_id: idx % REGION_COUNT,
          status_id: idx % STATUS_COUNT,
          amount: 100 + (idx % 5000) * 10,
          quantity: 1 + (idx % 100),
          note: idx % 5 === 0 ? null : `Order note ${idx}`,
        };
      });
      db.insert('orders').values(batch).exec();
    }
    t.mark('insert 500K orders');

    t.print();
  });

  // ─────────────────────────────────────────────────────────────────────────
  // 1. 单表查询 - 不同选择率
  // ─────────────────────────────────────────────────────────────────────────

  it('trace: single table queries at different selectivities', async () => {
    const t = timer();
    const layout = db.select('*').from('orders').where(col('amount').gt(1)).getSchemaLayout();

    // 0.1% selectivity (~500 rows) - indexed range
    const q1 = db.select('*').from('orders').where(col('amount').gt(49800));
    const r1 = await q1.exec();
    t.mark('exec() 0.1%', `~${(r1 as any).length} rows`);

    t.reset();
    const b1 = await db.select('*').from('orders').where(col('amount').gt(49800)).execBinary();
    const rs1 = new ResultSet(b1, layout);
    t.mark('execBinary() 0.1%', `${rs1.length} rows`);

    t.reset();
    const arr1 = rs1.toArray();
    t.mark('toArray() 0.1%', `${arr1.length} rows`);
    rs1.free();

    // 1% selectivity (~5000 rows)
    t.reset();
    const r2 = await db.select('*').from('orders').where(col('amount').gt(49000)).exec();
    t.mark('exec() 1%', `~${(r2 as any).length} rows`);

    t.reset();
    const b2 = await db.select('*').from('orders').where(col('amount').gt(49000)).execBinary();
    const rs2 = new ResultSet(b2, layout);
    t.mark('execBinary() 1%', `${rs2.length} rows`);

    t.reset();
    const arr2 = rs2.toArray();
    t.mark('toArray() 1%', `${arr2.length} rows`);
    rs2.free();

    // 10% selectivity (~50000 rows)
    t.reset();
    const r3 = await db.select('*').from('orders').where(col('amount').gt(45000)).exec();
    t.mark('exec() 10%', `~${(r3 as any).length} rows`);

    t.reset();
    const b3 = await db.select('*').from('orders').where(col('amount').gt(45000)).execBinary();
    const rs3 = new ResultSet(b3, layout);
    t.mark('execBinary() 10%', `${rs3.length} rows`);

    t.reset();
    const arr3 = rs3.toArray();
    t.mark('toArray() 10%', `${arr3.length} rows`);
    rs3.free();

    // Full table scan (100%)
    t.reset();
    const r4 = await db.select('*').from('orders').exec();
    t.mark('exec() 100%', `${(r4 as any).length} rows`);

    t.reset();
    const b4 = await db.select('*').from('orders').execBinary();
    const rs4 = new ResultSet(b4, layout);
    t.mark('execBinary() 100%', `${rs4.rowCount} rows`);

    t.reset();
    const arr4 = rs4.toArray();
    t.mark('toArray() 100%', `${arr4.length} rows`);
    rs4.free();

    t.print();
    expect(true).toBe(true);
  });

  // ─────────────────────────────────────────────────────────────────────────
  // 2. execBinary 分解: WASM exec vs toArray vs free
  // ─────────────────────────────────────────────────────────────────────────

  it('trace: execBinary breakdown (10% selectivity)', async () => {
    const t = timer();
    const RUNS = 5;
    const layout = db.select('*').from('orders').where(col('amount').gt(45000)).getSchemaLayout();

    let execMs = 0, toArrayMs = 0, freeMs = 0, lazyMs = 0;

    for (let i = 0; i < RUNS; i++) {
      let s = performance.now();
      const b = await db.select('*').from('orders').where(col('amount').gt(45000)).execBinary();
      execMs += performance.now() - s;

      s = performance.now();
      const rs = new ResultSet(b, layout);
      // Lazy access: just sum amounts without toArray
      let sum = 0;
      for (let j = 0; j < rs.length; j++) {
        sum += rs.getNumber(j, 5) as number; // amount column
      }
      lazyMs += performance.now() - s;

      s = performance.now();
      const arr = rs.toArray();
      toArrayMs += performance.now() - s;

      s = performance.now();
      rs.free();
      freeMs += performance.now() - s;
    }

    t.entries.push({ phase: 'execBinary()', ms: execMs / RUNS, detail: 'WASM→binary' });
    t.entries.push({ phase: 'lazy column access', ms: lazyMs / RUNS, detail: 'sum 50K values' });
    t.entries.push({ phase: 'toArray()', ms: toArrayMs / RUNS, detail: '→JS objects' });
    t.entries.push({ phase: 'free()', ms: freeMs / RUNS, detail: 'WASM dealloc' });

    t.print();
    expect(true).toBe(true);
  });

  // ─────────────────────────────────────────────────────────────────────────
  // 3. JOIN 查询 (通过 observe 路径)
  // ─────────────────────────────────────────────────────────────────────────

  it('trace: LEFT JOIN via observe (orders × categories)', async () => {
    const t = timer();

    // orders LEFT JOIN categories ON orders.category_id = categories.id
    const joinCond = col('category_id').eq('id');

    t.reset();
    const observable = db.select('*')
      .from('orders')
      .leftJoin('categories', joinCond)
      .observe();
    t.mark('observe() + initial exec', '500K × 50 LEFT JOIN');

    t.reset();
    const result = observable.getResult();
    t.mark('getResult() → JS objects', `${result.length} rows`);

    // Binary protocol path
    const layout = observable.getSchemaLayout();
    t.reset();
    const binary = observable.getResultBinary();
    t.mark('getResultBinary() → binary', `${binary.len()} bytes`);

    t.reset();
    const rs = new ResultSet(binary, layout);
    const arr = rs.toArray();
    t.mark('toArray() from binary', `${arr.length} rows`);
    rs.free();

    t.print();
    expect(result.length).toBe(MAIN_TABLE_SIZE);
  });

  // ─────────────────────────────────────────────────────────────────────────
  // 4. 多 JOIN
  // ─────────────────────────────────────────────────────────────────────────

  it('trace: multi LEFT JOIN (orders × categories × regions × statuses)', async () => {
    const t = timer();

    const catJoin = col('category_id').eq('id');
    const regJoin = col('region_id').eq('id');
    const statusJoin = col('status_id').eq('id');

    t.reset();
    const observable = db.select('*')
      .from('orders')
      .leftJoin('categories', catJoin)
      .leftJoin('regions', regJoin)
      .leftJoin('statuses', statusJoin)
      .observe();
    t.mark('observe() 3× LEFT JOIN', '500K × 50 × 20 × 5');

    t.reset();
    const result = observable.getResult();
    t.mark('getResult() → JS objects', `${result.length} rows`);

    // Binary protocol path
    const layout = observable.getSchemaLayout();
    t.reset();
    const binary = observable.getResultBinary();
    t.mark('getResultBinary() → binary', `${binary.len()} bytes`);

    t.reset();
    const rs = new ResultSet(binary, layout);
    const arr = rs.toArray();
    t.mark('toArray() from binary', `${arr.length} rows`);
    rs.free();

    t.print();
    expect(result.length).toBe(MAIN_TABLE_SIZE);
  });

  // ─────────────────────────────────────────────────────────────────────────
  // 5. 带 WHERE 的 JOIN
  // ─────────────────────────────────────────────────────────────────────────

  it('trace: filtered JOIN (WHERE amount > 45000, ~10%)', async () => {
    const t = timer();

    const catJoin = col('category_id').eq('id');

    t.reset();
    const observable = db.select('*')
      .from('orders')
      .where(col('amount').gt(45000))
      .leftJoin('categories', catJoin)
      .observe();
    t.mark('observe() filtered JOIN', '~50K × 50 LEFT JOIN');

    t.reset();
    const result = observable.getResult();
    t.mark('getResult()', `${result.length} rows`);

    t.print();
    expect(result.length).toBeGreaterThan(0);
  });

  // ─────────────────────────────────────────────────────────────────────────
  // 6. 对比: 单表 exec vs execBinary vs JOIN observe
  // ─────────────────────────────────────────────────────────────────────────

  it('summary: comparison table', async () => {
    const results: { op: string; ms: number; rows: number }[] = [];
    const layout = db.select('*').from('orders').where(col('amount').gt(45000)).getSchemaLayout();
    const fullLayout = db.select('*').from('orders').getSchemaLayout();

    // Single table exec()
    let s = performance.now();
    const r1 = await db.select('*').from('orders')
      .where(col('amount').gt(45000)).exec();
    results.push({ op: 'exec() 10%', ms: performance.now() - s, rows: (r1 as any).length });

    // Single table execBinary()
    s = performance.now();
    const b1 = await db.select('*').from('orders')
      .where(col('amount').gt(45000)).execBinary();
    const rs1 = new ResultSet(b1, layout);
    results.push({ op: 'execBinary() 10%', ms: performance.now() - s, rows: rs1.length });
    rs1.free();

    // execBinary + toArray
    s = performance.now();
    const b2 = await db.select('*').from('orders')
      .where(col('amount').gt(45000)).execBinary();
    const rs2 = new ResultSet(b2, layout);
    const arr2 = rs2.toArray();
    results.push({ op: 'execBinary()+toArray() 10%', ms: performance.now() - s, rows: arr2.length });
    rs2.free();

    // Full table exec()
    s = performance.now();
    const r3 = await db.select('*').from('orders').exec();
    results.push({ op: 'exec() 100%', ms: performance.now() - s, rows: (r3 as any).length });

    // Full table execBinary()
    s = performance.now();
    const b3 = await db.select('*').from('orders').execBinary();
    const rs3 = new ResultSet(b3, fullLayout);
    results.push({ op: 'execBinary() 100%', ms: performance.now() - s, rows: rs3.length });
    rs3.free();

    // JOIN observe (old path)
    s = performance.now();
    const catJoin = col('category_id').eq('id');
    const obs = db.select('*').from('orders')
      .leftJoin('categories', catJoin).observe();
    const jr = obs.getResult();
    results.push({ op: 'JOIN getResult()', ms: performance.now() - s, rows: jr.length });

    // JOIN observe (binary path)
    const joinLayout = obs.getSchemaLayout();
    s = performance.now();
    const jb = obs.getResultBinary();
    const jrs = new ResultSet(jb, joinLayout);
    results.push({ op: 'JOIN getResultBinary()', ms: performance.now() - s, rows: jrs.length });
    jrs.free();

    console.log('');
    console.log('┌──────────────────────────────────────────────────────────────┐');
    console.log('│ SUMMARY COMPARISON (500K rows)                               │');
    console.log('├──────────────────────────────────┬──────────┬────────────────┤');
    console.log('│ Operation                        │   ms     │   Rows         │');
    console.log('├──────────────────────────────────┼──────────┼────────────────┤');
    for (const r of results) {
      const op = r.op.padEnd(32);
      const ms = r.ms.toFixed(2).padStart(8);
      const rows = r.rows.toLocaleString().padStart(14);
      console.log(`│ ${op} │ ${ms} │ ${rows} │`);
    }
    console.log('└──────────────────────────────────┴──────────┴────────────────┘');
    console.log('');

    expect(true).toBe(true);
  });
});

// =============================================================================
// 真实业务场景: 500K 底表上的小量级 CRUD
// =============================================================================

describe('Business CRUD Trace (500K base, small reads)', () => {
  let db: InstanceType<typeof Database>;

  it('setup: create 500K-row base table', () => {
    const t = timer();

    db = new Database('crud_trace');

    const builder = db.createTable('orders')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('customer_id', JsDataType.Int64, null)
      .column('category_id', JsDataType.Int32, null)
      .column('region_id', JsDataType.Int32, null)
      .column('status_id', JsDataType.Int32, null)
      .column('amount', JsDataType.Int64, null)
      .column('quantity', JsDataType.Int32, null)
      .column('note', JsDataType.String, new ColumnOptions().setNullable(true))
      .index('idx_category', 'category_id')
      .index('idx_region', 'region_id')
      .index('idx_status', 'status_id')
      .index('idx_amount', 'amount');
    db.registerTable(builder);
    t.mark('create table');

    const BATCH = 50_000;
    for (let offset = 0; offset < MAIN_TABLE_SIZE; offset += BATCH) {
      const batch = Array.from({ length: BATCH }, (_, i) => {
        const idx = offset + i;
        return {
          id: idx + 1,
          customer_id: 1000 + (idx % 10000),
          category_id: idx % CATEGORY_COUNT,
          region_id: idx % REGION_COUNT,
          status_id: idx % STATUS_COUNT,
          amount: 100 + (idx % 5000) * 10,
          quantity: 1 + (idx % 100),
          note: idx % 5 === 0 ? null : `Order note ${idx}`,
        };
      });
      db.insert('orders').values(batch).exec();
    }
    t.mark('insert 500K rows');

    t.print();
  });

  // ─────────────────────────────────────────────────────────────────────────
  // Read: 1 / 100 / 1000 / 3000 行
  // ─────────────────────────────────────────────────────────────────────────

  it('trace: Read 1 / 100 / 1000 / 3000 rows (exec vs execBinary+toArray)', async () => {
    const layout = db.select('*').from('orders').where(col('id').eq(1)).getSchemaLayout();
    const RUNS = 10;

    // Use limit() for precise row counts on the 500K base table
    const targets = [
      { label: 'Read 1', buildQuery: () => db.select('*').from('orders').where(col('id').eq(42)) },
      { label: 'Read 100', buildQuery: () => db.select('*').from('orders').where(col('amount').gt(45000)).limit(100) },
      { label: 'Read 1000', buildQuery: () => db.select('*').from('orders').where(col('amount').gt(45000)).limit(1000) },
      { label: 'Read 3000', buildQuery: () => db.select('*').from('orders').where(col('amount').gt(45000)).limit(3000) },
    ];

    const results: { op: string; ms: number; rows: number }[] = [];

    for (const { label, buildQuery } of targets) {
      // Warmup
      await buildQuery().exec();

      // exec() — old path, returns JS objects directly
      let totalExec = 0;
      let rowCount = 0;
      for (let i = 0; i < RUNS; i++) {
        const s = performance.now();
        const r = await buildQuery().exec();
        totalExec += performance.now() - s;
        rowCount = (r as any).length;
      }
      results.push({ op: `${label} exec()`, ms: totalExec / RUNS, rows: rowCount });

      // execBinary() — binary only (no toArray)
      let totalBin = 0;
      for (let i = 0; i < RUNS; i++) {
        const s = performance.now();
        const b = await buildQuery().execBinary();
        const rs = new ResultSet(b, layout);
        totalBin += performance.now() - s;
        rowCount = rs.length;
        rs.free();
      }
      results.push({ op: `${label} execBinary()`, ms: totalBin / RUNS, rows: rowCount });

      // execBinary() + toArray() — full materialization
      let totalFull = 0;
      for (let i = 0; i < RUNS; i++) {
        const s = performance.now();
        const b = await buildQuery().execBinary();
        const rs = new ResultSet(b, layout);
        const arr = rs.toArray();
        totalFull += performance.now() - s;
        rowCount = arr.length;
        rs.free();
      }
      results.push({ op: `${label} binary+toArray()`, ms: totalFull / RUNS, rows: rowCount });
    }

    console.log('');
    console.log('┌──────────────────────────────────────────────────────────────────┐');
    console.log('│ CRUD READ TRACE (500K base table, avg of 10 runs)               │');
    console.log('├──────────────────────────────────┬──────────┬────────────────────┤');
    console.log('│ Operation                        │   ms     │   Rows             │');
    console.log('├──────────────────────────────────┼──────────┼────────────────────┤');
    for (const r of results) {
      const op = r.op.padEnd(32);
      const ms = r.ms.toFixed(3).padStart(8);
      const rows = r.rows.toLocaleString().padStart(18);
      console.log(`│ ${op} │ ${ms} │ ${rows} │`);
    }
    console.log('└──────────────────────────────────┴──────────┴────────────────────┘');
    console.log('');

    expect(true).toBe(true);
  });

  // ─────────────────────────────────────────────────────────────────────────
  // Create / Update / Delete 小批量
  // ─────────────────────────────────────────────────────────────────────────

  it('trace: Create / Update / Delete on 500K base', async () => {
    const RUNS = 10;
    const results: { op: string; ms: number }[] = [];

    // Insert 1 row
    let total = 0;
    for (let i = 0; i < RUNS; i++) {
      const row = { id: MAIN_TABLE_SIZE + 1000 + i, customer_id: 9999, category_id: 1, region_id: 1, status_id: 1, amount: 5000, quantity: 10, note: 'bench' };
      const s = performance.now();
      db.insert('orders').values([row]).exec();
      total += performance.now() - s;
    }
    results.push({ op: `Insert 1 row`, ms: total / RUNS });

    // Insert 100 rows
    total = 0;
    for (let i = 0; i < RUNS; i++) {
      const base = MAIN_TABLE_SIZE + 10000 + i * 100;
      const rows = Array.from({ length: 100 }, (_, j) => ({
        id: base + j, customer_id: 9999, category_id: 2, region_id: 2, status_id: 2, amount: 6000, quantity: 5, note: 'bench100',
      }));
      const s = performance.now();
      db.insert('orders').values(rows).exec();
      total += performance.now() - s;
    }
    results.push({ op: `Insert 100 rows`, ms: total / RUNS });

    // Update 1 row (by PK) — update non-indexed column to avoid constraint issues
    total = 0;
    for (let i = 0; i < RUNS; i++) {
      const s = performance.now();
      await db.update('orders').set('note', `updated_${i}`).where(col('id').eq(i + 1)).exec();
      total += performance.now() - s;
    }
    results.push({ op: `Update 1 row (by PK)`, ms: total / RUNS });

    // Update ~100 rows (by indexed range) — update non-indexed column
    total = 0;
    for (let i = 0; i < RUNS; i++) {
      const s = performance.now();
      await db.update('orders').set('note', `batch_${i}`).where(col('amount').gt(49990)).exec();
      total += performance.now() - s;
    }
    results.push({ op: `Update ~100 rows (indexed)`, ms: total / RUNS });

    // Delete 1 row (by PK)
    total = 0;
    for (let i = 0; i < RUNS; i++) {
      const targetId = MAIN_TABLE_SIZE + 1000 + i;
      const s = performance.now();
      await db.delete('orders').where(col('id').eq(targetId)).exec();
      total += performance.now() - s;
    }
    results.push({ op: `Delete 1 row (by PK)`, ms: total / RUNS });

    // Delete ~100 rows (by indexed range) — use the inserted bench100 rows
    total = 0;
    for (let i = 0; i < RUNS; i++) {
      const base = MAIN_TABLE_SIZE + 10000 + i * 100;
      const s = performance.now();
      await db.delete('orders').where(col('id').eq(base)).exec();
      total += performance.now() - s;
    }
    results.push({ op: `Delete 1 row (bench batch)`, ms: total / RUNS });

    console.log('');
    console.log('┌──────────────────────────────────────────────────────────────────┐');
    console.log('│ CRUD WRITE TRACE (500K base table, avg of 10 runs)              │');
    console.log('├──────────────────────────────────┬──────────────────────────────┤');
    console.log('│ Operation                        │   ms (avg)                   │');
    console.log('├──────────────────────────────────┼──────────────────────────────┤');
    for (const r of results) {
      const op = r.op.padEnd(32);
      const ms = r.ms.toFixed(3).padStart(28);
      console.log(`│ ${op} │ ${ms} │`);
    }
    console.log('└──────────────────────────────────┴──────────────────────────────┘');
    console.log('');

    expect(true).toBe(true);
  });
});
