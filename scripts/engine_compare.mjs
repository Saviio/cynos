import fs from 'node:fs/promises';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath, pathToFileURL } from 'node:url';

const SCRIPT_DIR = path.dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = path.resolve(SCRIPT_DIR, '..');
const TMP_DIR = path.join(ROOT_DIR, 'tmp');
const REPORT_PATH = path.join(TMP_DIR, 'engine_compare.md');
const CYNOS_WASM_PATH = path.join(ROOT_DIR, 'js', 'packages', 'core', 'dist', 'cynos.wasm');

const EXTERNAL_RESOLVE_BASES = [
  process.env.CYNOS_ENGINE_COMPARE_NODE_MODULES,
  SCRIPT_DIR,
  '/tmp/cynos-engine-compare',
].filter(Boolean);

function resolveExternal(specifier) {
  for (const base of EXTERNAL_RESOLVE_BASES) {
    const directCandidate = path.join(base, specifier);
    if (existsSync(directCandidate)) return directCandidate;

    const nodeModulesCandidate = path.join(base, 'node_modules', specifier);
    if (existsSync(nodeModulesCandidate)) return nodeModulesCandidate;
  }

  const searched = EXTERNAL_RESOLVE_BASES.join(', ');
  throw new Error(
    `Unable to resolve ${specifier}. Install benchmark dependencies in ${SCRIPT_DIR} or set CYNOS_ENGINE_COMPARE_NODE_MODULES. Searched: ${searched}`,
  );
}

function moduleUrl(filePath) {
  return pathToFileURL(filePath).href;
}

const cynosModule = await import(
  moduleUrl(path.join(ROOT_DIR, 'js', 'packages', 'core', 'dist', 'wasm.js')),
);
const resultSetModule = await import(
  moduleUrl(path.join(ROOT_DIR, 'js', 'packages', 'core', 'dist', 'result-set.js')),
);
const pgliteModule = await import(
  moduleUrl(resolveExternal('@electric-sql/pglite/dist/index.js')),
);
const pgliteLiveModule = await import(
  moduleUrl(resolveExternal('@electric-sql/pglite/dist/live/index.js')),
);
const sqlJsModule = await import(
  moduleUrl(resolveExternal('sql.js/dist/sql-wasm.js')),
);
const rxdbModule = await import(
  moduleUrl(resolveExternal('rxdb/dist/esm/index.js')),
);
const rxdbStorageMemoryModule = await import(
  moduleUrl(resolveExternal('rxdb/dist/esm/plugins/storage-memory/index.js')),
);

const {
  default: cynosInit,
  Database: CynosDatabase,
  JsDataType,
  JsSortOrder,
  ColumnOptions,
  col,
} = cynosModule;
const { ResultSet } = resultSetModule;
const { PGlite } = pgliteModule;
const { live } = pgliteLiveModule;
const { default: initSqlJs } = sqlJsModule;
const { createRxDatabase } = rxdbModule;
const { getRxStorageMemory } = rxdbStorageMemoryModule;

const SQLJS_WASM_DIR = path.dirname(resolveExternal('sql.js/dist/sql-wasm.js'));

const USER_COUNT = 10_000;
const DEPT_COUNT = 100;
const DOC_COUNT = 10_000;
const QUERY_REPEATS = 9;
const WARMUP_ROUNDS = 5;
const LIVE_UPDATES = 12;
const LIVE_WARMUP_UPDATES = 3;
const LIVE_TIMEOUT_MS = 2_000;

function uniqueId(prefix) {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2)}`;
}

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1] + sorted[mid]) / 2
    : sorted[mid];
}

function mean(values) {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withTimeout(promise, ms, label) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => {
          reject(new Error(`Timed out waiting for ${label} after ${ms}ms`));
        }, ms);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function fmtMs(value) {
  if (value == null || Number.isNaN(value)) return 'N/A';
  if (value < 1) return `${value.toFixed(3)} ms`;
  return `${value.toFixed(2)} ms`;
}

function fmtX(value) {
  if (value == null || Number.isNaN(value)) return 'N/A';
  return `${value.toFixed(2)}x`;
}

function fmtBytes(value) {
  if (value == null || Number.isNaN(value)) return 'N/A';
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${(value / (1024 * 1024)).toFixed(2)} MiB`;
}

function sqlString(value) {
  return `'${String(value).replace(/'/g, "''")}'`;
}

function makeUsers() {
  const tiers = ['bronze', 'silver', 'gold', 'platinum'];
  const cities = ['shanghai', 'beijing', 'shenzhen', 'hangzhou', 'chengdu'];
  const rows = [];
  for (let i = 1; i <= USER_COUNT; i++) {
    rows.push({
      id: i,
      age: 20 + (i % 50),
      dept_id: i % DEPT_COUNT,
      score: Number(((i % 1000) / 10).toFixed(1)),
      active: i % 3 !== 0,
      tier: tiers[i % tiers.length],
      name: `user_${i}`,
      city: cities[i % cities.length],
    });
  }
  return rows;
}

function makeDepartments() {
  const regions = ['east', 'north', 'south', 'west'];
  const rows = [];
  for (let i = 0; i < DEPT_COUNT; i++) {
    rows.push({
      id: i,
      name: `dept_${i}`,
      region: regions[i % regions.length],
    });
  }
  return rows;
}

const USERS = makeUsers();
const DEPARTMENTS = makeDepartments();
const RX_USERS = USERS.map((row) => ({
  id: String(row.id),
  age: row.age,
  deptId: row.dept_id,
  score: row.score,
  active: row.active,
  tier: row.tier,
  name: row.name,
  city: row.city,
}));

function makeDocuments() {
  const categories = ['tech', 'business', 'science', 'health', 'sports'];
  const statuses = ['published', 'draft', 'archived'];
  const regions = ['apac', 'emea', 'amer'];
  const rows = [];
  for (let i = 1; i <= DOC_COUNT; i++) {
    const category = categories[(i - 1) % categories.length];
    const status = statuses[(i - 1) % statuses.length];
    const priority = ((i - 1) % 5) + 1;
    const active = i % 2 === 0;
    rows.push({
      id: i,
      title: `doc_${i}`,
      updated_at: 1_700_000_000 + i,
      metadata: {
        category,
        status,
        priority,
        views: (i * 17) % 100_000,
        active,
        region: regions[(i - 1) % regions.length],
        tags: [category, active ? 'featured' : 'standard', `p${priority}`],
        author: `author_${i % 200}`,
      },
    });
  }
  return rows;
}

function toRxDocument(row) {
  return {
    id: String(row.id),
    title: row.title,
    updatedAt: row.updated_at,
    metadata: row.metadata,
  };
}

function makeMatchingDocument(id) {
  return {
    id,
    title: `matching_${id}`,
    updated_at: 1_800_000_000 + id,
    metadata: {
      category: 'tech',
      status: 'published',
      priority: 1,
      views: 900_000 + (id % 10_000),
      active: true,
      region: 'apac',
      tags: ['tech', 'featured', 'p1'],
      author: `author_${id % 200}`,
    },
  };
}

const DOCUMENTS = makeDocuments();
const RX_DOCUMENTS = DOCUMENTS.map(toRxDocument);

let cynosReady;
async function ensureCynosReady() {
  if (!cynosReady) {
    cynosReady = fs.readFile(CYNOS_WASM_PATH).then((bytes) =>
      cynosInit({ module_or_path: bytes }),
    );
  }
  await cynosReady;
}

let sqlJsReady;
async function ensureSqlJsReady() {
  if (!sqlJsReady) {
    sqlJsReady = initSqlJs({
      locateFile: (file) => path.join(SQLJS_WASM_DIR, file),
    });
  }
  return sqlJsReady;
}

async function benchmark(fn, { repeats = QUERY_REPEATS, warmup = WARMUP_ROUNDS } = {}) {
  for (let i = 0; i < warmup; i++) {
    await fn({ phase: 'warmup', iteration: i });
  }
  const samples = [];
  for (let i = 0; i < repeats; i++) {
    const start = performance.now();
    await fn({ phase: 'measure', iteration: i });
    samples.push(performance.now() - start);
  }
  return {
    median: median(samples),
    mean: mean(samples),
    samples,
  };
}

async function withCynos(fn) {
  await ensureCynosReady();
  const db = new CynosDatabase(uniqueId('cmp_cynos'));
  const users = db.createTable('users')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('age', JsDataType.Int32, null)
    .column('dept_id', JsDataType.Int32, null)
    .column('score', JsDataType.Float64, null)
    .column('active', JsDataType.Boolean, null)
    .column('tier', JsDataType.String, null)
    .column('name', JsDataType.String, null)
    .column('city', JsDataType.String, null)
    .index('idx_users_age', 'age')
    .index('idx_users_dept', 'dept_id');
  const departments = db.createTable('departments')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('name', JsDataType.String, null)
    .column('region', JsDataType.String, null);
  db.registerTable(users);
  db.registerTable(departments);
  await db.insert('users').values(USERS).exec();
  await db.insert('departments').values(DEPARTMENTS).exec();
  try {
    return await fn(db);
  } finally {
    if (typeof db.free === 'function') db.free();
  }
}

async function withPGlite(fn) {
  const db = new PGlite({ extensions: { live } });
  await db.exec(`
    create table users(
      id integer primary key,
      age integer not null,
      dept_id integer not null,
      score double precision not null,
      active boolean not null,
      tier text not null,
      name text not null,
      city text not null
    );
    create index idx_users_age on users(age);
    create index idx_users_dept on users(dept_id);
    create table departments(
      id integer primary key,
      name text not null,
      region text not null
    );
  `);

  await db.exec('begin');
  const deptValues = DEPARTMENTS
    .map((row) => `(${row.id}, ${sqlString(row.name)}, ${sqlString(row.region)})`)
    .join(',');
  await db.exec(`insert into departments(id, name, region) values ${deptValues};`);

  for (let start = 0; start < USERS.length; start += 1000) {
    const batch = USERS.slice(start, start + 1000)
      .map((row) => `(${row.id}, ${row.age}, ${row.dept_id}, ${row.score}, ${row.active}, ${sqlString(row.tier)}, ${sqlString(row.name)}, ${sqlString(row.city)})`)
      .join(',');
    await db.exec(`insert into users(id, age, dept_id, score, active, tier, name, city) values ${batch};`);
  }
  await db.exec('commit');

  try {
    return await fn(db);
  } finally {
    await db.close();
  }
}

async function withSqlite(fn) {
  const SQL = await ensureSqlJsReady();
  const db = new SQL.Database();
  db.run(`
    create table users(
      id integer primary key,
      age integer not null,
      dept_id integer not null,
      score real not null,
      active integer not null,
      tier text not null,
      name text not null,
      city text not null
    );
    create index idx_users_age on users(age);
    create index idx_users_dept on users(dept_id);
    create table departments(
      id integer primary key,
      name text not null,
      region text not null
    );
  `);

  const insertDept = db.prepare('insert into departments(id, name, region) values (?, ?, ?)');
  for (const row of DEPARTMENTS) insertDept.run([row.id, row.name, row.region]);
  insertDept.free();

  db.run('begin transaction');
  const insertUser = db.prepare(
    'insert into users(id, age, dept_id, score, active, tier, name, city) values (?, ?, ?, ?, ?, ?, ?, ?)',
  );
  for (const row of USERS) {
    insertUser.run([row.id, row.age, row.dept_id, row.score, row.active ? 1 : 0, row.tier, row.name, row.city]);
  }
  insertUser.free();
  db.run('commit');

  try {
    return await fn(db);
  } finally {
    db.close();
  }
}

async function withRxdb(fn) {
  const db = await createRxDatabase({
    name: uniqueId('cmp_rxdb'),
    storage: getRxStorageMemory(),
  });

  const schema = {
    title: 'user schema',
    version: 0,
    primaryKey: 'id',
    type: 'object',
    properties: {
      id: { type: 'string', maxLength: 100 },
      age: { type: 'integer', minimum: 0, maximum: 200, multipleOf: 1 },
      deptId: { type: 'integer', minimum: 0, maximum: 1000, multipleOf: 1 },
      score: { type: 'number', minimum: 0, maximum: 1000, multipleOf: 0.1 },
      active: { type: 'boolean' },
      tier: { type: 'string', maxLength: 100 },
      name: { type: 'string', maxLength: 100 },
      city: { type: 'string', maxLength: 100 },
    },
    required: ['id', 'age', 'deptId', 'score', 'active', 'tier', 'name', 'city'],
    indexes: ['age', 'deptId', 'active'],
  };

  const { users } = await db.addCollections({ users: { schema } });
  await users.bulkInsert(RX_USERS);

  try {
    return await fn({ db, users });
  } finally {
    await db.close();
  }
}

async function withCynosDocuments(fn, { metadataIndex = true } = {}) {
  await ensureCynosReady();
  const db = new CynosDatabase(uniqueId('cmp_cynos_docs'));
  let documents = db.createTable('documents')
    .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
    .column('title', JsDataType.String, null)
    .column('updated_at', JsDataType.Int64, null)
    .column('metadata', JsDataType.Jsonb, null)
    .index('idx_documents_updated_at', 'updated_at');
  if (metadataIndex) {
    documents = documents.index('idx_documents_metadata', 'metadata');
  }
  db.registerTable(documents);
  await db.insert('documents').values(DOCUMENTS).exec();
  try {
    return await fn(db);
  } finally {
    if (typeof db.free === 'function') db.free();
  }
}

async function withPGliteDocuments(fn) {
  const db = new PGlite();
  await db.exec(`
    create table documents(
      id bigint primary key,
      title text not null,
      updated_at bigint not null,
      metadata jsonb not null
    );
    create index idx_documents_category on documents ((metadata->>'category'));
    create index idx_documents_compound
      on documents (
        (metadata->>'category'),
        (metadata->>'status'),
        (((metadata->>'priority')::int)),
        updated_at desc
      );
    create index idx_documents_updated_at on documents(updated_at desc);
  `);

  await db.exec('begin');
  for (let start = 0; start < DOCUMENTS.length; start += 500) {
    const batch = DOCUMENTS.slice(start, start + 500)
      .map((row) => `(${row.id}, ${sqlString(row.title)}, ${row.updated_at}, ${sqlString(JSON.stringify(row.metadata))}::jsonb)`)
      .join(',');
    await db.exec(`insert into documents(id, title, updated_at, metadata) values ${batch};`);
  }
  await db.exec('commit');

  try {
    return await fn(db);
  } finally {
    await db.close();
  }
}

async function withSqliteDocuments(fn) {
  const SQL = await ensureSqlJsReady();
  const db = new SQL.Database();
  db.run(`
    create table documents(
      id integer primary key,
      title text not null,
      updated_at integer not null,
      metadata text not null
    );
    create index idx_documents_category
      on documents(json_extract(metadata, '$.category'));
    create index idx_documents_compound
      on documents(
        json_extract(metadata, '$.category'),
        json_extract(metadata, '$.status'),
        json_extract(metadata, '$.priority'),
        updated_at desc
      );
    create index idx_documents_updated_at on documents(updated_at desc);
  `);

  db.run('begin transaction');
  const stmt = db.prepare(
    'insert into documents(id, title, updated_at, metadata) values (?, ?, ?, ?)',
  );
  for (const row of DOCUMENTS) {
    stmt.run([row.id, row.title, row.updated_at, JSON.stringify(row.metadata)]);
  }
  stmt.free();
  db.run('commit');

  try {
    return await fn(db);
  } finally {
    db.close();
  }
}

async function withRxdbDocuments(fn) {
  const db = await createRxDatabase({
    name: uniqueId('cmp_rxdb_docs'),
    storage: getRxStorageMemory(),
  });

  const schema = {
    title: 'document schema',
    version: 0,
    primaryKey: 'id',
    type: 'object',
    properties: {
      id: { type: 'string', maxLength: 100 },
      title: { type: 'string', maxLength: 100 },
      updatedAt: { type: 'integer', minimum: 0, maximum: 3_000_000_000, multipleOf: 1 },
      metadata: {
        type: 'object',
        properties: {
          category: { type: 'string', maxLength: 100 },
          status: { type: 'string', maxLength: 100 },
          priority: { type: 'integer', minimum: 0, maximum: 10, multipleOf: 1 },
          views: { type: 'integer', minimum: 0, maximum: 1_000_000, multipleOf: 1 },
          active: { type: 'boolean' },
          region: { type: 'string', maxLength: 100 },
          author: { type: 'string', maxLength: 100 },
          tags: {
            type: 'array',
            items: { type: 'string' },
          },
        },
        required: ['category', 'status', 'priority', 'views', 'active', 'region', 'author', 'tags'],
      },
    },
    required: ['id', 'title', 'updatedAt', 'metadata'],
    indexes: [
      ['metadata.category', 'metadata.status', 'metadata.priority', 'updatedAt'],
      'metadata.category',
      'updatedAt',
    ],
  };

  const { documents } = await db.addCollections({ documents: { schema } });
  await documents.bulkInsert(RX_DOCUMENTS);

  try {
    return await fn({ db, documents });
  } finally {
    await db.close();
  }
}

async function measureCynos() {
  return withCynos(async (db) => {
    const pointQuery = db.select('*').from('users').where(col('id').eq(9000));
    const filter100Query = db.select('*').from('users').where(col('age').gt(60)).limit(100);
    const scan5000Query = db.select('*').from('users').limit(5000);
    const orderedRange500Query = db
      .select('*')
      .from('users')
      .where(col('age').between(30, 40))
      .orderBy('id', JsSortOrder.Asc)
      .limit(500);
    const join1000Query = db
      .select('*')
      .from('users')
      .innerJoin('departments', col('dept_id').eq('id'))
      .where(col('age').gt(60))
      .limit(1000);
    const aggregateQuery = db.select('*').from('users').groupBy('dept_id').count();

    const point = await benchmark(async () => {
      const rows = await pointQuery.exec();
      if (rows.length !== 1) throw new Error(`unexpected Cynos point rows: ${rows.length}`);
    });
    const filter100 = await benchmark(async () => {
      const rows = await filter100Query.exec();
      if (rows.length !== 100) throw new Error(`unexpected Cynos filter rows: ${rows.length}`);
    });
    const scan5000 = await benchmark(async () => {
      const rows = await scan5000Query.exec();
      if (rows.length !== 5000) throw new Error(`unexpected Cynos scan rows: ${rows.length}`);
    });
    const orderedRange500 = await benchmark(async () => {
      const rows = await orderedRange500Query.exec();
      if (rows.length !== 500) throw new Error(`unexpected Cynos ordered range rows: ${rows.length}`);
    });
    const joinProbeRows = await join1000Query.exec();
    const join1000 = joinProbeRows.length === 1000
      ? await benchmark(async () => {
          const rows = await join1000Query.exec();
          if (rows.length !== 1000) throw new Error(`unexpected Cynos join rows: ${rows.length}`);
        })
      : null;
    const aggregate = await benchmark(async () => {
      const rows = await aggregateQuery.exec();
      if (rows.length !== DEPT_COUNT) throw new Error(`unexpected Cynos aggregate rows: ${rows.length}`);
    });

    const filter100Layout = filter100Query.getSchemaLayout();
    const scan5000Layout = scan5000Query.getSchemaLayout();
    const pointLayout = pointQuery.getSchemaLayout();

    const pointBinaryOnly = await benchmark(async () => {
      const result = await pointQuery.execBinary();
      if (result.isEmpty()) throw new Error('unexpected empty Cynos binary point result');
      result.free();
    });
    const pointBinaryDecode = await benchmark(async () => {
      const result = await pointQuery.execBinary();
      const rs = new ResultSet(result, pointLayout);
      const rows = rs.toArray();
      if (rows.length !== 1) throw new Error(`unexpected Cynos point decode rows: ${rows.length}`);
      rs.free();
    });
    const filter100BinaryOnly = await benchmark(async () => {
      const result = await filter100Query.execBinary();
      if (result.isEmpty()) throw new Error('unexpected empty Cynos binary filter result');
      result.free();
    });
    const filter100BinaryDecode = await benchmark(async () => {
      const result = await filter100Query.execBinary();
      const rs = new ResultSet(result, filter100Layout);
      const rows = rs.toArray();
      if (rows.length !== 100) throw new Error(`unexpected Cynos filter decode rows: ${rows.length}`);
      rs.free();
    });
    const scan5000BinaryOnly = await benchmark(async () => {
      const result = await scan5000Query.execBinary();
      if (result.isEmpty()) throw new Error('unexpected empty Cynos binary scan result');
      result.free();
    });
    const scan5000BinaryDecode = await benchmark(async () => {
      const result = await scan5000Query.execBinary();
      const rs = new ResultSet(result, scan5000Layout);
      const rows = rs.toArray();
      if (rows.length !== 5000) throw new Error(`unexpected Cynos scan decode rows: ${rows.length}`);
      rs.free();
    });

    const pointBuffer = await pointQuery.execBinary();
    const pointBinaryBytes = pointBuffer.len();
    pointBuffer.free();
    const filterBuffer = await filter100Query.execBinary();
    const filter100BinaryBytes = filterBuffer.len();
    filterBuffer.free();
    const scanBuffer = await scan5000Query.execBinary();
    const scan5000BinaryBytes = scanBuffer.len();
    scanBuffer.free();

    const liveChanges = await measureCynosLive(db, 'changes', 1000);
    const liveTrace = await measureCynosLive(db, 'trace', 2000);

    return {
      point,
      filter100,
      scan5000,
      orderedRange500,
      join1000,
      joinProbeRowCount: joinProbeRows.length,
      aggregate,
      pointBinaryOnly,
      pointBinaryDecode,
      pointBinaryBytes,
      filter100BinaryOnly,
      filter100BinaryDecode,
      filter100BinaryBytes,
      scan5000BinaryOnly,
      scan5000BinaryDecode,
      scan5000BinaryBytes,
      liveChanges,
      liveTrace,
    };
  });
}

async function measureCynosLive(db, mode, baseOffset) {
  const query = db.select('*').from('users').where(col('dept_id').eq(42));
  const observable = mode === 'trace' ? query.trace() : query.changes();

  let resolveNext;
  let expectInitial = mode === 'changes';
  let sawInitial = false;
  const latencies = [];

  const nextUpdate = () => new Promise((resolve) => {
    resolveNext = resolve;
  });

  const unsubscribe = observable.subscribe((payload) => {
    if (expectInitial && !sawInitial) {
      sawInitial = true;
      return;
    }
    if (resolveNext) {
      const resolve = resolveNext;
      resolveNext = undefined;
      resolve(payload);
    }
  });

  if (mode === 'changes') {
    await delay(20);
    if (!sawInitial) expectInitial = false;
  }

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const id = USER_COUNT + baseOffset + i;
    const wait = nextUpdate();
    await db.insert('users').values([{ id, age: 30, dept_id: 42, score: 88.8, active: true, tier: 'gold', name: `warm_${id}`, city: 'shanghai' }]).exec();
    await withTimeout(wait, LIVE_TIMEOUT_MS, `Cynos ${mode} warmup update`);
  }

  for (let i = 0; i < LIVE_UPDATES; i++) {
    const id = USER_COUNT + baseOffset + LIVE_WARMUP_UPDATES + i;
    const wait = nextUpdate();
    const start = performance.now();
    await db.insert('users').values([{ id, age: 30, dept_id: 42, score: 88.8, active: true, tier: 'gold', name: `live_${id}`, city: 'shanghai' }]).exec();
    await withTimeout(wait, LIVE_TIMEOUT_MS, `Cynos ${mode} update`);
    latencies.push(performance.now() - start);
  }
  unsubscribe();

  return {
    median: median(latencies),
    mean: mean(latencies),
    samples: latencies,
  };
}

async function measurePGlite() {
  return withPGlite(async (db) => {
    const point = await benchmark(async () => {
      const result = await db.query('select * from users where id = $1', [9000]);
      if (result.rows.length !== 1) throw new Error(`unexpected PGlite point rows: ${result.rows.length}`);
    });
    const filter100 = await benchmark(async () => {
      const result = await db.query('select * from users where age > $1 limit 100', [60]);
      if (result.rows.length !== 100) throw new Error(`unexpected PGlite filter rows: ${result.rows.length}`);
    });
    const scan5000 = await benchmark(async () => {
      const result = await db.query('select * from users limit 5000');
      if (result.rows.length !== 5000) throw new Error(`unexpected PGlite scan rows: ${result.rows.length}`);
    });
    const orderedRange500 = await benchmark(async () => {
      const result = await db.query(
        'select * from users where age between $1 and $2 order by id asc limit 500',
        [30, 40],
      );
      if (result.rows.length !== 500) throw new Error(`unexpected PGlite ordered range rows: ${result.rows.length}`);
    });
    const join1000 = await benchmark(async () => {
      const result = await db.query(
        `select u.*, d.name as dept_name, d.region as dept_region
         from users u
         join departments d on u.dept_id = d.id
         where u.age > $1
         limit 1000`,
        [60],
      );
      if (result.rows.length !== 1000) throw new Error(`unexpected PGlite join rows: ${result.rows.length}`);
    });
    const aggregate = await benchmark(async () => {
      const result = await db.query('select dept_id, count(*) as count from users group by dept_id');
      if (result.rows.length !== DEPT_COUNT) throw new Error(`unexpected PGlite aggregate rows: ${result.rows.length}`);
    });
    const liveQuery = await measurePGliteLive(db);
    return { point, filter100, scan5000, orderedRange500, join1000, aggregate, liveQuery };
  });
}

async function measurePGliteLive(db) {
  const query = await db.live.incrementalQuery(
    'select * from users where dept_id = $1 order by id',
    [42],
    'id',
  );

  let resolveNext;
  const latencies = [];
  const nextUpdate = () => new Promise((resolve) => {
    resolveNext = resolve;
  });

  query.subscribe((payload) => {
    if (resolveNext) {
      const resolve = resolveNext;
      resolveNext = undefined;
      resolve(payload);
    }
  });

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const id = USER_COUNT + 3000 + i;
    const wait = nextUpdate();
    await db.query(
      'insert into users(id, age, dept_id, score, active, tier, name, city) values ($1, $2, $3, $4, $5, $6, $7, $8)',
      [id, 30, 42, 88.8, true, 'gold', `warm_${id}`, 'shanghai'],
    );
    await withTimeout(wait, LIVE_TIMEOUT_MS, 'PGlite live warmup update');
  }

  for (let i = 0; i < LIVE_UPDATES; i++) {
    const id = USER_COUNT + 3000 + LIVE_WARMUP_UPDATES + i;
    const wait = nextUpdate();
    const start = performance.now();
    await db.query(
      'insert into users(id, age, dept_id, score, active, tier, name, city) values ($1, $2, $3, $4, $5, $6, $7, $8)',
      [id, 30, 42, 88.8, true, 'gold', `live_${id}`, 'shanghai'],
    );
    await withTimeout(wait, LIVE_TIMEOUT_MS, 'PGlite live update');
    latencies.push(performance.now() - start);
  }

  await query.unsubscribe();
  return {
    median: median(latencies),
    mean: mean(latencies),
    samples: latencies,
  };
}

async function measureSqlite() {
  return withSqlite(async (db) => {
    const pointStmt = db.prepare('select * from users where id = ?');
    const filterStmt = db.prepare('select * from users where age > ? limit 100');
    const scanStmt = db.prepare('select * from users limit 5000');
    const orderedRangeStmt = db.prepare('select * from users where age between ? and ? order by id asc limit 500');
    const joinStmt = db.prepare(`
      select u.*, d.name as dept_name, d.region as dept_region
      from users u
      join departments d on u.dept_id = d.id
      where u.age > ?
      limit 1000
    `);
    const aggStmt = db.prepare('select dept_id, count(*) as count from users group by dept_id');

    const point = await benchmark(async () => {
      pointStmt.bind([9000]);
      const rows = [];
      while (pointStmt.step()) rows.push(pointStmt.getAsObject());
      pointStmt.reset();
      if (rows.length !== 1) throw new Error(`unexpected SQLite point rows: ${rows.length}`);
    });
    const filter100 = await benchmark(async () => {
      filterStmt.bind([60]);
      let count = 0;
      while (filterStmt.step()) {
        filterStmt.get();
        count += 1;
      }
      filterStmt.reset();
      if (count !== 100) throw new Error(`unexpected SQLite filter rows: ${count}`);
    });
    const scan5000 = await benchmark(async () => {
      let count = 0;
      while (scanStmt.step()) {
        scanStmt.get();
        count += 1;
      }
      scanStmt.reset();
      if (count !== 5000) throw new Error(`unexpected SQLite scan rows: ${count}`);
    });
    const orderedRange500 = await benchmark(async () => {
      orderedRangeStmt.bind([30, 40]);
      let count = 0;
      while (orderedRangeStmt.step()) {
        orderedRangeStmt.get();
        count += 1;
      }
      orderedRangeStmt.reset();
      if (count !== 500) throw new Error(`unexpected SQLite ordered range rows: ${count}`);
    });
    const join1000 = await benchmark(async () => {
      joinStmt.bind([60]);
      let count = 0;
      while (joinStmt.step()) {
        joinStmt.get();
        count += 1;
      }
      joinStmt.reset();
      if (count !== 1000) throw new Error(`unexpected SQLite join rows: ${count}`);
    });
    const aggregate = await benchmark(async () => {
      let count = 0;
      while (aggStmt.step()) {
        aggStmt.get();
        count += 1;
      }
      aggStmt.reset();
      if (count !== DEPT_COUNT) throw new Error(`unexpected SQLite aggregate rows: ${count}`);
    });

    pointStmt.free();
    filterStmt.free();
    scanStmt.free();
    orderedRangeStmt.free();
    joinStmt.free();
    aggStmt.free();
    return { point, filter100, scan5000, orderedRange500, join1000, aggregate };
  });
}

async function measureRxdb() {
  return withRxdb(async ({ users }) => {
    const point = await benchmark(async () => {
      const doc = await users.findOne('9000').exec();
      if (!doc) throw new Error('RxDB point lookup failed');
    });
    const filter100 = await benchmark(async () => {
      const docs = await users.find({ selector: { age: { $gt: 60 } }, limit: 100 }).exec();
      if (docs.length !== 100) throw new Error(`unexpected RxDB filter docs: ${docs.length}`);
    });
    const scan5000 = await benchmark(async () => {
      const docs = await users.find({ selector: {}, limit: 5000 }).exec();
      if (docs.length !== 5000) throw new Error(`unexpected RxDB scan docs: ${docs.length}`);
    });
    const liveQuery = await measureRxdbLive(users);
    return { point, filter100, scan5000, liveQuery };
  });
}

async function measureRxdbLive(users) {
  const query = users.find({ selector: { deptId: { $eq: 42 } } });
  let expectInitial = true;
  let initialSeen = false;
  let resolveNext;
  const latencies = [];
  const nextUpdate = () => new Promise((resolve) => {
    resolveNext = resolve;
  });

  const subscription = query.$.subscribe((docs) => {
    if (expectInitial && !initialSeen) {
      initialSeen = true;
      return;
    }
    if (resolveNext) {
      const resolve = resolveNext;
      resolveNext = undefined;
      resolve(docs);
    }
  });

  await delay(20);
  if (!initialSeen) expectInitial = false;

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const id = String(USER_COUNT + 4000 + i);
    const wait = nextUpdate();
    await users.insert({ id, age: 30, deptId: 42, score: 88.8, active: true, tier: 'gold', name: `warm_${id}`, city: 'shanghai' });
    await withTimeout(wait, LIVE_TIMEOUT_MS, 'RxDB live warmup update');
  }

  for (let i = 0; i < LIVE_UPDATES; i++) {
    const id = String(USER_COUNT + 4000 + LIVE_WARMUP_UPDATES + i);
    const wait = nextUpdate();
    const start = performance.now();
    await users.insert({ id, age: 30, deptId: 42, score: 88.8, active: true, tier: 'gold', name: `live_${id}`, city: 'shanghai' });
    await withTimeout(wait, LIVE_TIMEOUT_MS, 'RxDB live update');
    latencies.push(performance.now() - start);
  }
  subscription.unsubscribe();

  return {
    median: median(latencies),
    mean: mean(latencies),
    samples: latencies,
  };
}

function makeCynosDocumentQueries(db) {
  const jsonSingleQuery = db
    .select('*')
    .from('documents')
    .where(col('metadata').get('$.category').eq('tech'))
    .limit(100);
  const jsonCompoundQuery = db
    .select('*')
    .from('documents')
    .where(
      col('metadata').get('$.category').eq('tech')
        .and(col('metadata').get('$.status').eq('published'))
        .and(col('metadata').get('$.priority').eq(1))
    )
    .limit(100);
  const complexSortedQuery = db
    .select('*')
    .from('documents')
    .where(
      col('metadata').get('$.category').eq('tech')
        .and(col('metadata').get('$.status').eq('published'))
        .and(col('metadata').get('$.priority').eq(1))
    )
    .orderBy('updated_at', JsSortOrder.Desc)
    .limit(20);

  return {
    jsonSingleQuery,
    jsonCompoundQuery,
    complexSortedQuery,
  };
}

async function measureCynosDocumentQueries(db) {
  const { jsonSingleQuery, jsonCompoundQuery, complexSortedQuery } = makeCynosDocumentQueries(db);
  const jsonSingleExpected = (await jsonSingleQuery.exec()).length;
  const jsonCompoundExpected = (await jsonCompoundQuery.exec()).length;
  const complexSortedExpected = (await complexSortedQuery.exec()).length;

  const jsonSingle = await benchmark(async () => {
    const rows = await jsonSingleQuery.exec();
    if (rows.length !== jsonSingleExpected) throw new Error(`unexpected Cynos JSON single rows: ${rows.length}`);
  });
  const jsonCompound = await benchmark(async () => {
    const rows = await jsonCompoundQuery.exec();
    if (rows.length !== jsonCompoundExpected) throw new Error(`unexpected Cynos JSON compound rows: ${rows.length}`);
  });
  const complexSorted = await benchmark(async () => {
    const rows = await complexSortedQuery.exec();
    if (rows.length !== complexSortedExpected) throw new Error(`unexpected Cynos JSON sorted rows: ${rows.length}`);
  });

  return {
    jsonSingle,
    jsonCompound,
    complexSorted,
  };
}

async function measureCynosDocuments() {
  return withCynosDocuments(async (db) => {
    const documentQueries = await measureCynosDocumentQueries(db);
    const mutationDriven = await measureCynosDocumentMutation(db);
    const reactiveChanges = await measureCynosDocumentReactive(db, 'changes', 10_000);
    const reactiveTrace = await measureCynosDocumentReactive(db, 'trace', 20_000);

    return {
      ...documentQueries,
      mutationDriven,
      reactiveChanges,
      reactiveTrace,
    };
  });
}

function pickCynosDocumentQueryMetrics(measurement) {
  return {
    jsonSingle: measurement.jsonSingle,
    jsonCompound: measurement.jsonCompound,
    complexSorted: measurement.complexSorted,
  };
}

async function measureCynosJsonIndexImpact(withIndexMeasurement) {
  const withoutIndex = await withCynosDocuments(
    async (db) => measureCynosDocumentQueries(db),
    { metadataIndex: false },
  );

  return {
    withIndex: pickCynosDocumentQueryMetrics(withIndexMeasurement),
    withoutIndex,
  };
}

async function measureCynosDocumentMutation(db) {
  const query = db
    .select('*')
    .from('documents')
    .where(
      col('metadata').get('$.category').eq('tech')
        .and(col('metadata').get('$.status').eq('published'))
        .and(col('metadata').get('$.priority').eq(1))
    )
    .orderBy('updated_at', JsSortOrder.Desc)
    .limit(20);

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const row = makeMatchingDocument(DOC_COUNT + 1_000 + i);
    await db.insert('documents').values([row]).exec();
    const result = await query.exec();
    if (result[0]?.updated_at !== row.updated_at) {
      throw new Error('unexpected Cynos mutation warmup ordering');
    }
  }

  const samples = [];
  for (let i = 0; i < LIVE_UPDATES; i++) {
    const row = makeMatchingDocument(DOC_COUNT + 1_000 + LIVE_WARMUP_UPDATES + i);
    const start = performance.now();
    await db.insert('documents').values([row]).exec();
    const result = await query.exec();
    if (result[0]?.updated_at !== row.updated_at) {
      throw new Error('unexpected Cynos mutation ordering');
    }
    samples.push(performance.now() - start);
  }

  return {
    median: median(samples),
    mean: mean(samples),
    samples,
  };
}

async function measureCynosDocumentReactive(db, mode, baseOffset) {
  const query = db
    .select('*')
    .from('documents')
    .where(
      col('metadata').get('$.category').eq('tech')
        .and(col('metadata').get('$.status').eq('published'))
        .and(col('metadata').get('$.priority').eq(1))
    );
  const observable = mode === 'trace' ? query.trace() : query.changes();

  let resolveNext;
  let expectInitial = mode === 'changes';
  let sawInitial = false;
  const latencies = [];
  const nextUpdate = () => new Promise((resolve) => {
    resolveNext = resolve;
  });

  const unsubscribe = observable.subscribe((payload) => {
    if (expectInitial && !sawInitial) {
      sawInitial = true;
      return;
    }
    if (resolveNext) {
      const resolve = resolveNext;
      resolveNext = undefined;
      resolve(payload);
    }
  });

  if (mode === 'changes') {
    await delay(20);
    if (!sawInitial) expectInitial = false;
  }

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const row = makeMatchingDocument(DOC_COUNT + baseOffset + i);
    const wait = nextUpdate();
    await db.insert('documents').values([row]).exec();
    await withTimeout(wait, LIVE_TIMEOUT_MS, `Cynos JSON ${mode} warmup update`);
  }

  for (let i = 0; i < LIVE_UPDATES; i++) {
    const row = makeMatchingDocument(DOC_COUNT + baseOffset + LIVE_WARMUP_UPDATES + i);
    const wait = nextUpdate();
    const start = performance.now();
    await db.insert('documents').values([row]).exec();
    await withTimeout(wait, LIVE_TIMEOUT_MS, `Cynos JSON ${mode} update`);
    latencies.push(performance.now() - start);
  }
  unsubscribe();

  return {
    median: median(latencies),
    mean: mean(latencies),
    samples: latencies,
  };
}

async function measureRxdbDocuments() {
  return withRxdbDocuments(async ({ documents }) => {
    const jsonSingleQuery = {
      selector: { 'metadata.category': { $eq: 'tech' } },
      limit: 100,
    };
    const jsonCompoundQuery = {
      selector: {
        'metadata.category': { $eq: 'tech' },
        'metadata.status': { $eq: 'published' },
        'metadata.priority': { $eq: 1 },
      },
      limit: 100,
    };
    const complexSortedQuery = {
      selector: {
        'metadata.category': { $eq: 'tech' },
        'metadata.status': { $eq: 'published' },
        'metadata.priority': { $eq: 1 },
      },
      sort: [{ updatedAt: 'desc' }],
      limit: 20,
    };

    const jsonSingleExpected = (await documents.find(jsonSingleQuery).exec()).length;
    const jsonCompoundExpected = (await documents.find(jsonCompoundQuery).exec()).length;
    const complexSortedExpected = (await documents.find(complexSortedQuery).exec()).length;

    const jsonSingle = await benchmark(async () => {
      const docs = await documents.find(jsonSingleQuery).exec();
      if (docs.length !== jsonSingleExpected) throw new Error(`unexpected RxDB JSON single docs: ${docs.length}`);
    });
    const jsonCompound = await benchmark(async () => {
      const docs = await documents.find(jsonCompoundQuery).exec();
      if (docs.length !== jsonCompoundExpected) throw new Error(`unexpected RxDB JSON compound docs: ${docs.length}`);
    });
    const complexSorted = await benchmark(async () => {
      const docs = await documents.find(complexSortedQuery).exec();
      if (docs.length !== complexSortedExpected) throw new Error(`unexpected RxDB JSON sorted docs: ${docs.length}`);
    });

    const mutationDriven = await measureRxdbDocumentMutation(documents);
    const reactiveQuery = await measureRxdbDocumentReactive(documents);

    return {
      jsonSingle,
      jsonCompound,
      complexSorted,
      mutationDriven,
      reactiveQuery,
    };
  });
}

async function measurePGliteDocuments() {
  return withPGliteDocuments(async (db) => {
    const jsonSingleSql = `
      select *
      from documents
      where metadata->>'category' = $1
      limit 100
    `;
    const jsonCompoundSql = `
      select *
      from documents
      where metadata->>'category' = $1
        and metadata->>'status' = $2
        and ((metadata->>'priority')::int) = $3
      limit 100
    `;
    const complexSortedSql = `
      select *
      from documents
      where metadata->>'category' = $1
        and metadata->>'status' = $2
        and ((metadata->>'priority')::int) = $3
      order by updated_at desc
      limit 20
    `;

    const jsonSingleExpected = (await db.query(jsonSingleSql, ['tech'])).rows.length;
    const jsonCompoundExpected = (await db.query(jsonCompoundSql, ['tech', 'published', 1])).rows.length;
    const complexSortedExpected = (await db.query(complexSortedSql, ['tech', 'published', 1])).rows.length;

    const jsonSingle = await benchmark(async () => {
      const result = await db.query(jsonSingleSql, ['tech']);
      if (result.rows.length !== jsonSingleExpected) {
        throw new Error(`unexpected PGlite JSON single rows: ${result.rows.length}`);
      }
    });
    const jsonCompound = await benchmark(async () => {
      const result = await db.query(jsonCompoundSql, ['tech', 'published', 1]);
      if (result.rows.length !== jsonCompoundExpected) {
        throw new Error(`unexpected PGlite JSON compound rows: ${result.rows.length}`);
      }
    });
    const complexSorted = await benchmark(async () => {
      const result = await db.query(complexSortedSql, ['tech', 'published', 1]);
      if (result.rows.length !== complexSortedExpected) {
        throw new Error(`unexpected PGlite JSON sorted rows: ${result.rows.length}`);
      }
    });

    const mutationDriven = await measurePGliteDocumentMutation(db);

    return {
      jsonSingle,
      jsonCompound,
      complexSorted,
      mutationDriven,
    };
  });
}

async function measurePGliteDocumentMutation(db) {
  const sql = `
    select *
    from documents
    where metadata->>'category' = $1
      and metadata->>'status' = $2
      and ((metadata->>'priority')::int) = $3
    order by updated_at desc
    limit 20
  `;

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const row = makeMatchingDocument(DOC_COUNT + 7_000 + i);
    await db.query(
      'insert into documents(id, title, updated_at, metadata) values ($1, $2, $3, $4::jsonb)',
      [row.id, row.title, row.updated_at, JSON.stringify(row.metadata)],
    );
    const result = await db.query(sql, ['tech', 'published', 1]);
    if (Number(result.rows[0]?.updated_at) !== row.updated_at) {
      throw new Error('unexpected PGlite mutation warmup ordering');
    }
  }

  const samples = [];
  for (let i = 0; i < LIVE_UPDATES; i++) {
    const row = makeMatchingDocument(DOC_COUNT + 7_000 + LIVE_WARMUP_UPDATES + i);
    const start = performance.now();
    await db.query(
      'insert into documents(id, title, updated_at, metadata) values ($1, $2, $3, $4::jsonb)',
      [row.id, row.title, row.updated_at, JSON.stringify(row.metadata)],
    );
    const result = await db.query(sql, ['tech', 'published', 1]);
    if (Number(result.rows[0]?.updated_at) !== row.updated_at) {
      throw new Error('unexpected PGlite mutation ordering');
    }
    samples.push(performance.now() - start);
  }

  return {
    median: median(samples),
    mean: mean(samples),
    samples,
  };
}

async function measureSqliteDocuments() {
  return withSqliteDocuments(async (db) => {
    const jsonSingleStmt = db.prepare(`
      select *
      from documents
      where json_extract(metadata, '$.category') = ?
      limit 100
    `);
    const jsonCompoundStmt = db.prepare(`
      select *
      from documents
      where json_extract(metadata, '$.category') = ?
        and json_extract(metadata, '$.status') = ?
        and json_extract(metadata, '$.priority') = ?
      limit 100
    `);
    const complexSortedStmt = db.prepare(`
      select *
      from documents
      where json_extract(metadata, '$.category') = ?
        and json_extract(metadata, '$.status') = ?
        and json_extract(metadata, '$.priority') = ?
      order by updated_at desc
      limit 20
    `);

    const jsonSingleExpected = countSqliteRows(jsonSingleStmt, ['tech']);
    const jsonCompoundExpected = countSqliteRows(jsonCompoundStmt, ['tech', 'published', 1]);
    const complexSortedExpected = countSqliteRows(complexSortedStmt, ['tech', 'published', 1]);

    const jsonSingle = await benchmark(async () => {
      const count = countSqliteRows(jsonSingleStmt, ['tech']);
      if (count !== jsonSingleExpected) throw new Error(`unexpected SQLite JSON single rows: ${count}`);
    });
    const jsonCompound = await benchmark(async () => {
      const count = countSqliteRows(jsonCompoundStmt, ['tech', 'published', 1]);
      if (count !== jsonCompoundExpected) throw new Error(`unexpected SQLite JSON compound rows: ${count}`);
    });
    const complexSorted = await benchmark(async () => {
      const count = countSqliteRows(complexSortedStmt, ['tech', 'published', 1]);
      if (count !== complexSortedExpected) throw new Error(`unexpected SQLite JSON sorted rows: ${count}`);
    });

    const mutationDriven = await measureSqliteDocumentMutation(db);

    jsonSingleStmt.free();
    jsonCompoundStmt.free();
    complexSortedStmt.free();

    return {
      jsonSingle,
      jsonCompound,
      complexSorted,
      mutationDriven,
    };
  });
}

function countSqliteRows(stmt, params) {
  stmt.bind(params);
  let count = 0;
  while (stmt.step()) {
    stmt.get();
    count += 1;
  }
  stmt.reset();
  return count;
}

async function measureSqliteDocumentMutation(db) {
  const query = db.prepare(`
    select *
    from documents
    where json_extract(metadata, '$.category') = ?
      and json_extract(metadata, '$.status') = ?
      and json_extract(metadata, '$.priority') = ?
    order by updated_at desc
    limit 20
  `);
  const insert = db.prepare(
    'insert into documents(id, title, updated_at, metadata) values (?, ?, ?, ?)',
  );

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const row = makeMatchingDocument(DOC_COUNT + 9_000 + i);
    insert.run([row.id, row.title, row.updated_at, JSON.stringify(row.metadata)]);
    query.bind(['tech', 'published', 1]);
    if (!query.step()) {
      query.reset();
      throw new Error('unexpected empty SQLite mutation warmup result');
    }
    const current = query.getAsObject();
    query.reset();
    if (current.updated_at !== row.updated_at) {
      throw new Error('unexpected SQLite mutation warmup ordering');
    }
  }

  const samples = [];
  for (let i = 0; i < LIVE_UPDATES; i++) {
    const row = makeMatchingDocument(DOC_COUNT + 9_000 + LIVE_WARMUP_UPDATES + i);
    const start = performance.now();
    insert.run([row.id, row.title, row.updated_at, JSON.stringify(row.metadata)]);
    query.bind(['tech', 'published', 1]);
    if (!query.step()) {
      query.reset();
      throw new Error('unexpected empty SQLite mutation result');
    }
    const current = query.getAsObject();
    query.reset();
    if (current.updated_at !== row.updated_at) {
      throw new Error('unexpected SQLite mutation ordering');
    }
    samples.push(performance.now() - start);
  }

  query.free();
  insert.free();

  return {
    median: median(samples),
    mean: mean(samples),
    samples,
  };
}

async function measureRxdbDocumentMutation(documents) {
  const query = {
    selector: {
      'metadata.category': { $eq: 'tech' },
      'metadata.status': { $eq: 'published' },
      'metadata.priority': { $eq: 1 },
    },
    sort: [{ updatedAt: 'desc' }],
    limit: 20,
  };

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const row = toRxDocument(makeMatchingDocument(DOC_COUNT + 3_000 + i));
    await documents.insert(row);
    const result = await documents.find(query).exec();
    if (result[0]?.get('updatedAt') !== row.updatedAt) {
      throw new Error('unexpected RxDB mutation warmup ordering');
    }
  }

  const samples = [];
  for (let i = 0; i < LIVE_UPDATES; i++) {
    const row = toRxDocument(makeMatchingDocument(DOC_COUNT + 3_000 + LIVE_WARMUP_UPDATES + i));
    const start = performance.now();
    await documents.insert(row);
    const result = await documents.find(query).exec();
    if (result[0]?.get('updatedAt') !== row.updatedAt) {
      throw new Error('unexpected RxDB mutation ordering');
    }
    samples.push(performance.now() - start);
  }

  return {
    median: median(samples),
    mean: mean(samples),
    samples,
  };
}

async function measureRxdbDocumentReactive(documents) {
  const query = documents.find({
    selector: {
      'metadata.category': { $eq: 'tech' },
      'metadata.status': { $eq: 'published' },
      'metadata.priority': { $eq: 1 },
    },
  });

  let expectInitial = true;
  let initialSeen = false;
  let resolveNext;
  const latencies = [];
  const nextUpdate = () => new Promise((resolve) => {
    resolveNext = resolve;
  });

  const subscription = query.$.subscribe((docs) => {
    if (expectInitial && !initialSeen) {
      initialSeen = true;
      return;
    }
    if (resolveNext) {
      const resolve = resolveNext;
      resolveNext = undefined;
      resolve(docs);
    }
  });

  await delay(20);
  if (!initialSeen) expectInitial = false;

  for (let i = 0; i < LIVE_WARMUP_UPDATES; i++) {
    const row = toRxDocument(makeMatchingDocument(DOC_COUNT + 5_000 + i));
    const wait = nextUpdate();
    await documents.insert(row);
    await withTimeout(wait, LIVE_TIMEOUT_MS, 'RxDB JSON live warmup update');
  }

  for (let i = 0; i < LIVE_UPDATES; i++) {
    const row = toRxDocument(makeMatchingDocument(DOC_COUNT + 5_000 + LIVE_WARMUP_UPDATES + i));
    const wait = nextUpdate();
    const start = performance.now();
    await documents.insert(row);
    await withTimeout(wait, LIVE_TIMEOUT_MS, 'RxDB JSON live update');
    latencies.push(performance.now() - start);
  }
  subscription.unsubscribe();

  return {
    median: median(latencies),
    mean: mean(latencies),
    samples: latencies,
  };
}

async function measureInsertTimes() {
  const cynos = await (async () => {
    await ensureCynosReady();
    const start = performance.now();
    const db = new CynosDatabase(uniqueId('cmp_cynos_insert'));
    const users = db.createTable('users')
      .column('id', JsDataType.Int64, new ColumnOptions().primaryKey(true))
      .column('age', JsDataType.Int32, null)
      .column('dept_id', JsDataType.Int32, null)
      .column('score', JsDataType.Float64, null)
      .column('active', JsDataType.Boolean, null)
      .column('tier', JsDataType.String, null)
      .column('name', JsDataType.String, null)
      .column('city', JsDataType.String, null);
    db.registerTable(users);
    await db.insert('users').values(USERS).exec();
    const elapsed = performance.now() - start;
    if (typeof db.free === 'function') db.free();
    return elapsed;
  })();

  const pglite = await (async () => {
    const start = performance.now();
    const db = new PGlite();
    await db.exec(`
      create table users(
        id integer primary key,
        age integer not null,
        dept_id integer not null,
        score double precision not null,
        active boolean not null,
        tier text not null,
        name text not null,
        city text not null
      );
    `);
    await db.exec('begin');
    for (let offset = 0; offset < USERS.length; offset += 1000) {
      const batch = USERS.slice(offset, offset + 1000)
        .map((row) => `(${row.id}, ${row.age}, ${row.dept_id}, ${row.score}, ${row.active}, ${sqlString(row.tier)}, ${sqlString(row.name)}, ${sqlString(row.city)})`)
        .join(',');
      await db.exec(`insert into users(id, age, dept_id, score, active, tier, name, city) values ${batch};`);
    }
    await db.exec('commit');
    const elapsed = performance.now() - start;
    await db.close();
    return elapsed;
  })();

  const sqlite = await (async () => {
    const SQL = await ensureSqlJsReady();
    const start = performance.now();
    const db = new SQL.Database();
    db.run(`
      create table users(
        id integer primary key,
        age integer not null,
        dept_id integer not null,
        score real not null,
        active integer not null,
        tier text not null,
        name text not null,
        city text not null
      );
    `);
    db.run('begin transaction');
    const stmt = db.prepare(
      'insert into users(id, age, dept_id, score, active, tier, name, city) values (?, ?, ?, ?, ?, ?, ?, ?)',
    );
    for (const row of USERS) {
      stmt.run([row.id, row.age, row.dept_id, row.score, row.active ? 1 : 0, row.tier, row.name, row.city]);
    }
    stmt.free();
    db.run('commit');
    const elapsed = performance.now() - start;
    db.close();
    return elapsed;
  })();

  const rxdb = await (async () => {
    const start = performance.now();
    const db = await createRxDatabase({
      name: uniqueId('cmp_rxdb_insert'),
      storage: getRxStorageMemory(),
    });
    const schema = {
      title: 'user schema',
      version: 0,
      primaryKey: 'id',
      type: 'object',
      properties: {
        id: { type: 'string', maxLength: 100 },
        age: { type: 'integer', minimum: 0, maximum: 200, multipleOf: 1 },
        deptId: { type: 'integer', minimum: 0, maximum: 1000, multipleOf: 1 },
        score: { type: 'number', minimum: 0, maximum: 1000, multipleOf: 0.1 },
        active: { type: 'boolean' },
        tier: { type: 'string', maxLength: 100 },
        name: { type: 'string', maxLength: 100 },
        city: { type: 'string', maxLength: 100 },
      },
      required: ['id', 'age', 'deptId', 'score', 'active', 'tier', 'name', 'city'],
    };
    const { users } = await db.addCollections({ users: { schema } });
    await users.bulkInsert(RX_USERS);
    const elapsed = performance.now() - start;
    await db.close();
    return elapsed;
  })();

  return { cynos, pglite, sqlite, rxdb };
}

async function packageVersion(packageJsonSpecifier) {
  const raw = await fs.readFile(resolveExternal(packageJsonSpecifier), 'utf8');
  return JSON.parse(raw).version;
}

function buildReport(results, versions) {
  const lines = [];
  const cynosJoinCell = results.cynos.join1000
    ? fmtMs(results.cynos.join1000.median)
    : `incorrect in current local build (${results.cynos.joinProbeRowCount} rows)`;
  const cynosJsonIndexSingleSpeedup = results.cynosJsonIndex.withoutIndex.jsonSingle.median / results.cynosJsonIndex.withIndex.jsonSingle.median;
  const cynosJsonIndexCompoundSpeedup = results.cynosJsonIndex.withoutIndex.jsonCompound.median / results.cynosJsonIndex.withIndex.jsonCompound.median;
  const cynosJsonIndexComplexSpeedup = results.cynosJsonIndex.withoutIndex.complexSorted.median / results.cynosJsonIndex.withIndex.complexSorted.median;
  lines.push('# Temporary Engine Comparison');
  lines.push('');
  lines.push(`Generated on: ${new Date().toISOString()}`);
  lines.push('');
  lines.push('Scope: Node.js local benchmark with Cynos (WASM), SQLite via sql.js (WASM), PGlite (Postgres-on-WASM), plus a separate RxDB reactive/document reference section.');
  lines.push('');
  lines.push('## Method');
  lines.push('');
  lines.push(`- Dataset: ${USER_COUNT.toLocaleString()} users, ${DEPT_COUNT} departments.`);
  lines.push(`- Extra document dataset: ${DOC_COUNT.toLocaleString()} documents with nested metadata for Cynos, PGlite, SQLite JSON queries, plus RxDB nested-document comparisons.`);
  lines.push(`- User rows are intentionally wider than the first draft: integers + float + boolean + multiple strings, so JS/WASM result materialization cost shows up more clearly.`);
  lines.push('- Document rows intentionally use nested metadata, multi-predicate filters, sort+limit, and mutation-driven requery so the JSON section is not just measuring warm-cache steady-state reads.');
  lines.push('- JSON query indexes are enabled in the main document comparison for each engine where applicable; Cynos also gets a separate index on/off isolation section.');
  lines.push(`- Each query benchmark uses ${WARMUP_ROUNDS} warmup runs, then ${QUERY_REPEATS} measured runs; tables below report median latency.`);
  lines.push(`- Live-query latency uses ${LIVE_WARMUP_UPDATES} unmeasured warmup inserts, then ${LIVE_UPDATES} measured inserts.`);
  lines.push('- Cynos is already measured as Node.js + WASM here, so the runtime is aligned with sql.js and PGlite.');
  lines.push('- RxDB is not a relational WASM SQL engine; it is shown separately as a JS document/reactive baseline, not as an apples-to-apples SQL engine peer.');
  lines.push('- RxDB repeated read numbers on an unchanged dataset are heavily shaped by its query cache and document cache, so treat them as app-layer cache-hit latency, not raw relational scan throughput.');
  lines.push('');
  lines.push('## Versions');
  lines.push('');
  lines.push(`- Node.js: ${process.version}`);
  lines.push('- Cynos: local workspace JS/WASM build');
  lines.push(`- PGlite: ${versions.pglite}`);
  lines.push(`- RxDB: ${versions.rxdb}`);
  lines.push(`- sql.js: ${versions.sqljs}`);
  lines.push('');
  lines.push('## Capability Snapshot');
  lines.push('');
  lines.push('| Capability | Cynos | PGlite | SQLite (sql.js) | RxDB |');
  lines.push('| --- | --- | --- | --- | --- |');
  lines.push('| Relational joins | Yes | Yes | Yes | No built-in cross-collection relational join engine |');
  lines.push('| GROUP BY / aggregates | Yes | Yes | Yes | No built-in relational aggregate query layer |');
  lines.push('| Structured JSON query in this harness | Yes (`JSONB` path query + metadata index) | Yes (`jsonb` + expression indexes) | Yes (JSON1 `json_extract` + expression indexes) | Yes (nested Mango selectors) |');
  lines.push('| Live full-result query API | Yes (`changes()`) | Yes (`live.incrementalQuery()`) | No built-in API in this harness | Yes (`RxQuery.$`) |');
  lines.push('| Delta-first reactive path | Yes (`trace()`) | Partial (`live.changes`) | No | No comparable built-in delta stream |');
  lines.push('| Binary result transport | Yes (`execBinary()`) | No comparable engine-native path in this harness | No comparable engine-native path in this harness | No |');
  lines.push('');
  lines.push('## Insert 10K Rows');
  lines.push('');
  lines.push('| Engine | Time |');
  lines.push('| --- | ---: |');
  lines.push(`| Cynos | ${fmtMs(results.insert.cynos)} |`);
  lines.push(`| PGlite | ${fmtMs(results.insert.pglite)} |`);
  lines.push(`| SQLite (sql.js) | ${fmtMs(results.insert.sqlite)} |`);
  lines.push(`| RxDB | ${fmtMs(results.insert.rxdb)} |`);
  lines.push('');
  lines.push('## WASM Relational Engine Benchmarks');
  lines.push('');
  lines.push('| Benchmark | Cynos | PGlite | SQLite (sql.js) |');
  lines.push('| --- | ---: | ---: | ---: |');
  lines.push(`| Point lookup (` + '`id = 9000`' + `) | ${fmtMs(results.cynos.point.median)} | ${fmtMs(results.pglite.point.median)} | ${fmtMs(results.sqlite.point.median)} |`);
  lines.push(`| Indexed filter (` + '`age > 60 LIMIT 100`' + `) | ${fmtMs(results.cynos.filter100.median)} | ${fmtMs(results.pglite.filter100.median)} | ${fmtMs(results.sqlite.filter100.median)} |`);
  lines.push(`| Wide scan (` + '`SELECT * LIMIT 5000`' + `) | ${fmtMs(results.cynos.scan5000.median)} | ${fmtMs(results.pglite.scan5000.median)} | ${fmtMs(results.sqlite.scan5000.median)} |`);
  lines.push(`| Ordered range (` + '`age BETWEEN 30 AND 40 ORDER BY id LIMIT 500`' + `) | ${fmtMs(results.cynos.orderedRange500.median)} | ${fmtMs(results.pglite.orderedRange500.median)} | ${fmtMs(results.sqlite.orderedRange500.median)} |`);
  lines.push(`| Join (` + '`users JOIN departments WHERE age > 60 LIMIT 1000`' + `) | ${cynosJoinCell} | ${fmtMs(results.pglite.join1000.median)} | ${fmtMs(results.sqlite.join1000.median)} |`);
  lines.push(`| Aggregate (` + '`GROUP BY dept_id COUNT(*)`' + `) | ${fmtMs(results.cynos.aggregate.median)} | ${fmtMs(results.pglite.aggregate.median)} | ${fmtMs(results.sqlite.aggregate.median)} |`);
  if (!results.cynos.join1000) {
    lines.push('');
    lines.push(`Note: the current local Cynos JS/WASM build returned ${results.cynos.joinProbeRowCount} rows for the join workload, so I left the Cynos join cell as correctness-invalid instead of publishing a misleading timing.`);
  }
  lines.push('');
  lines.push('## Cynos Transport Breakdown');
  lines.push('');
  lines.push('This section is where Cynos has a design-specific path that the others do not: `execBinary()` lets the engine return a compact WASM buffer instead of eagerly materializing JS objects.');
  lines.push('');
  lines.push('| Workload | `exec()` | `execBinary()` only | `execBinary()` + `toArray()` | Binary size | `exec()` vs binary-only | `exec()` vs binary+decode |');
  lines.push('| --- | ---: | ---: | ---: | ---: | ---: | ---: |');
  lines.push(`| Point lookup (1 row) | ${fmtMs(results.cynos.point.median)} | ${fmtMs(results.cynos.pointBinaryOnly.median)} | ${fmtMs(results.cynos.pointBinaryDecode.median)} | ${fmtBytes(results.cynos.pointBinaryBytes)} | ${fmtX(results.cynos.point.median / results.cynos.pointBinaryOnly.median)} | ${fmtX(results.cynos.point.median / results.cynos.pointBinaryDecode.median)} |`);
  lines.push(`| Indexed filter (100 rows) | ${fmtMs(results.cynos.filter100.median)} | ${fmtMs(results.cynos.filter100BinaryOnly.median)} | ${fmtMs(results.cynos.filter100BinaryDecode.median)} | ${fmtBytes(results.cynos.filter100BinaryBytes)} | ${fmtX(results.cynos.filter100.median / results.cynos.filter100BinaryOnly.median)} | ${fmtX(results.cynos.filter100.median / results.cynos.filter100BinaryDecode.median)} |`);
  lines.push(`| Wide scan (5000 rows) | ${fmtMs(results.cynos.scan5000.median)} | ${fmtMs(results.cynos.scan5000BinaryOnly.median)} | ${fmtMs(results.cynos.scan5000BinaryDecode.median)} | ${fmtBytes(results.cynos.scan5000BinaryBytes)} | ${fmtX(results.cynos.scan5000.median / results.cynos.scan5000BinaryOnly.median)} | ${fmtX(results.cynos.scan5000.median / results.cynos.scan5000BinaryDecode.median)} |`);
  lines.push('');
  lines.push('## Reactive / Live Query Latency');
  lines.push('');
  lines.push('| Mode | Median latency |');
  lines.push('| --- | ---: |');
  lines.push(`| Cynos ` + '`changes()`' + ` | ${fmtMs(results.cynos.liveChanges.median)} |`);
  lines.push(`| Cynos ` + '`trace()`' + ` | ${fmtMs(results.cynos.liveTrace.median)} |`);
  lines.push(`| PGlite ` + '`live.incrementalQuery()`' + ` | ${fmtMs(results.pglite.liveQuery.median)} |`);
  lines.push(`| RxDB ` + '`RxQuery.$`' + ` | ${fmtMs(results.rxdb.liveQuery.median)} |`);
  lines.push(`| SQLite (sql.js) | N/A |`);
  lines.push('');
  lines.push('## RxDB Document / Reactive Reference');
  lines.push('');
  lines.push('| Benchmark | Cynos | RxDB |');
  lines.push('| --- | ---: | ---: |');
  lines.push(`| Point lookup | ${fmtMs(results.cynos.point.median)} | ${fmtMs(results.rxdb.point.median)} |`);
  lines.push(`| Filter (` + '`age > 60 LIMIT 100`' + `) | ${fmtMs(results.cynos.filter100.median)} | ${fmtMs(results.rxdb.filter100.median)} |`);
  lines.push(`| Wide scan (` + '`LIMIT 5000`' + `) | ${fmtMs(results.cynos.scan5000.median)} | ${fmtMs(results.rxdb.scan5000.median)} |`);
  lines.push(`| Live query update | ${fmtMs(results.cynos.liveChanges.median)} | ${fmtMs(results.rxdb.liveQuery.median)} |`);
  lines.push('');
  lines.push('## JSON / Document Query Benchmarks');
  lines.push('');
  lines.push('This section uses a separate 10K-document dataset with nested metadata. Cynos, PGlite, and SQLite run structured JSON predicates inside SQL/WASM engines; RxDB runs nested document selectors over its document store.');
  lines.push('');
  lines.push('| Benchmark | Cynos | PGlite | SQLite (sql.js) | RxDB |');
  lines.push('| --- | ---: | ---: | ---: | ---: |');
  lines.push(`| JSON filter (` + '`metadata.category = tech LIMIT 100`' + `) | ${fmtMs(results.cynosDocs.jsonSingle.median)} | ${fmtMs(results.pgliteDocs.jsonSingle.median)} | ${fmtMs(results.sqliteDocs.jsonSingle.median)} | ${fmtMs(results.rxdbDocs.jsonSingle.median)} |`);
  lines.push(`| Compound JSON filter (` + '`category + status + priority`' + `) | ${fmtMs(results.cynosDocs.jsonCompound.median)} | ${fmtMs(results.pgliteDocs.jsonCompound.median)} | ${fmtMs(results.sqliteDocs.jsonCompound.median)} | ${fmtMs(results.rxdbDocs.jsonCompound.median)} |`);
  lines.push(`| Complex doc query (` + '`category + status + priority ORDER BY updatedAt DESC LIMIT 20`' + `) | ${fmtMs(results.cynosDocs.complexSorted.median)} | ${fmtMs(results.pgliteDocs.complexSorted.median)} | ${fmtMs(results.sqliteDocs.complexSorted.median)} | ${fmtMs(results.rxdbDocs.complexSorted.median)} |`);
  lines.push(`| Mutation-driven requery (` + '`insert + complex query`' + `) | ${fmtMs(results.cynosDocs.mutationDriven.median)} | ${fmtMs(results.pgliteDocs.mutationDriven.median)} | ${fmtMs(results.sqliteDocs.mutationDriven.median)} | ${fmtMs(results.rxdbDocs.mutationDriven.median)} |`);
  lines.push('');
  lines.push('## Cynos JSONB Index Impact');
  lines.push('');
  lines.push('This section isolates the Cynos `metadata` secondary index. Both sides keep the same dataset and `updated_at` index; the only difference is whether the JSONB metadata index exists.');
  lines.push('');
  lines.push('| Benchmark | With metadata index | Without metadata index | Speedup |');
  lines.push('| --- | ---: | ---: | ---: |');
  lines.push(`| JSON filter (` + '`metadata.category = tech LIMIT 100`' + `) | ${fmtMs(results.cynosJsonIndex.withIndex.jsonSingle.median)} | ${fmtMs(results.cynosJsonIndex.withoutIndex.jsonSingle.median)} | ${fmtX(cynosJsonIndexSingleSpeedup)} |`);
  lines.push(`| Compound JSON filter (` + '`category + status + priority`' + `) | ${fmtMs(results.cynosJsonIndex.withIndex.jsonCompound.median)} | ${fmtMs(results.cynosJsonIndex.withoutIndex.jsonCompound.median)} | ${fmtX(cynosJsonIndexCompoundSpeedup)} |`);
  lines.push(`| Complex doc query (` + '`category + status + priority ORDER BY updated_at DESC LIMIT 20`' + `) | ${fmtMs(results.cynosJsonIndex.withIndex.complexSorted.median)} | ${fmtMs(results.cynosJsonIndex.withoutIndex.complexSorted.median)} | ${fmtX(cynosJsonIndexComplexSpeedup)} |`);
  lines.push('');
  lines.push('| Reactive complex document update | Cynos `changes()` | Cynos `trace()` | RxDB `RxQuery.$` |');
  lines.push('| --- | ---: | ---: | ---: |');
  lines.push(`| Insert matching doc into compound nested query | ${fmtMs(results.cynosDocs.reactiveChanges.median)} | ${fmtMs(results.cynosDocs.reactiveTrace.median)} | ${fmtMs(results.rxdbDocs.reactiveQuery.median)} |`);
  lines.push('');
  lines.push('## What This Suggests');
  lines.push('');
  lines.push('- Cynos is not trying to beat every mature SQL engine on every scalar query. In this harness, SQLite(sql.js) is still excellent on very small point lookups and aggregate-heavy paths.');
  lines.push('- Cynos becomes more differentiated once you care about one engine doing joins, aggregates, structured JSON queries, and reactive updates together inside a compact embedded runtime.');
  lines.push('- `execBinary()` is the clearest Cynos-specific advantage in JS/WASM embeddings: when result sets get wider or larger, it cuts object materialization cost and can materially outperform plain `exec()`.');
  lines.push('- Relative to PGlite, Cynos trades SQL breadth and Postgres compatibility for a tighter app-runtime execution path and much cheaper live-query updates in this workload.');
  lines.push('- Relative to SQLite and PGlite, Cynos can expose structured JSON query plus fine-grained reactive APIs without switching to a separate sync or query layer.');
  lines.push('- Relative to RxDB, the real Cynos advantage is not “single flat query faster on warm cache”; it is that the same engine can do relational joins, aggregates, JSONB path filters, binary result transport, and delta-style reactivity without switching query models.');
  lines.push('- The JSONB index-isolation section tells you whether Cynos path-query wins are coming from real index leverage or only from upper-layer caching; keep an eye on that delta when evaluating production schema choices.');
  lines.push('');
  lines.push('## Fact-Check Pointers');
  lines.push('');
  lines.push('- RxDB `RxQuery`, query cache, and EventReduce: https://rxdb.info/rx-query.html');
  lines.push('- RxDB Query Optimizer (premium / optional): https://rxdb.info/query-optimizer.html');
  lines.push('- PGlite live queries: https://pglite.dev/docs/live-queries');
  lines.push('- PostgreSQL JSON functions/operators (PGlite compatibility baseline): https://www.postgresql.org/docs/current/functions-json.html');
  lines.push('- SQLite JSON1 extension: https://www.sqlite.org/json1.html');
  lines.push('');
  return lines.join('\n');
}

async function main() {
  const versions = {
    pglite: await packageVersion('@electric-sql/pglite/package.json'),
    rxdb: await packageVersion('rxdb/package.json'),
    sqljs: await packageVersion('sql.js/package.json'),
  };

  const insert = await measureInsertTimes();
  const cynos = await measureCynos();
  const cynosDocs = await measureCynosDocuments();
  const cynosJsonIndex = await measureCynosJsonIndexImpact(cynosDocs);
  const pglite = await measurePGlite();
  const pgliteDocs = await measurePGliteDocuments();
  const sqlite = await measureSqlite();
  const sqliteDocs = await measureSqliteDocuments();
  const rxdb = await measureRxdb();
  const rxdbDocs = await measureRxdbDocuments();

  const results = { insert, cynos, cynosDocs, cynosJsonIndex, pglite, pgliteDocs, sqlite, sqliteDocs, rxdb, rxdbDocs };
  const report = buildReport(results, versions);
  await fs.mkdir(TMP_DIR, { recursive: true });
  await fs.writeFile(REPORT_PATH, report, 'utf8');
  console.log(report);
  console.error(`\nReport written to ${REPORT_PATH}`);
}

await main();
