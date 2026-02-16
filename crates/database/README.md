# cynos-database

WASM API bindings and JavaScript API for Cynos database.

## Overview

This crate provides the public API for the Cynos in-memory database, including WASM bindings for use in JavaScript/TypeScript applications.

## Core Components

- `Database`: Main entry point for database operations
- `TableBuilder`: Builder for creating table schemas
- `SelectBuilder`: Query builder for SELECT statements with reactive query support
- `InsertBuilder`, `UpdateBuilder`, `DeleteBuilder`: DML builders
- `JsObservableQuery`: Re-query observable — re-executes cached physical plan on each change, O(result_set)
- `JsIvmObservableQuery`: IVM observable — propagates DBSP deltas through dataflow, O(Δoutput)
- `JsChangesStream`: Convenience wrapper over re-query with immediate initial emission

## Features

- Full WASM support for browser and Node.js
- Type-safe query builder API
- Reactive queries with subscription support
- Binary protocol for efficient data transfer

## Usage (JavaScript)

```javascript
import { Database, DataType, col } from '@cynos/core';

const db = await Database.create('mydb');

db.createTable('users')
  .column('id', DataType.Int64, { primaryKey: true })
  .column('name', DataType.String)
  .column('age', DataType.Int32)
  .build();

await db.insert('users').values([
  { id: 1, name: 'Alice', age: 25 },
  { id: 2, name: 'Bob', age: 30 },
]).exec();

const results = await db.select()
  .from('users')
  .where(col('age').gt(25))
  .exec();
```

## Reactive Queries

Two reactive query strategies with explicit API:

```javascript
// IVM path — O(Δoutput), delta-based via DBSP dataflow
// Only supports incrementalizable operators (no ORDER BY / LIMIT)
const ivm = db.select('*')
  .from('users')
  .where(col('age').gt(18))
  .trace();

ivm.subscribe((delta) => {
  console.log('Added:', delta.added);   // only new rows
  console.log('Removed:', delta.removed); // only removed rows
});

// Re-query path — O(result_set), re-executes full query on each change
// Supports all operators including ORDER BY / LIMIT
const requery = db.select('*')
  .from('users')
  .where(col('age').gt(18))
  .orderBy('name')
  .changes();

requery.subscribe((rows) => {
  console.log('Current result:', rows); // full result set
});
```

## License

Apache-2.0
