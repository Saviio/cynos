# cynos-database

WASM API bindings and JavaScript API for Cynos database.

## Overview

This crate provides the public API for the Cynos in-memory database, including WASM bindings for use in JavaScript/TypeScript applications.

## Core Components

- `Database`: Main entry point for database operations
- `TableBuilder`: Builder for creating table schemas
- `SelectBuilder`: Query builder for SELECT statements
- `InsertBuilder`, `UpdateBuilder`, `DeleteBuilder`: DML builders
- `JsObservableQuery`: Observable query with subscription support

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

```javascript
const query = db.select()
  .from('users')
  .where(col('age').gt(18))
  .observe();

query.subscribe((changes) => {
  console.log('Added:', changes.added);
  console.log('Removed:', changes.removed);
});
```

## License

Apache-2.0
