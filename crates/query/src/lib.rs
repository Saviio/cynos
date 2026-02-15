//! Cynos Query - Query engine for Cynos in-memory database.
//!
//! This crate provides the query execution engine including:
//!
//! - `ast`: Expression and predicate AST definitions
//! - `planner`: Logical and physical query plans
//! - `optimizer`: Query optimization passes
//! - `executor`: Query execution operators (scan, filter, project, join, aggregate, sort, limit)
//! - `context`: Execution context
//! - `plan_cache`: Query plan caching for repeated queries

#![no_std]

extern crate alloc;

pub mod ast;
pub mod context;
pub mod executor;
pub mod optimizer;
pub mod plan_cache;
pub mod planner;
