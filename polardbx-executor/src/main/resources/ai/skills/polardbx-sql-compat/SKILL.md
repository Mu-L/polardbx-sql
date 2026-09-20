---
name: polardbx-sql-compat
description: |
  MySQL to PolarDB-X compatibility checks, Sequence usage, and distributed transactions for PolarDB-X 2.0 Enterprise Edition AUTO mode.
  Use when migrating MySQL SQL to PolarDB-X, checking compatibility differences, using Sequence, or understanding distributed transaction behavior.
  Triggers: "MySQL兼容性", "MySQL compatibility", "迁移PolarDB-X", "migrate to PolarDB-X", "Sequence", "自增", "AUTO_INCREMENT", "分布式事务", "distributed transaction", "不支持的语法", "unsupported syntax", "TSO", "XA"
metadata:
  version: 0.2.0
---

# PolarDB-X SQL Compatibility & Diagnostics

MySQL to PolarDB-X compatibility checks, Sequence usage, and distributed transactions for PolarDB-X 2.0 Enterprise Edition AUTO mode.

**Scope**: PolarDB-X 2.0 Enterprise Edition + AUTO mode database only.

Not applicable to:
- PolarDB-X 1.0 (DRDS 1.0)
- PolarDB-X 2.0 Standard Edition
- PolarDB-X 2.0 Enterprise Edition DRDS mode databases

## Core Workflow

1. **Confirm the target engine and version**:
   - Run `SELECT VERSION();` to determine the instance type:
     - Result contains `TDDL` with version > 5.4.12 → **2.0 Enterprise Edition**, this skill applies.
     - Result contains `TDDL` with version <= 5.4.12 → **DRDS 1.0**. **HARD STOP — refuse.**
     - Result contains `X-Cluster` → **2.0 Standard Edition**. **HARD STOP — refuse.**
   - Run `SHOW CREATE DATABASE db_name;` to verify AUTO mode (MODE = 'auto').
   - Parse the version number (e.g., 5.4.19) — affects feature availability (NEW SEQUENCE requires 5.4.14+).

2. **MySQL SQL compatibility check** (when user provides MySQL SQL):
   - Identify unsupported features and provide PolarDB-X alternatives.
   - Clearly mark behavioral differences and version requirements.
   - Common unsupported features: stored procedures, triggers, EVENTs, SPATIAL, NATURAL JOIN, `:=` assignment, subqueries in HAVING/JOIN ON.

3. **Sequence usage** (when user needs auto-increment/unique IDs):
   - Default type: `NEW SEQUENCE` (5.4.14+), globally ordered, distributed alternative to AUTO_INCREMENT.
   - Other types: GROUP (high-performance batch), SIMPLE (single-point), TIME (time-based).
   - Creation: `CREATE [NEW|GROUP|SIMPLE|TIME] SEQUENCE seq_name [START WITH n]`

## Key Differences Quick Reference

- **Sequence**: Globally unique sequence, default `NEW SEQUENCE` (5.4.14+). Types: NEW (globally ordered) / GROUP (high-performance batch) / SIMPLE (single-point monotone) / TIME (timestamp-based).
- **Distributed transactions**: Based on TSO global clock + MVCC + 2PC, strong consistency by default. Single-shard transactions automatically optimized to local transactions. Isolation levels: READ_COMMITTED (default), REPEATABLE_READ.
- **Unsupported MySQL features**:
  - Stored procedures / triggers / EVENTs / SPATIAL / GEOMETRY
  - LOAD XML / HANDLER
  - STRAIGHT_JOIN / NATURAL JOIN → use standard JOIN syntax
  - `:=` assignment operator → move logic to application layer
  - Subqueries in HAVING/JOIN ON clauses → rewrite as JOINs or CTEs
- **Behavioral differences**:
  - AUTO_INCREMENT with Sequence: globally unique but may have gaps; not necessarily monotone within single connection.
  - LAST_INSERT_ID(): returns the first value of the batch, not the last.
  - REPLACE INTO / INSERT ... ON DUPLICATE KEY: works correctly but may have different locking behavior.

## Best Practices

1. **Avoid unsupported MySQL syntax**: Don't use stored procedures, triggers, EVENTs, SPATIAL, NATURAL JOIN, `:=`, etc.
2. **Rewrite HAVING/JOIN ON subqueries**: Use JOINs or CTEs instead.
3. **Check long transactions before DDL**: Long transactions can block DDL via MDL locks.
4. **Prefer NEW SEQUENCE**: Use `NEW SEQUENCE` (5.4.14+) for globally ordered unique IDs; use GROUP for high-throughput scenarios where global ordering is not required.
5. **Test compatibility before migration**: Run MySQL SQL through PolarDB-X compatibility check before going live.
6. **For EXPLAIN diagnostics**: Use `plan-analysis` skill for execution plan interpretation and slow query diagnosis.

## Reference Links

| Reference | Description |
|-----------|-------------|
| [references/mysql-compatibility-notes.md](references/mysql-compatibility-notes.md) | MySQL vs PolarDB-X compatibility differences and development limitations |
| [references/sequence.md](references/sequence.md) | Sequence types (NEW/GROUP/SIMPLE/TIME), creation and usage |
| [references/transactions.md](references/transactions.md) | Distributed transaction model, isolation levels, and considerations |
