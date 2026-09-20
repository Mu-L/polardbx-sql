---
name: polardbx-partition-design
description: |
  Design partition schemes, select partition keys, create GSI, and write CREATE TABLE SQL for PolarDB-X 2.0 Enterprise Edition AUTO mode databases.
  Use when designing partition schemes, selecting partition keys, converting single tables to partitioned tables, or creating GSI/UGSI indexes on PolarDB-X.
  Triggers: "partition design", "分区设计", "partition key", "分区键", "GSI", "create table", "建表", "partitioned table", "分区表", "single table to partitioned", "单表转分区", "table sharding", "distributed table", "AUTO mode", "256 partitions", "CO_HASH", "BROADCAST", "SINGLE"
metadata:
  version: 0.1.0
---

# PolarDB-X Partition Design

Design partition schemes, select partition keys, create GSI, and write CREATE TABLE SQL for PolarDB-X 2.0 Enterprise Edition AUTO mode databases.

**Scope**: PolarDB-X 2.0 Enterprise Edition + AUTO mode database only.

Not applicable to:
- PolarDB-X 1.0 (DRDS 1.0)
- PolarDB-X 2.0 Standard Edition
- PolarDB-X 2.0 Enterprise Edition DRDS mode databases

## Core Workflow

**⚠️ CRITICAL: If the user's question involves any of the following, switch to the dedicated skill immediately — do NOT handle these in this skill:**
- **TTL / data expiration / cold data archiving / auto-add partitions** → switch to `polardbx-ttl20` skill
- **Online DDL / lock-free DDL / DDL safety / OMC / MDL lock** → switch to `polardbx-online-ddl` skill
- **Pagination / deep paging / large table traversal / LIMIT optimization** → switch to `polardbx-pagination` skill

1. **Confirm the target engine and version**:
   - Run `SELECT VERSION();` to determine the instance type:
     - Result contains `TDDL` with version > 5.4.12 → **2.0 Enterprise Edition**, this skill applies.
     - Result contains `TDDL` with version <= 5.4.12 → **DRDS 1.0**. **HARD STOP — refuse.**
     - Result contains `X-Cluster` → **2.0 Standard Edition**. **HARD STOP — refuse.**
   - Run `SHOW CREATE DATABASE db_name;` to verify AUTO mode (MODE = 'auto').

2. **Determine the table type**:
   - Small or dictionary tables frequently joined with partitioned tables → Broadcast table `BROADCAST`.
   - Small tables NOT joined with partitioned tables → Both `BROADCAST` and `SINGLE` are acceptable.
   - Otherwise → Partitioned table (default).

3. **Partition scheme design** (for partitioned tables):
   - **Collect SQL access pattern data** (prerequisite): prefer SQL Insight; when unavailable, use slow query logs + application code analysis.
   - **Partition key selection — comprehensive multi-dimensional analysis**: List all candidate fields, evaluate EVERY candidate on ALL dimensions:
     - **Equality query ratio**: proportion of SQL templates with this field as equality condition.
     - **Cardinality**: number of distinct values.
     - **Hotspot risk**: whether a few values dominate data distribution.
     - **Primary key / unique key status**: PKs/UKs have highest cardinality and zero hotspot risk.
     - **Semantic analysis**: Infer query patterns from table type and field meaning.
     The best partition key scores well across all dimensions combined. High-frequency non-partition-key queries → add GSI.
   - **GSI selection**: Based on write volume — regular GSI for few returned rows, Clustered GSI for one-to-many, UGSI for unique constraints. **GSI must include `PARTITION BY KEY(...) PARTITIONS N`**.
   - **Partition algorithm**: ~90% use HASH/KEY; multi-dimensional use CO_HASH; time-based cleanup use HASH+RANGE; multi-tenant use LIST+HASH.
   - **Partition count**: 256 by default; several times the number of DN nodes; single partition < 100M rows.
   - **Migration workflow** (three-step method): (1) Convert to 1 partition → (2) Create GSI/UGSI → (3) Change to target partition count.

4. **Generate SQL** using PolarDB-X safe defaults:
   - Avoid unsupported MySQL features.
   - Use `KEY`/`HASH` partitioning instead of AUTO_INCREMENT hotspot.
   - Add GSI for non-partition-key queries.

## Key Differences Quick Reference

- **Three table types**: Single (`SINGLE`), Broadcast (`BROADCAST`), Partitioned (default).
- **Partitioned tables**: KEY/HASH/RANGE/LIST/RANGE COLUMNS/LIST COLUMNS/CO_HASH + secondary partitions (49 combinations).
- **Primary keys and unique keys**: Global (globally unique) vs Local (unique within partition). Prefer choosing partition keys FROM existing PK/UK columns to naturally guarantee global uniqueness — do NOT modify user's existing PK definition.
- **GSI syntax**: Must specify its own PARTITION BY clause — it is an independently partitioned table:
  ```sql
  -- ✅ Correct
  GLOBAL INDEX g_i_seller(seller_id) PARTITION BY KEY(seller_id) PARTITIONS 16
  CLUSTERED INDEX cg_i_buyer(buyer_id) PARTITION BY KEY(buyer_id) PARTITIONS 16
  -- ❌ Wrong: Missing PARTITION BY
  GLOBAL INDEX gsi_seller(seller_id)
  ```
- **Table groups**: Same partition rules bound to same table group → JOIN pushdown, avoid cross-shard shuffle.

## Best Practices

1. **Choose the right table type**: Broadcast for small/dictionary tables joined with partitioned tables. BROADCAST or SINGLE for small tables not joined. Partitioned for everything else.
2. **Multi-dimensional partition key analysis**: Always collect SQL patterns first. Evaluate ALL dimensions (equality ratio, cardinality, hotspot, PK/UK status, semantics). Never decide on a single dimension alone.
3. **Prefer partition keys from PK/UK columns**: Naturally guarantees Global uniqueness without schema changes. Do NOT modify user's PK to add partition columns.
4. **Create GSIs wisely**: Regular GSI for few rows, Clustered GSI for one-to-many, UGSI for unique constraints. Every GSI must have `PARTITION BY KEY(...) PARTITIONS N`.
5. **Use 256 partitions**: Suits vast majority of workloads, several times DN node count.
6. **Three-step migration**: Single→1 partition (preserving uniqueness)→Create GSI/UGSI→Target partition count.
7. **Don't force partition key hits for low-ratio SQL**: Low-QPS cross-shard queries have limited total cost.
8. **Use table groups to optimize JOINs**: Bind frequently joined tables to same table group with same partition rules.

## Reference Links

| Reference | Description |
|-----------|-------------|
| [references/partition-design-best-practice.md](references/partition-design-best-practice.md) | Partition design best practices: partition key/GSI/algorithm/count selection, three-step migration, complete examples |
| [references/create-table.md](references/create-table.md) | CREATE TABLE syntax, table types (single/broadcast/partitioned), partition strategies, secondary partitions |
| [references/gsi.md](references/gsi.md) | Global Secondary Index GSI/UGSI/Clustered GSI creation, querying, and limitations |
| [references/primary-key-unique-key.md](references/primary-key-unique-key.md) | Primary key and unique key Global/Local classification, rules, risks, and recommendations |
