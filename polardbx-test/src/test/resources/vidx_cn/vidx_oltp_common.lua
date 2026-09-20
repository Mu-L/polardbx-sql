-- Copyright (C) 2006-2018 Alexey Kopytov <akopytov@gmail.com>

-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

-- -----------------------------------------------------------------------------
-- Common code for Vector Index OLTP benchmarks (PolarDB-X CN Edition).
-- Based on DN sysbench_vidx/vidx_oltp_common.lua with distributed table support.
--
-- Table structure: standard sbtest columns + VECTOR(N) embedding column
-- Vector index: HNSW with configurable M and DISTANCE parameters
-- Data generation: normalized random vectors (inspired by VectorDBBench)
-- Query: ANN search via VEC_DISTANCE + ORDER BY + LIMIT
--
-- CN-specific extensions:
--   - partition_type: KEY, HASH, or SINGLE (distributed table partitioning)
--   - partitions: number of partitions for distributed tables
-- -----------------------------------------------------------------------------

function init()
   assert(event ~= nil,
          "this script is meant to be included by other OLTP scripts and " ..
             "should not be called directly.")
end

if sysbench.cmdline.command == nil then
   error("Command is required. Supported commands: prepare, prewarm, run, " ..
            "cleanup, help")
end

-- Command line options (standard OLTP + vector extensions)
sysbench.cmdline.options = {
   table_size =
      {"Number of rows per table", 10000},
   range_size =
      {"Range size for range SELECT queries", 100},
   tables =
      {"Number of tables", 1},
   point_selects =
      {"Number of point SELECT queries per transaction", 10},
   simple_ranges =
      {"Number of simple range SELECT queries per transaction", 1},
   sum_ranges =
      {"Number of SELECT SUM() queries per transaction", 1},
   order_ranges =
      {"Number of SELECT ORDER BY queries per transaction", 1},
   distinct_ranges =
      {"Number of SELECT DISTINCT queries per transaction", 1},
   index_updates =
      {"Number of UPDATE index queries per transaction", 1},
   non_index_updates =
      {"Number of UPDATE non-index queries per transaction", 1},
   delete_inserts =
      {"Number of DELETE/INSERT combinations per transaction", 1},
   range_selects =
      {"Enable/disable all range SELECT queries", true},
   auto_inc =
   {"Use AUTO_INCREMENT column as Primary Key (for MySQL), " ..
       "or its alternatives in other DBMS. When disabled, use " ..
       "client-generated IDs", true},
   skip_trx =
      {"Don't start explicit transactions and execute all queries " ..
          "in the AUTOCOMMIT mode", false},
   secondary =
      {"Use a secondary index in place of the PRIMARY KEY", false},
   create_secondary =
      {"Create a secondary index in addition to the PRIMARY KEY", true},
   mysql_storage_engine =
      {"Storage engine, if MySQL is used", "innodb"},
   pgsql_variant =
      {"Use this PostgreSQL variant when running with the " ..
          "PostgreSQL driver. The only currently supported " ..
          "variant is 'redshift'. When enabled, " ..
          "create_secondary is automatically disabled, and " ..
          "delete_inserts is set to 0"},

   -- Vector index specific options
   vector_dim =
      {"Vector dimension for VECTOR column", 768},
   vector_m =
      {"HNSW M parameter (max neighbors per node)", 6},
   vector_distance =
      {"Distance metric for vector index (EUCLIDEAN or COSINE)", "EUCLIDEAN"},
   vector_ef_search =
      {"HNSW ef_search parameter for ANN queries", 20},
   vector_ann_selects =
      {"Number of ANN SELECT queries per transaction", 1},
   vector_ann_limit =
      {"Top-K results for ANN queries", 10},
   vector_updates =
      {"Number of vector UPDATE queries per transaction", 1},
   vector_pool_size =
      {"Pre-generated vector pool size for queries", 100},

   -- PolarDB-X CN distributed table options
   partition_type =
      {"Partition type for distributed table: KEY, HASH, or SINGLE", "KEY"},
   partitions =
      {"Number of partitions for distributed table", 16},
}

-- ---------------------------------------------------------------------------
-- Vector data generation (inspired by VectorDBBench random vector approach)
-- ---------------------------------------------------------------------------

-- Generate a normalized random vector string of the form "[v1,v2,...,vN]"
-- Each dimension is a random float in [-1, 1], then L2-normalized.
function generate_random_vector_str(dim)
   local parts = {}
   local norm = 0
   for i = 1, dim do
      local v = math.random() * 2 - 1
      parts[i] = v
      norm = norm + v * v
   end
   norm = math.sqrt(norm)
   local strs = {}
   for i = 1, dim do
      strs[i] = string.format("%.6f", parts[i] / norm)
   end
   return "[" .. table.concat(strs, ",") .. "]"
end

-- Get a vector from the pre-generated pool (cyclic access)
function get_pool_vector()
   vector_pool_idx = (vector_pool_idx % sysbench.opt.vector_pool_size) + 1
   return vector_pool[vector_pool_idx]
end

-- ---------------------------------------------------------------------------
-- Prepare / Prewarm commands
-- ---------------------------------------------------------------------------

-- Prepare the dataset. This command supports parallel execution, i.e. will
-- benefit from executing with --threads > 1 as long as --tables > 1
function cmd_prepare()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables,
   sysbench.opt.threads do
     create_table(drv, con, i)
   end
end

-- Preload the dataset into the server cache. This command supports parallel
-- execution, i.e. will benefit from executing with --threads > 1 as long as
-- --tables > 1
function cmd_prewarm()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   assert(drv:name() == "mysql", "prewarm is currently MySQL only")

   con:query("SET tmp_table_size=2*1024*1024*1024")
   con:query("SET max_heap_table_size=2*1024*1024*1024")

   for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables,
   sysbench.opt.threads do
      local t = "sbtest" .. i
      print("Prewarming table " .. t)
      con:query("ANALYZE TABLE sbtest" .. i)
      con:query(string.format(
                   "SELECT AVG(id) FROM " ..
                      "(SELECT * FROM %s FORCE KEY (PRIMARY) " ..
                      "LIMIT %u) t",
                   t, sysbench.opt.table_size))
      con:query(string.format(
                   "SELECT COUNT(*) FROM " ..
                      "(SELECT * FROM %s WHERE k LIKE '%%0%%' LIMIT %u) t",
                   t, sysbench.opt.table_size))
   end
end

-- Implement parallel prepare and prewarm commands
sysbench.cmdline.commands = {
   prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND},
   prewarm = {cmd_prewarm, sysbench.cmdline.PARALLEL_COMMAND}
}

-- ---------------------------------------------------------------------------
-- Value templates
-- ---------------------------------------------------------------------------

-- Template strings of random digits with 11-digit groups separated by dashes

-- 10 groups, 119 characters
local c_value_template = "###########-###########-###########-" ..
   "###########-###########-###########-" ..
   "###########-###########-###########-" ..
   "###########"

-- 5 groups, 59 characters
local pad_value_template = "###########-###########-###########-" ..
   "###########-###########"

function get_c_value()
   return sysbench.rand.string(c_value_template)
end

function get_pad_value()
   return sysbench.rand.string(pad_value_template)
end

-- ---------------------------------------------------------------------------
-- Table creation with VECTOR column and HNSW index
-- ---------------------------------------------------------------------------

function create_table(drv, con, table_num)
   local id_index_def, id_def
   local engine_def = ""
   local extra_table_options = ""
   local query

   if sysbench.opt.secondary then
     id_index_def = "KEY xid"
   else
     id_index_def = "PRIMARY KEY"
   end

   if drv:name() == "mysql" or drv:name() == "attachsql" or
      drv:name() == "drizzle"
   then
      if sysbench.opt.auto_inc then
         id_def = "INTEGER NOT NULL AUTO_INCREMENT"
      else
         id_def = "INTEGER NOT NULL"
      end
      engine_def = "/*! ENGINE = " .. sysbench.opt.mysql_storage_engine .. " */"
      extra_table_options = mysql_table_options or ""
   elseif drv:name() == "pgsql"
   then
      if not sysbench.opt.auto_inc then
         id_def = "INTEGER NOT NULL"
      elseif pgsql_variant == 'redshift' then
        id_def = "INTEGER IDENTITY(1,1)"
      else
        id_def = "SERIAL"
      end
   else
      error("Unsupported database driver:" .. drv:name())
   end

   -- Build partition clause for PolarDB-X distributed tables
   local partition_clause = ""
   if sysbench.opt.partition_type == "KEY" then
      partition_clause = string.format(" PARTITION BY KEY(id) PARTITIONS %d",
                                        sysbench.opt.partitions)
   elseif sysbench.opt.partition_type == "HASH" then
      partition_clause = string.format(" PARTITION BY HASH(id) PARTITIONS %d",
                                        sysbench.opt.partitions)
   end
   -- partition_type == "SINGLE": no partition clause (single table)

   print(string.format("Creating table 'sbtest%d' with VECTOR(%d) column (partition: %s, count: %d)...",
                        table_num, sysbench.opt.vector_dim,
                        sysbench.opt.partition_type, sysbench.opt.partitions))

   -- Create table with VECTOR column and optional partition clause
   query = string.format([[
CREATE TABLE sbtest%d(
  id %s,
  k INTEGER DEFAULT '0' NOT NULL,
  c CHAR(120) DEFAULT '' NOT NULL,
  pad CHAR(60) DEFAULT '' NOT NULL,
  embedding VECTOR(%d),
  %s (id)
) %s %s%s]],
      table_num, id_def, sysbench.opt.vector_dim,
      id_index_def, engine_def, extra_table_options, partition_clause)

   con:query(query)

   if (sysbench.opt.table_size > 0) then
      print(string.format("Inserting %d records into 'sbtest%d'",
                          sysbench.opt.table_size, table_num))
   end

   -- Bulk insert with vector data
   if sysbench.opt.auto_inc then
      query = "INSERT INTO sbtest" .. table_num .. "(k, c, pad, embedding) VALUES"
   else
      query = "INSERT INTO sbtest" .. table_num .. "(id, k, c, pad, embedding) VALUES"
   end

   con:bulk_insert_init(query)

   local c_val
   local pad_val
   local vec_str

   for i = 1, sysbench.opt.table_size do

      c_val = get_c_value()
      pad_val = get_pad_value()
      vec_str = generate_random_vector_str(sysbench.opt.vector_dim)

      if (sysbench.opt.auto_inc) then
         query = string.format("(%d, '%s', '%s', VEC_FROMTEXT('%s'))",
                               sb_rand(1, sysbench.opt.table_size), c_val,
                               pad_val, vec_str)
      else
         query = string.format("(%d, %d, '%s', '%s', VEC_FROMTEXT('%s'))",
                               i, sb_rand(1, sysbench.opt.table_size), c_val,
                               pad_val, vec_str)
      end

      con:bulk_insert_next(query)
   end

   con:bulk_insert_done()

   -- Create secondary index on k column
   if sysbench.opt.create_secondary then
      print(string.format("Creating a secondary index on 'sbtest%d'...",
                          table_num))
      con:query(string.format("CREATE INDEX k_%d ON sbtest%d(k)",
                              table_num, table_num))
   end

   -- Create HNSW vector index (after data loading for better performance)
   print(string.format(
      "Creating HNSW vector index on 'sbtest%d' (M=%d, DISTANCE=%s)...",
      table_num, sysbench.opt.vector_m, sysbench.opt.vector_distance))
   con:query(string.format(
      "ALTER TABLE sbtest%d ADD VECTOR INDEX vi_%d (embedding) M=%d DISTANCE=%s",
      table_num, table_num, sysbench.opt.vector_m, sysbench.opt.vector_distance))
end

-- ---------------------------------------------------------------------------
-- Prepared statement definitions (standard OLTP queries)
-- ---------------------------------------------------------------------------

local t = sysbench.sql.type
local stmt_defs = {
   point_selects = {
      "SELECT c FROM sbtest%u WHERE id=?",
      t.INT},
   simple_ranges = {
      "SELECT c FROM sbtest%u WHERE id BETWEEN ? AND ?",
      t.INT, t.INT},
   sum_ranges = {
      "SELECT SUM(k) FROM sbtest%u WHERE id BETWEEN ? AND ?",
       t.INT, t.INT},
   order_ranges = {
      "SELECT c FROM sbtest%u WHERE id BETWEEN ? AND ? ORDER BY c",
       t.INT, t.INT},
   distinct_ranges = {
      "SELECT DISTINCT c FROM sbtest%u WHERE id BETWEEN ? AND ? ORDER BY c",
      t.INT, t.INT},
   index_updates = {
      "UPDATE sbtest%u SET k=k+1 WHERE id=?",
      t.INT},
   non_index_updates = {
      "UPDATE sbtest%u SET c=? WHERE id=?",
      {t.CHAR, 120}, t.INT},
   deletes = {
      "DELETE FROM sbtest%u WHERE id=?",
      t.INT},
}

function prepare_begin()
   stmt.begin = con:prepare("BEGIN")
end

function prepare_commit()
   stmt.commit = con:prepare("COMMIT")
end

function prepare_for_each_table(key)
   for t = 1, sysbench.opt.tables do
      stmt[t][key] = con:prepare(string.format(stmt_defs[key][1], t))

      local nparam = #stmt_defs[key] - 1

      if nparam > 0 then
         param[t][key] = {}
      end

      for p = 1, nparam do
         local btype = stmt_defs[key][p+1]
         local len

         if type(btype) == "table" then
            len = btype[2]
            btype = btype[1]
         end
         if btype == sysbench.sql.type.VARCHAR or
            btype == sysbench.sql.type.CHAR then
               param[t][key][p] = stmt[t][key]:bind_create(btype, len)
         else
            param[t][key][p] = stmt[t][key]:bind_create(btype)
         end
      end

      if nparam > 0 then
         stmt[t][key]:bind_param(unpack(param[t][key]))
      end
   end
end

function prepare_point_selects()
   prepare_for_each_table("point_selects")
end

function prepare_simple_ranges()
   prepare_for_each_table("simple_ranges")
end

function prepare_sum_ranges()
   prepare_for_each_table("sum_ranges")
end

function prepare_order_ranges()
   prepare_for_each_table("order_ranges")
end

function prepare_distinct_ranges()
   prepare_for_each_table("distinct_ranges")
end

function prepare_index_updates()
   prepare_for_each_table("index_updates")
end

function prepare_non_index_updates()
   prepare_for_each_table("non_index_updates")
end

function prepare_delete_inserts()
   prepare_for_each_table("deletes")
end

-- ---------------------------------------------------------------------------
-- Vector prepared statement support
--
-- VEC_FROMTEXT(?) CAN be used with prepared statements: the vector string
-- (e.g. "[0.1,0.2,...,0.768]") is bound as a CHAR parameter.
-- When a scenario calls these prepare functions, the corresponding execute
-- functions automatically switch to PS mode; otherwise they fall back to
-- con:query() text protocol.
-- ---------------------------------------------------------------------------

-- Max CHAR buffer size for a vector string.
-- 768 dims * ~9 chars/dim + brackets ≈ 6914, round up to 8192.
local VECTOR_STR_MAXLEN = 8192

function prepare_vector_ann_selects()
   if not vector_ps_enabled then return end
   for t = 1, sysbench.opt.tables do
      local sql = string.format(
         "SELECT id, VEC_DISTANCE(embedding, VEC_FROMTEXT(?)) AS distance " ..
         "FROM sbtest%d force index(vi_%d) ORDER BY distance LIMIT %d",
         t, t, sysbench.opt.vector_ann_limit)
      stmt[t].vector_ann_selects = con:prepare(sql)
      param[t].vector_ann_selects = {}
      param[t].vector_ann_selects[1] = stmt[t].vector_ann_selects:bind_create(
         sysbench.sql.type.CHAR, VECTOR_STR_MAXLEN)
      stmt[t].vector_ann_selects:bind_param(param[t].vector_ann_selects[1])
   end
end

function prepare_vector_noindex_selects()
   if not vector_ps_enabled then return end
   for t = 1, sysbench.opt.tables do
      local sql = string.format(
         "SELECT id, VECTOR_DIM(embedding) AS distance0, " ..
         "VECTOR_DIM(VEC_FROMTEXT(?)) AS distance " ..
         "FROM sbtest%d ORDER BY distance LIMIT %d",
         t, sysbench.opt.vector_ann_limit)
      stmt[t].vector_noindex_selects = con:prepare(sql)
      param[t].vector_noindex_selects = {}
      param[t].vector_noindex_selects[1] =
         stmt[t].vector_noindex_selects:bind_create(
            sysbench.sql.type.CHAR, VECTOR_STR_MAXLEN)
      stmt[t].vector_noindex_selects:bind_param(
         param[t].vector_noindex_selects[1])
   end
end

function prepare_vector_updates()
   if not vector_ps_enabled then return end
   for t = 1, sysbench.opt.tables do
      local sql = string.format(
         "UPDATE sbtest%d SET embedding = VEC_FROMTEXT(?) WHERE id = ?", t)
      stmt[t].vector_updates = con:prepare(sql)
      param[t].vector_updates = {}
      param[t].vector_updates[1] = stmt[t].vector_updates:bind_create(
         sysbench.sql.type.CHAR, VECTOR_STR_MAXLEN)
      param[t].vector_updates[2] = stmt[t].vector_updates:bind_create(
         sysbench.sql.type.INT)
      stmt[t].vector_updates:bind_param(
         param[t].vector_updates[1], param[t].vector_updates[2])
   end
end

function prepare_vector_inserts()
   if not vector_ps_enabled then return end
   for t = 1, sysbench.opt.tables do
      local sql = string.format(
         "INSERT INTO sbtest%d (k, c, pad, embedding) VALUES " ..
         "(?, ?, ?, VEC_FROMTEXT(?))", t)
      stmt[t].vector_inserts = con:prepare(sql)
      param[t].vector_inserts = {}
      param[t].vector_inserts[1] = stmt[t].vector_inserts:bind_create(
         sysbench.sql.type.INT)
      param[t].vector_inserts[2] = stmt[t].vector_inserts:bind_create(
         sysbench.sql.type.CHAR, 120)
      param[t].vector_inserts[3] = stmt[t].vector_inserts:bind_create(
         sysbench.sql.type.CHAR, 60)
      param[t].vector_inserts[4] = stmt[t].vector_inserts:bind_create(
         sysbench.sql.type.CHAR, VECTOR_STR_MAXLEN)
      stmt[t].vector_inserts:bind_param(
         param[t].vector_inserts[1], param[t].vector_inserts[2],
         param[t].vector_inserts[3], param[t].vector_inserts[4])
   end
end

function prepare_vector_delete_inserts()
   prepare_for_each_table("deletes")
   if not vector_ps_enabled then return end
   for t = 1, sysbench.opt.tables do
      local sql = string.format(
         "INSERT INTO sbtest%d (id, k, c, pad, embedding) VALUES " ..
         "(?, ?, ?, ?, VEC_FROMTEXT(?))", t)
      stmt[t].vector_reinserts = con:prepare(sql)
      param[t].vector_reinserts = {}
      param[t].vector_reinserts[1] = stmt[t].vector_reinserts:bind_create(
         sysbench.sql.type.INT)
      param[t].vector_reinserts[2] = stmt[t].vector_reinserts:bind_create(
         sysbench.sql.type.INT)
      param[t].vector_reinserts[3] = stmt[t].vector_reinserts:bind_create(
         sysbench.sql.type.CHAR, 120)
      param[t].vector_reinserts[4] = stmt[t].vector_reinserts:bind_create(
         sysbench.sql.type.CHAR, 60)
      param[t].vector_reinserts[5] = stmt[t].vector_reinserts:bind_create(
         sysbench.sql.type.CHAR, VECTOR_STR_MAXLEN)
      stmt[t].vector_reinserts:bind_param(
         param[t].vector_reinserts[1], param[t].vector_reinserts[2],
         param[t].vector_reinserts[3], param[t].vector_reinserts[4],
         param[t].vector_reinserts[5])
   end
end

-- ---------------------------------------------------------------------------
-- Thread lifecycle
-- ---------------------------------------------------------------------------

function thread_init()
   drv = sysbench.sql.driver()
   con = drv:connect()

   -- Auto-detect: when --db-ps-mode=disable, sysbench PS emulation may
   -- truncate large CHAR parameters (vector strings ~7000 chars), so we
   -- fall back to con:query() text protocol for vector operations.
   local ok, mode = pcall(function() return sysbench.opt.db_ps_mode end)
   vector_ps_enabled = not (ok and mode == "disable")

   -- Set session variables for vector index
   con:query("SET SESSION transaction_isolation = 'READ-COMMITTED'")
   con:query(string.format("SET SESSION vidx_hnsw_ef_search = %d",
             sysbench.opt.vector_ef_search))

   -- Pre-generate vector pool to avoid per-query random vector generation
   -- overhead. Each thread maintains its own independent pool.
   vector_pool = {}
   for i = 1, sysbench.opt.vector_pool_size do
      vector_pool[i] = generate_random_vector_str(sysbench.opt.vector_dim)
   end
   vector_pool_idx = 0

   -- Create global nested tables for prepared statements and their
   -- parameters. We need a statement and a parameter set for each combination
   -- of connection/table/query
   stmt = {}
   param = {}

   for t = 1, sysbench.opt.tables do
      stmt[t] = {}
      param[t] = {}
   end

   -- This function is a 'callback' defined by individual benchmark scripts
   prepare_statements()
end

-- Close prepared statements
function close_statements()
   for t = 1, sysbench.opt.tables do
      for k, s in pairs(stmt[t]) do
         stmt[t][k]:close()
      end
   end
   if (stmt.begin ~= nil) then
      stmt.begin:close()
   end
   if (stmt.commit ~= nil) then
      stmt.commit:close()
   end
end

function thread_done()
   close_statements()
   con:disconnect()
end

function cleanup()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   for i = 1, sysbench.opt.tables do
      print(string.format("Dropping table 'sbtest%d'...", i))
      con:query("DROP TABLE IF EXISTS sbtest" .. i )
   end
end

-- ---------------------------------------------------------------------------
-- Helper functions
-- ---------------------------------------------------------------------------

function get_table_num()
   return sysbench.rand.uniform(1, sysbench.opt.tables)
end

function get_id()
   return sysbench.rand.default(1, sysbench.opt.table_size)
end

-- ---------------------------------------------------------------------------
-- Standard OLTP execute functions (using prepared statements)
-- ---------------------------------------------------------------------------

function begin()
   stmt.begin:execute()
end

function commit()
   stmt.commit:execute()
end

function execute_point_selects()
   local tnum = get_table_num()
   local i

   for i = 1, sysbench.opt.point_selects do
      param[tnum].point_selects[1]:set(get_id())

      stmt[tnum].point_selects:execute()
   end
end

local function execute_range(key)
   local tnum = get_table_num()

   for i = 1, sysbench.opt[key] do
      local id = get_id()

      param[tnum][key][1]:set(id)
      param[tnum][key][2]:set(id + sysbench.opt.range_size - 1)

      stmt[tnum][key]:execute()
   end
end

function execute_simple_ranges()
   execute_range("simple_ranges")
end

function execute_sum_ranges()
   execute_range("sum_ranges")
end

function execute_order_ranges()
   execute_range("order_ranges")
end

function execute_distinct_ranges()
   execute_range("distinct_ranges")
end

function execute_index_updates()
   local tnum = get_table_num()

   for i = 1, sysbench.opt.index_updates do
      param[tnum].index_updates[1]:set(get_id())

      stmt[tnum].index_updates:execute()
   end
end

function execute_non_index_updates()
   local tnum = get_table_num()

   for i = 1, sysbench.opt.non_index_updates do
      param[tnum].non_index_updates[1]:set_rand_str(c_value_template)
      param[tnum].non_index_updates[2]:set(get_id())

      stmt[tnum].non_index_updates:execute()
   end
end

-- ---------------------------------------------------------------------------
-- Vector-specific execute functions
--
-- Each function checks whether its prepared statement was set up (by the
-- corresponding prepare_vector_*() call in the scenario script).
-- If yes, it uses the PS path; otherwise it falls back to con:query().
-- ---------------------------------------------------------------------------

-- ANN search: find top-K nearest neighbors using HNSW vector index
function execute_vector_ann_selects()
   local tnum = get_table_num()

   for i = 1, sysbench.opt.vector_ann_selects do
      local vec_str = get_pool_vector()
      if stmt[tnum].vector_ann_selects then
         param[tnum].vector_ann_selects[1]:set(vec_str)
         stmt[tnum].vector_ann_selects:execute()
      else
         con:query(string.format(
            "SELECT id, VEC_DISTANCE(embedding, VEC_FROMTEXT('%s')) AS distance " ..
            "FROM sbtest%d force index(vi_%d) ORDER BY distance LIMIT %d",
            vec_str, tnum, tnum, sysbench.opt.vector_ann_limit))
      end
   end
end

-- No-index baseline: same ORDER BY LIMIT pattern but without vector index
function execute_vector_noindex_selects()
   local tnum = get_table_num()

   for i = 1, sysbench.opt.vector_ann_selects do
      local vec_str = get_pool_vector()
      if stmt[tnum].vector_noindex_selects then
         param[tnum].vector_noindex_selects[1]:set(vec_str)
         stmt[tnum].vector_noindex_selects:execute()
      else
         con:query(string.format(
            "SELECT id, VECTOR_DIM(embedding) AS distance0, " ..
            "VECTOR_DIM(VEC_FROMTEXT('%s')) AS distance " ..
            "FROM sbtest%d ORDER BY distance LIMIT %d",
            vec_str, tnum, sysbench.opt.vector_ann_limit))
      end
   end
end

-- Vector column update: modify embedding for a random row
function execute_vector_updates()
   local tnum = get_table_num()

   for i = 1, sysbench.opt.vector_updates do
      local id = get_id()
      local vec_str = get_pool_vector()
      if stmt[tnum].vector_updates then
         param[tnum].vector_updates[1]:set(vec_str)
         param[tnum].vector_updates[2]:set(id)
         stmt[tnum].vector_updates:execute()
      else
         con:query(string.format(
            "UPDATE sbtest%d SET embedding = VEC_FROMTEXT('%s') WHERE id = %d",
            tnum, vec_str, id))
      end
   end
end

-- Single-row INSERT with vector data
function execute_vector_insert()
   local tnum = get_table_num()
   local k = sysbench.rand.default(1, sysbench.opt.table_size)
   local c_val = get_c_value()
   local pad_val = get_pad_value()
   local vec_str = get_pool_vector()

   if stmt[tnum].vector_inserts then
      param[tnum].vector_inserts[1]:set(k)
      param[tnum].vector_inserts[2]:set(c_val)
      param[tnum].vector_inserts[3]:set(pad_val)
      param[tnum].vector_inserts[4]:set(vec_str)
      stmt[tnum].vector_inserts:execute()
   else
      con:query(string.format(
         "INSERT INTO sbtest%d (k, c, pad, embedding) VALUES " ..
         "(%d, '%s', '%s', VEC_FROMTEXT('%s'))",
         tnum, k, c_val, pad_val, vec_str))
   end
end

-- Delete + Insert with vector data
function execute_vector_delete_inserts()
   local tnum = get_table_num()

   for i = 1, sysbench.opt.delete_inserts do
      local id = get_id()
      local k = get_id()
      local c_val = get_c_value()
      local pad_val = get_pad_value()
      local vec_str = get_pool_vector()

      if stmt[tnum].vector_reinserts then
         -- PS mode: use prepared statements for both delete and re-insert
         param[tnum].deletes[1]:set(id)
         stmt[tnum].deletes:execute()
         param[tnum].vector_reinserts[1]:set(id)
         param[tnum].vector_reinserts[2]:set(k)
         param[tnum].vector_reinserts[3]:set(c_val)
         param[tnum].vector_reinserts[4]:set(pad_val)
         param[tnum].vector_reinserts[5]:set(vec_str)
         stmt[tnum].vector_reinserts:execute()
      else
         con:query(string.format(
            "DELETE FROM sbtest%d WHERE id = %d", tnum, id))
         con:query(string.format(
            "INSERT INTO sbtest%d (id, k, c, pad, embedding) VALUES " ..
            "(%d, %d, '%s', '%s', VEC_FROMTEXT('%s'))",
            tnum, id, k, c_val, pad_val, vec_str))
      end
   end
end

-- ---------------------------------------------------------------------------
-- Reconnect hook
-- ---------------------------------------------------------------------------

-- Re-prepare statements if we have reconnected, which is possible when some of
-- the listed error codes are in the --mysql-ignore-errors list
function sysbench.hooks.before_restart_event(errdesc)
   if errdesc.sql_errno == 2013 or -- CR_SERVER_LOST
      errdesc.sql_errno == 2055 or -- CR_SERVER_LOST_EXTENDED
      errdesc.sql_errno == 2006 or -- CR_SERVER_GONE_ERROR
      errdesc.sql_errno == 2011    -- CR_TCP_CONNECTION
   then
      close_statements()
      prepare_statements()
   end
end
