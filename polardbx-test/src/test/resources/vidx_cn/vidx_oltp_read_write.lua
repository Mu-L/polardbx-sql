#!/usr/bin/env sysbench
-- Copyright (C) 2006-2017 Alexey Kopytov <akopytov@gmail.com>

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

-- ----------------------------------------------------------------------
-- Read/Write OLTP benchmark with Vector Index operations
--
-- Workload per transaction:
--   - Standard OLTP reads: point selects, range selects
--   - Standard OLTP writes: index updates, non-index updates
--   - Vector reads: ANN search (VEC_DISTANCE + ORDER BY + LIMIT)
--   - Vector writes: embedding updates, delete + insert with vector data
-- ----------------------------------------------------------------------

-- Add script directory to module search path so vidx_oltp_common.lua
-- can be found regardless of the current working directory.
local script_dir = debug.getinfo(1, "S").source:match("^@(.*/)")
if script_dir then
   package.path = script_dir .. "?.lua;" .. package.path
end

require("vidx_oltp_common")

function prepare_statements()
   if not sysbench.opt.skip_trx then
      prepare_begin()
      prepare_commit()
   end

   prepare_point_selects()
   prepare_vector_ann_selects()

   if sysbench.opt.range_selects then
      prepare_simple_ranges()
      prepare_sum_ranges()
      prepare_order_ranges()
      prepare_distinct_ranges()
   end

   prepare_index_updates()
   prepare_non_index_updates()
   prepare_vector_updates()
   prepare_vector_delete_inserts()
end

function event()
   if not sysbench.opt.skip_trx then
      begin()
   end

   execute_point_selects()

   if sysbench.opt.range_selects then
      execute_simple_ranges()
      execute_sum_ranges()
      execute_order_ranges()
      execute_distinct_ranges()
   end

   execute_vector_ann_selects()

   execute_index_updates()
   execute_non_index_updates()
   execute_vector_updates()
   execute_vector_delete_inserts()

   if not sysbench.opt.skip_trx then
      commit()
   end
end
