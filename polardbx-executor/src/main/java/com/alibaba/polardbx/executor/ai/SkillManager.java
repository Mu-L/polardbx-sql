/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ai;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.model.SkillConfigAccessor;
import com.alibaba.polardbx.gms.metadb.model.SkillConfigRecord;
import com.alibaba.polardbx.gms.metadb.model.SkillReferenceAccessor;
import com.alibaba.polardbx.gms.metadb.model.SkillReferenceRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AI Skill Manager - manages skill registration, update, deletion and querying.
 * Singleton with local cache for skill configurations and references.
 */
public class SkillManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(SkillManager.class);

    private static final SkillManager INSTANCE = new SkillManager();

    public static final String BUILTIN_PREFIX = "__BUILTIN_"; // legacy, kept for LEGACY_SKILLS_TO_REMOVE matching
    private static final String STATUS_ACTIVE = "ACTIVE";
    private static final String STATUS_INACTIVE = "INACTIVE";

    // Built-in skill: skill usage guide
    private static final String TEST_SKILL_NAME = "skill-guide";
    private static final String TEST_SKILL_DESC = "Skill 使用指南 - 教用户管理 skill";
    private static final String TEST_SKILL_PROMPT =
        "当用户询问如何使用 skill、如何管理 skill、或说\"帮助\"\"help\"时，告知以下指令用法:\n\n"
            + "skill 管理指令:\n"
            + "- SELECT AI_LIST_SKILLS(); -- 列出所有已注册的 skill\n"
            + "- SELECT AI_DESCRIBE_SKILL('skill_name'); -- 查看 skill 详情及其关联的 reference\n"
            + "- SELECT AI_REGISTER_SKILL('name', 'prompt'); -- 注册新 skill，prompt 是该技能的提示词\n"
            + "- SELECT AI_UPDATE_SKILL('name', '{\"prompt\":\"新提示词\",\"status\":\"ACTIVE\"}'); -- 更新 skill 属性\n"
            + "- SELECT AI_DROP_SKILL('name'); -- 删除 skill（内置 skill 不可删除）\n\n"
            + "reference 管理指令:\n"
            + "- SELECT AI_ADD_SKILL_REFERENCE('skill_name', 'ref_name', '参考内容'); -- 为 skill 添加参考文档\n"
            + "- SELECT AI_REMOVE_SKILL_REFERENCE('skill_name', 'ref_name'); -- 移除参考文档\n\n"
            + "说明:\n"
            + "- 所有状态为 ACTIVE 的 skill 及其 reference 会自动注入到对话的系统提示词中\n"
            + "- skill 名称唯一，builtin 标记为 true 的为系统内置，不可删除\n"
            + "- reference 用于为 skill 提供额外的领域知识或参考文档";

    // Data-driven file-backed builtin skills
    private static final class FileBackedBuiltinSpec {
        final String registeredName;
        final String resourceBase;

        FileBackedBuiltinSpec(String registeredName, String resourceBase) {
            this.registeredName = registeredName;
            this.resourceBase = resourceBase;
        }
    }

    private static final FileBackedBuiltinSpec[] FILE_BACKED_BUILTINS = {
        new FileBackedBuiltinSpec("partition-design", "ai/skills/polardbx-partition-design/"),
        new FileBackedBuiltinSpec("sql-compat", "ai/skills/polardbx-sql-compat/"),
        new FileBackedBuiltinSpec("online-ddl", "ai/skills/polardbx-online-ddl/"),
        new FileBackedBuiltinSpec("pagination", "ai/skills/polardbx-pagination/"),
        new FileBackedBuiltinSpec("plan-analysis", "ai/skills/polardbx-plan-analysis/"),
        new FileBackedBuiltinSpec("ttl20", "ai/skills/polardbx-ttl20/"),
        new FileBackedBuiltinSpec("ops-diagnostics", "ai/skills/polardbx-ops-diagnostics/"),
        new FileBackedBuiltinSpec("ai-functions", "ai/skills/polardbx-ai-functions/"),
        new FileBackedBuiltinSpec("vector-search", "ai/skills/polardbx-vector-search/")
    };

    private static final String[] LEGACY_SKILLS_TO_REMOVE = {
        "__BUILTIN_TEST_SKILL", "__BUILTIN_SQL_ASSISTANT",
        "__BUILTIN_SLS_QUERY", "__BUILTIN_SQL_SKILL",
        "__BUILTIN_SKILL_GUIDE",
        "__BUILTIN_POLARDBX_PARTITION_DESIGN", "__BUILTIN_POLARDBX_SQL_COMPAT",
        "__BUILTIN_POLARDBX_ONLINE_DDL", "__BUILTIN_POLARDBX_PAGINATION",
        "__BUILTIN_POLARDBX_PLAN_ANALYSIS", "__BUILTIN_POLARDBX_TTL20"
    };

    /**
     * skill name → SkillConfigRecord
     */
    private volatile Map<String, SkillConfigRecord> skillCache = new ConcurrentHashMap<>();

    /**
     * skill name → List<SkillReferenceRecord> (sorted by priority)
     */
    private volatile Map<String, List<SkillReferenceRecord>> refCache = new ConcurrentHashMap<>();

    private SkillManager() {
    }

    public static SkillManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        ensureBuiltinSkills();
        reloadFromMetaDb();
        setupConfigListener();
    }

    // ==================== Read Operations (cache only) ====================

    /**
     * Get all active skills sorted by priority.
     */
    public List<SkillConfigRecord> getActiveSkills() {
        List<SkillConfigRecord> active = new ArrayList<>();
        for (SkillConfigRecord record : skillCache.values()) {
            if (STATUS_ACTIVE.equals(record.status)) {
                active.add(record);
            }
        }
        active.sort((a, b) -> {
            int cmp = Integer.compare(a.priority, b.priority);
            return cmp != 0 ? cmp : Long.compare(a.id, b.id);
        });
        return active;
    }

    /**
     * Get references for a skill from cache.
     */
    public List<SkillReferenceRecord> getReferences(String skillName) {
        List<SkillReferenceRecord> refs = refCache.get(skillName);
        return refs != null ? refs : Collections.emptyList();
    }

    /**
     * Get the prompt of a skill by name. Returns null if skill not found or not active.
     * Used by AI_GET_SKILL_PROMPT for progressive loading.
     */
    public SkillConfigRecord getSkillConfig(String skillName) {
        SkillConfigRecord record = skillCache.get(skillName);
        if (record != null && STATUS_ACTIVE.equals(record.status)) {
            return record;
        }
        return null;
    }

    /**
     * List all skills as JSON string.
     */
    public String listSkills() {
        JSONArray arr = new JSONArray();
        List<SkillConfigRecord> all = new ArrayList<>(skillCache.values());
        all.sort((a, b) -> {
            int cmp = Integer.compare(a.priority, b.priority);
            return cmp != 0 ? cmp : Long.compare(a.id, b.id);
        });
        for (SkillConfigRecord r : all) {
            JSONObject obj = new JSONObject(true);
            obj.put("name", r.name);
            obj.put("description", r.description);
            obj.put("status", r.status);
            obj.put("priority", r.priority);
            obj.put("builtin", r.builtin == 1);
            List<SkillReferenceRecord> refs = getReferences(r.name);
            if (!refs.isEmpty()) {
                JSONArray refNames = new JSONArray();
                for (SkillReferenceRecord ref : refs) {
                    refNames.add(ref.refName);
                }
                obj.put("references", refNames);
            }
            arr.add(obj);
        }
        return arr.toJSONString();
    }

    /**
     * Describe a skill with full prompt and references.
     */
    public String describeSkill(String name) {
        SkillConfigRecord r = skillCache.get(name);
        if (r == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Skill '" + name + "' not found");
        }
        JSONObject obj = new JSONObject(true);
        obj.put("name", r.name);
        obj.put("description", r.description);
        obj.put("prompt", r.prompt);
        obj.put("status", r.status);
        obj.put("priority", r.priority);
        obj.put("builtin", r.builtin == 1);

        List<SkillReferenceRecord> refs = getReferences(r.name);
        if (!refs.isEmpty()) {
            JSONArray refArr = new JSONArray();
            for (SkillReferenceRecord ref : refs) {
                JSONObject refObj = new JSONObject(true);
                refObj.put("ref_name", ref.refName);
                refObj.put("content_length", ref.content != null ? ref.content.length() : 0);
                refObj.put("priority", ref.priority);
                refArr.add(refObj);
            }
            obj.put("references", refArr);
        }
        return obj.toJSONString();
    }

    // ==================== Write Operations (MetaDB + notify + sync) ====================

    public String registerSkill(String name, String prompt, String optionsJson) {
        if (name == null || name.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "skill name is required");
        }
        if (prompt == null || prompt.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "skill prompt is required");
        }
        // Reject if a builtin skill with this name already exists
        SkillConfigRecord existingBuiltin = skillCache.get(name.trim());
        if (existingBuiltin != null && existingBuiltin.builtin == 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Cannot overwrite built-in skill '" + name.trim() + "'");
        }

        SkillConfigRecord record = new SkillConfigRecord();
        record.name = name.trim();
        record.prompt = prompt;
        record.status = STATUS_ACTIVE;
        record.priority = 100;
        record.builtin = 0;

        if (optionsJson != null && !optionsJson.trim().isEmpty()) {
            JSONObject options = JSON.parseObject(optionsJson);
            if (options.containsKey("description")) {
                record.description = options.getString("description");
            }
            if (options.containsKey("priority")) {
                record.priority = options.getIntValue("priority");
            }
            if (options.containsKey("status")) {
                record.status = options.getString("status").toUpperCase();
            }
        }

        try (Connection conn = MetaDbUtil.getConnection()) {
            conn.setAutoCommit(false);
            try {
                SkillConfigAccessor accessor = new SkillConfigAccessor();
                accessor.setConnection(conn);

                List<SkillConfigRecord> existing = accessor.querySkillByName(record.name);
                if (!existing.isEmpty()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Skill '" + record.name + "' already exists");
                }

                accessor.insertSkill(record);
                MetaDbConfigManager.getInstance()
                    .notify(MetaDbDataIdBuilder.getAiSkillConfigDataId(), conn);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw GeneralUtil.nestedException(e);
            } finally {
                conn.setAutoCommit(true);
            }

            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getAiSkillConfigDataId());
            return "Skill '" + record.name + "' registered successfully";
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to register skill: " + name, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Failed to register skill: " + e.getMessage());
        }
    }

    public String updateSkill(String name, String optionsJson) {
        if (name == null || name.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "skill name is required");
        }
        if (optionsJson == null || optionsJson.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "options are required for update");
        }

        try (Connection conn = MetaDbUtil.getConnection()) {
            conn.setAutoCommit(false);
            try {
                SkillConfigAccessor accessor = new SkillConfigAccessor();
                accessor.setConnection(conn);

                List<SkillConfigRecord> existing = accessor.querySkillByName(name.trim());
                if (existing.isEmpty()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Skill '" + name + "' not found");
                }

                SkillConfigRecord record = existing.get(0);
                JSONObject options = JSON.parseObject(optionsJson);
                if (options.containsKey("description")) {
                    record.description = options.getString("description");
                }
                if (options.containsKey("prompt")) {
                    record.prompt = options.getString("prompt");
                }
                if (options.containsKey("status")) {
                    record.status = options.getString("status").toUpperCase();
                }
                if (options.containsKey("priority")) {
                    record.priority = options.getIntValue("priority");
                }

                accessor.updateSkill(record);
                MetaDbConfigManager.getInstance()
                    .notify(MetaDbDataIdBuilder.getAiSkillConfigDataId(), conn);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw GeneralUtil.nestedException(e);
            } finally {
                conn.setAutoCommit(true);
            }

            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getAiSkillConfigDataId());
            return "Skill '" + name + "' updated successfully";
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to update skill: " + name, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Failed to update skill: " + e.getMessage());
        }
    }

    public String dropSkill(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "skill name is required");
        }
        // Reject if skill is builtin
        SkillConfigRecord existing = skillCache.get(name.trim());
        if (existing != null && existing.builtin == 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Cannot drop built-in skill '" + name + "'. Use AI_UPDATE_SKILL to deactivate it.");
        }

        try (Connection conn = MetaDbUtil.getConnection()) {
            conn.setAutoCommit(false);
            try {
                SkillConfigAccessor skillAccessor = new SkillConfigAccessor();
                skillAccessor.setConnection(conn);
                SkillReferenceAccessor refAccessor = new SkillReferenceAccessor();
                refAccessor.setConnection(conn);

                // Delete references first
                refAccessor.deleteAllBySkillName(name.trim());
                int deleted = skillAccessor.deleteSkillByName(name.trim());
                if (deleted == 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Skill '" + name + "' not found");
                }

                MetaDbConfigManager.getInstance()
                    .notify(MetaDbDataIdBuilder.getAiSkillConfigDataId(), conn);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw GeneralUtil.nestedException(e);
            } finally {
                conn.setAutoCommit(true);
            }

            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getAiSkillConfigDataId());
            return "Skill '" + name + "' dropped successfully";
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to drop skill: " + name, e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Failed to drop skill: " + e.getMessage());
        }
    }

    public String addReference(String skillName, String refName, String content) {
        if (skillName == null || skillName.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "skill_name is required");
        }
        if (refName == null || refName.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "ref_name is required");
        }
        if (content == null || content.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "content is required");
        }

        SkillReferenceRecord record = new SkillReferenceRecord();
        record.skillName = skillName.trim();
        record.refName = refName.trim();
        record.content = content;
        record.priority = 100;

        try (Connection conn = MetaDbUtil.getConnection()) {
            conn.setAutoCommit(false);
            try {
                // Verify skill exists
                SkillConfigAccessor skillAccessor = new SkillConfigAccessor();
                skillAccessor.setConnection(conn);
                List<SkillConfigRecord> skills = skillAccessor.querySkillByName(record.skillName);
                if (skills.isEmpty()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Skill '" + record.skillName + "' not found");
                }

                SkillReferenceAccessor refAccessor = new SkillReferenceAccessor();
                refAccessor.setConnection(conn);
                refAccessor.insertReference(record);

                MetaDbConfigManager.getInstance()
                    .notify(MetaDbDataIdBuilder.getAiSkillConfigDataId(), conn);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw GeneralUtil.nestedException(e);
            } finally {
                conn.setAutoCommit(true);
            }

            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getAiSkillConfigDataId());
            return "Reference '" + record.refName + "' added to skill '" + record.skillName + "'";
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to add reference", e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Failed to add reference: " + e.getMessage());
        }
    }

    public String removeReference(String skillName, String refName) {
        if (skillName == null || skillName.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "skill_name is required");
        }
        if (refName == null || refName.trim().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "ref_name is required");
        }

        try (Connection conn = MetaDbUtil.getConnection()) {
            conn.setAutoCommit(false);
            try {
                SkillReferenceAccessor refAccessor = new SkillReferenceAccessor();
                refAccessor.setConnection(conn);
                int deleted = refAccessor.deleteReference(skillName.trim(), refName.trim());
                if (deleted == 0) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Reference '" + refName + "' not found in skill '" + skillName + "'");
                }

                MetaDbConfigManager.getInstance()
                    .notify(MetaDbDataIdBuilder.getAiSkillConfigDataId(), conn);
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                throw GeneralUtil.nestedException(e);
            } finally {
                conn.setAutoCommit(true);
            }

            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getAiSkillConfigDataId());
            return "Reference '" + refName + "' removed from skill '" + skillName + "'";
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("Failed to remove reference", e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Failed to remove reference: " + e.getMessage());
        }
    }

    // ==================== Internal ====================

    private void setupConfigListener() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            SkillConfigListener listener = new SkillConfigListener();
            MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getAiSkillConfigDataId(), conn);
            MetaDbConfigManager.getInstance().bindListener(MetaDbDataIdBuilder.getAiSkillConfigDataId(), listener);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "setup ai skill config listener failed");
        }
    }

    private void reloadFromMetaDb() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            SkillConfigAccessor skillAccessor = new SkillConfigAccessor();
            skillAccessor.setConnection(conn);
            List<SkillConfigRecord> skills = skillAccessor.queryAllSkills();

            Map<String, SkillConfigRecord> newSkillCache = new ConcurrentHashMap<>();
            for (SkillConfigRecord record : skills) {
                newSkillCache.put(record.name, record);
            }
            this.skillCache = newSkillCache;

            SkillReferenceAccessor refAccessor = new SkillReferenceAccessor();
            refAccessor.setConnection(conn);
            List<SkillReferenceRecord> allRefs = refAccessor.queryAll();

            Map<String, List<SkillReferenceRecord>> newRefCache = new ConcurrentHashMap<>();
            for (SkillReferenceRecord ref : allRefs) {
                newRefCache.computeIfAbsent(ref.skillName, k -> new ArrayList<>()).add(ref);
            }
            this.refCache = newRefCache;
        } catch (SQLException e) {
            logger.error("Failed to reload skill configs from MetaDB", e);
            throw GeneralUtil.nestedException(e);
        }
    }

    private void ensureBuiltinSkills() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            SkillConfigAccessor accessor = new SkillConfigAccessor();
            accessor.setConnection(conn);
            SkillReferenceAccessor refAccessor = new SkillReferenceAccessor();
            refAccessor.setConnection(conn);

            // 1. Remove legacy skills
            for (String legacy : LEGACY_SKILLS_TO_REMOVE) {
                removeBuiltinIfExists(accessor, refAccessor, legacy);
            }

            // 2. Inline guide skill
            ensureOneBuiltinSkill(accessor, TEST_SKILL_NAME, TEST_SKILL_DESC, TEST_SKILL_PROMPT);

            // 3. File-backed builtin skills
            for (FileBackedBuiltinSpec spec : FILE_BACKED_BUILTINS) {
                installFileBackedBuiltin(accessor, refAccessor, spec);
            }
        } catch (Exception e) {
            logger.warn("Failed to ensure built-in skills: " + e.getMessage());
        }
    }

    private void removeBuiltinIfExists(SkillConfigAccessor accessor, SkillReferenceAccessor refAccessor,
                                       String skillName) {
        List<SkillConfigRecord> existing = accessor.querySkillByName(skillName);
        if (!existing.isEmpty()) {
            refAccessor.deleteAllBySkillName(skillName);
            accessor.deleteSkillByName(skillName);
        }
    }

    private void installFileBackedBuiltin(SkillConfigAccessor accessor, SkillReferenceAccessor refAccessor,
                                          FileBackedBuiltinSpec spec) {
        // 1. Load SKILL.md and parse frontmatter
        String raw = loadClasspathResource(spec.resourceBase + "SKILL.md");
        if (raw == null) {
            logger.warn("SKILL.md not found for: " + spec.registeredName);
            return;
        }
        String[] parsed = parseSkillFile(raw); // [0]=description, [1]=prompt body
        String description = parsed[0];
        String prompt = parsed[1];

        // 2. Register/update skill
        ensureOneBuiltinSkill(accessor, spec.registeredName, description, prompt);

        // 3. Load references.txt manifest and ensure references
        String manifest = loadClasspathResource(spec.resourceBase + "references.txt");
        if (manifest != null) {
            List<String> refFiles = parseManifest(manifest);
            ensureReferences(refAccessor, spec.registeredName, spec.resourceBase + "references/", refFiles);
        }
    }

    /**
     * Parse a SKILL.md file with optional YAML frontmatter.
     * Returns [description, promptBody].
     * If frontmatter exists (starts with ---), extracts the description field and
     * returns the content after the closing --- as the prompt body.
     */
    private String[] parseSkillFile(String raw) {
        String description = "";
        String promptBody = raw;

        if (raw.startsWith("---\n") || raw.startsWith("---\r\n")) {
            // Find the closing ---
            int secondDash = raw.indexOf("\n---\n", 4);
            if (secondDash < 0) {
                secondDash = raw.indexOf("\n---\r\n", 4);
            }
            if (secondDash > 0) {
                String frontmatter = raw.substring(4, secondDash);
                promptBody = raw.substring(secondDash + 5); // skip \n---\n
                // Parse description from frontmatter (line-by-line)
                description = parseFrontmatterDescription(frontmatter);
            }
        }

        // Truncate description to 1000 chars
        if (description.length() > 1000) {
            description = description.substring(0, 1000);
        }

        return new String[] {description, promptBody.trim()};
    }

    /**
     * Parse the 'description' field from YAML frontmatter.
     * Supports both single-line and multi-line (| block scalar) values.
     */
    private String parseFrontmatterDescription(String frontmatter) {
        String[] lines = frontmatter.split("\n");
        StringBuilder desc = new StringBuilder();
        boolean inDescription = false;
        int baseIndent = -1;

        for (String line : lines) {
            if (!inDescription) {
                if (line.startsWith("description:")) {
                    String value = line.substring("description:".length()).trim();
                    if (value.equals("|") || value.equals("|-") || value.equals("|+")) {
                        // Multi-line block scalar
                        inDescription = true;
                    } else if (!value.isEmpty()) {
                        // Single-line value
                        return value;
                    }
                }
            } else {
                // Inside multi-line description block
                if (line.isEmpty() || line.trim().isEmpty()) {
                    desc.append("\n");
                    continue;
                }
                int indent = 0;
                while (indent < line.length() && line.charAt(indent) == ' ') {
                    indent++;
                }
                if (indent == 0) {
                    // No indent means we left the block
                    break;
                }
                if (baseIndent < 0) {
                    baseIndent = indent;
                }
                String content = indent >= baseIndent ? line.substring(baseIndent) : line.trim();
                if (desc.length() > 0) {
                    desc.append("\n");
                }
                desc.append(content);
            }
        }
        return desc.toString().trim();
    }

    /**
     * Parse references.txt manifest — one filename per line, skip empty/comment lines.
     */
    private List<String> parseManifest(String manifest) {
        List<String> files = new ArrayList<>();
        for (String line : manifest.split("\n")) {
            String trimmed = line.trim();
            if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
                files.add(trimmed);
            }
        }
        return files;
    }

    /**
     * Ensure all references listed in manifest are registered for the skill.
     * Only inserts missing references (idempotent).
     */
    private void ensureReferences(SkillReferenceAccessor refAccessor, String skillName,
                                  String resourceBase, List<String> refFiles) {
        // Get existing reference names
        List<SkillReferenceRecord> existingRefs = refAccessor.queryBySkillName(skillName);
        Set<String> existingNames = new HashSet<>();
        for (SkillReferenceRecord r : existingRefs) {
            existingNames.add(r.refName);
        }

        // Insert missing references
        for (String refFile : refFiles) {
            String refName = refFile.endsWith(".md") ? refFile.substring(0, refFile.length() - 3) : refFile;
            if (existingNames.contains(refName)) {
                continue;
            }
            String refContent = loadClasspathResource(resourceBase + refFile);
            if (refContent != null) {
                SkillReferenceRecord ref = new SkillReferenceRecord();
                ref.skillName = skillName;
                ref.refName = refName;
                ref.content = refContent;
                ref.priority = 100;
                refAccessor.insertReference(ref);
            }
        }
    }

    private void ensureOneBuiltinSkill(SkillConfigAccessor accessor, String name, String desc, String prompt) {
        List<SkillConfigRecord> existing = accessor.querySkillByName(name);
        if (existing.isEmpty()) {
            SkillConfigRecord record = new SkillConfigRecord();
            record.name = name;
            record.description = desc;
            record.prompt = prompt;
            record.status = STATUS_ACTIVE;
            record.priority = 100;
            record.builtin = 1;
            accessor.insertSkill(record);
        } else {
            SkillConfigRecord cur = existing.get(0);
            if (!prompt.equals(cur.prompt) || !desc.equals(cur.description)) {
                SkillConfigRecord update = new SkillConfigRecord();
                update.name = name;
                update.description = desc;
                update.prompt = prompt;
                update.status = cur.status;
                update.priority = cur.priority;
                accessor.updateSkill(update);
            }
        }
    }

    private String loadClasspathResource(String path) {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path)) {
            if (is == null) {
                logger.warn("Classpath resource not found: " + path);
                return null;
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append('\n');
            }
            return sb.toString();
        } catch (Exception e) {
            logger.warn("Failed to load classpath resource: " + path, e);
            return null;
        }
    }

    protected static class SkillConfigListener implements ConfigListener {
        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            SkillManager.getInstance().reloadFromMetaDb();
        }
    }
}
