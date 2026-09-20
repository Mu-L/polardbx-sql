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

package com.alibaba.polardbx.gms.privilege;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author shicai.xsc 2020/3/3 13:34
 * @since 5.0.0.0
 */
public class PolarTbPriv extends BasePolarPriv {

    private String catalogName = "";
    private String dbName;
    private String tbName;

    public PolarTbPriv() {
        super(PrivilegeScope.TABLE);
    }

    static Collection<PolarAccountInfo> loadTbPrivs(Connection conn, Collection<PolarAccount> accounts)
        throws SQLException {
        final String sql = String.format("SELECT * FROM %s WHERE %s = ? and %s = ?", PolarPrivUtil.TABLE_PRIV_TABLE,
            PolarPrivUtil.USER_NAME, PolarPrivUtil.HOST);
        List<PolarAccountInfo> accountInfos = new ArrayList<>(accounts.size());
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (PolarAccount account : accounts) {
                stmt.setString(1, account.getUsername());
                stmt.setString(2, account.getHost());

                PolarAccountInfo info = new PolarAccountInfo(account);
                try (ResultSet rs = stmt.executeQuery()) {
                    loadTbPriv(rs, info::addTbPriv);
                }
                accountInfos.add(info);
            }
        }

        return accountInfos;
    }

    static void loadTbPriv(ResultSet rs, Consumer<PolarTbPriv> consumer) throws SQLException {
        while (rs.next()) {
            PolarTbPriv tbPriv = new PolarTbPriv();
            loadBasePriv(rs, tbPriv);
            tbPriv.setCatalogName(rs.getString("catalog_name"));
            tbPriv.setDbName(rs.getString(PolarPrivUtil.DB_NAME));
            tbPriv.setTbName(rs.getString(PolarPrivUtil.TABLE_NAME));
            consumer.accept(tbPriv);
        }
    }

    public PolarTbPriv deepCopy() {
        PolarTbPriv clone = new PolarTbPriv();
        copy(this, clone);
        clone.catalogName = this.catalogName;
        clone.dbName = this.dbName;
        clone.tbName = this.tbName;
        return clone;
    }

    @Override
    public String getIdentifier() {
        if (isCatalogPriv()) {
            return (catalogName + "." + dbName + "@" + tbName).toLowerCase();
        }
        return (dbName + "@" + tbName).toLowerCase();
    }

    @Override
    public String toInsertNewSql() {
        return String.format(
            "INSERT IGNORE INTO %s(%s, %s, catalog_name, %s, %s) VALUES ('%s', '%s', '%s', '%s', '%s')",
            PolarPrivUtil.TABLE_PRIV_TABLE,
            PolarPrivUtil.USER_NAME,
            PolarPrivUtil.HOST,
            PolarPrivUtil.DB_NAME,
            PolarPrivUtil.TABLE_NAME,
            userName,
            host,
            catalogName,
            dbName,
            tbName);
    }

    public boolean canCover(PolarTbPriv privToCover, boolean checkGrant) {
        return super.canCover(privToCover, checkGrant);
    }

    public Optional<String> showGrantsResult(PolarAccount user) {
        String displayTarget;
        if (isCatalogPriv()) {
            displayTarget = catalogName.toLowerCase() + "." + dbName.toLowerCase() + "." + tbName.toLowerCase();
        } else {
            displayTarget = dbName + "." + tbName;
        }
        return super.showGrantsResult(user, displayTarget, false);
    }

    public String getDbName() {
        return this.dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTbName() {
        return this.tbName;
    }

    public void setTbName(String tbName) {
        this.tbName = tbName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public boolean isCatalogPriv() {
        return catalogName != null && !catalogName.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PolarTbPriv that = (PolarTbPriv) o;
        return Objects.equals(getCatalogName(), that.getCatalogName())
            && Objects.equals(getDbName(), that.getDbName())
            && Objects.equals(getTbName(), that.getTbName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCatalogName(), getDbName(), getTbName());
    }

    @Override
    public String toString() {
        if (isCatalogPriv()) {
            return "PolarTbPriv(catalogName=" + this.getCatalogName() + ", dbName=" + this.getDbName()
                + ", tbName=" + this.getTbName() + ")";
        }
        return "PolarTbPriv(dbName=" + this.getDbName() + ", tbName=" + this.getTbName() + ")";
    }

    public Optional<String> toUpdatePrivilegeSql(boolean grant) {
        return toSetPrivilegeSql(grant).map(setSql ->
            String.format(
                "update %s set %s where %s = '%s' and %s = '%s' and catalog_name = '%s' and %s = '%s' and %s = '%s'",
                PolarPrivUtil.TABLE_PRIV_TABLE, setSql, PolarPrivUtil.USER_NAME, userName, PolarPrivUtil.HOST, host,
                catalogName, PolarPrivUtil.DB_NAME, dbName, PolarPrivUtil.TABLE_NAME, tbName));
    }

    @Override
    public List<Permission> toPermissions() {
        if (isCatalogPriv()) {
            // Catalog privileges live in a separate namespace and must not be mapped to local table permissions.
            return Collections.emptyList();
        }
        return getGrantedPrivileges()
            .stream()
            .map(privilege -> Permission.tablePermission(dbName, tbName, privilege))
            .collect(Collectors.toList());
    }
}
