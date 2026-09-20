package com.alibaba.polardbx.druid.sql.ast.expr;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLExprImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * @author chenghui.lch
 */
public class SQLTimeToLiveDefinitionExpr extends SQLExprImpl {

    protected SQLExpr ttlEnableExpr;
    protected SQLExpr ttlExpr;
    protected SQLExpr ttlJobExpr;
    protected SQLExpr ttlColEncoderExpr;
    protected SQLExpr ttlColDecoderExpr;
    protected SQLExpr ttlFilterExpr;
    protected SQLExpr ttlPartIntervalExpr;
    protected SQLExpr ttlCleanupExpr;
    protected SQLExpr archiveTypeExpr;
    protected SQLExpr archiveTableSchemaExpr;
    protected SQLExpr archiveTableNameExpr;
    protected SQLExpr archiveTablePreAllocateExpr;
    protected SQLExpr archiveTablePostAllocateExpr;
    protected SQLExpr ttlRefColList;
    protected SQLExpr ttlHybrid;

    public SQLTimeToLiveDefinitionExpr() {
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        SQLTimeToLiveDefinitionExpr otherTtlDefineExpr = (SQLTimeToLiveDefinitionExpr) obj;
        SQLExpr otherTtlEnableExpr = otherTtlDefineExpr.getTtlEnableExpr();
        SQLExpr otherTtlExpr = otherTtlDefineExpr.getTtlExpr();
        SQLExpr otherTtlJobExpr = otherTtlDefineExpr.getTtlJobExpr();
        SQLExpr otherTtlEncoderExpr = otherTtlDefineExpr.getTtlColEncoderExpr();
        SQLExpr otherTtlDecoderExpr = otherTtlDefineExpr.getTtlColDecoderExpr();
        SQLExpr otherTtlFilterExpr = otherTtlDefineExpr.getTtlFilterExpr();
        SQLExpr otherTtlSkipCleanupExpr = otherTtlDefineExpr.getTtlCleanupExpr();
        SQLExpr otherArchiveTypeExpr = otherTtlDefineExpr.getArchiveTypeExpr();
        SQLExpr otherArchiveSchemaExpr = otherTtlDefineExpr.getArchiveTableSchemaExpr();
        SQLExpr otherArchiveNameExpr = otherTtlDefineExpr.getArchiveTableNameExpr();
        SQLExpr otherArchivePreAllocateExpr = otherTtlDefineExpr.getArchiveTablePreAllocateExpr();
        SQLExpr otherArchivePostAllocateExpr = otherTtlDefineExpr.getArchiveTablePostAllocateExpr();
        SQLExpr otherTtlRefColList = otherTtlDefineExpr.getTtlRefColList();
        SQLExpr otherTtlHybrid = otherTtlDefineExpr.getTtlHybrid();

        if (ttlEnableExpr != null) {
            if (otherTtlEnableExpr == null) {
                return false;
            }
            if (!ttlEnableExpr.equals(otherTtlEnableExpr)) {
                return false;
            }
        } else {
            if (otherTtlEnableExpr != null) {
                return false;
            }
        }

        if (ttlExpr != null) {
            if (otherTtlExpr == null) {
                return false;
            }
            if (!ttlExpr.equals(otherTtlExpr)) {
                return false;
            }
        } else {
            if (otherTtlExpr != null) {
                return false;
            }
        }

        if (ttlJobExpr != null) {
            if (otherTtlJobExpr == null) {
                return false;
            }
            if (!ttlJobExpr.equals(otherTtlJobExpr)) {
                return false;
            }
        } else {
            if (otherTtlJobExpr != null) {
                return false;
            }
        }

        if (ttlColEncoderExpr != null) {
            if (otherTtlEncoderExpr == null) {
                return false;
            }
            if (!ttlColEncoderExpr.equals(otherTtlEncoderExpr)) {
                return false;
            }
        } else {
            if (otherTtlEncoderExpr != null) {
                return false;
            }
        }

        if (ttlColDecoderExpr != null) {
            if (otherTtlDecoderExpr == null) {
                return false;
            }
            if (!ttlColDecoderExpr.equals(otherTtlDecoderExpr)) {
                return false;
            }
        } else {
            if (otherTtlDecoderExpr != null) {
                return false;
            }
        }

        if (ttlFilterExpr != null) {
            if (otherTtlFilterExpr == null) {
                return false;
            }
            if (!ttlFilterExpr.equals(otherTtlFilterExpr)) {
                return false;
            }
        } else {
            if (otherTtlFilterExpr != null) {
                return false;
            }
        }

        if (ttlCleanupExpr != null) {
            if (otherTtlSkipCleanupExpr == null) {
                return false;
            }
            if (!ttlCleanupExpr.equals(otherTtlSkipCleanupExpr)) {
                return false;
            }
        } else {
            if (otherTtlSkipCleanupExpr != null) {
                return false;
            }
        }

        if (archiveTypeExpr != null) {
            if (otherArchiveTypeExpr == null) {
                return false;
            }
            if (!archiveTypeExpr.equals(otherArchiveTypeExpr)) {
                return false;
            }
        } else {
            if (otherArchiveTypeExpr != null) {
                return false;
            }
        }

        if (archiveTableSchemaExpr != null) {
            if (otherArchiveSchemaExpr == null) {
                return false;
            }
            if (!archiveTableSchemaExpr.equals(otherArchiveSchemaExpr)) {
                return false;
            }
        } else {
            if (otherArchiveSchemaExpr != null) {
                return false;
            }
        }

        if (archiveTableNameExpr != null) {
            if (otherArchiveNameExpr == null) {
                return false;
            }
            if (!archiveTableNameExpr.equals(otherArchiveNameExpr)) {
                return false;
            }
        } else {
            if (otherArchiveNameExpr != null) {
                return false;
            }
        }

        if (archiveTablePreAllocateExpr != null) {
            if (otherArchivePreAllocateExpr == null) {
                return false;
            }
            if (!archiveTablePreAllocateExpr.equals(otherArchivePreAllocateExpr)) {
                return false;
            }
        } else {
            if (otherArchivePreAllocateExpr != null) {
                return false;
            }
        }

        if (archiveTablePostAllocateExpr != null) {
            if (otherArchivePostAllocateExpr == null) {
                return false;
            }
            if (!archiveTablePostAllocateExpr.equals(otherArchivePostAllocateExpr)) {
                return false;
            }
        } else {
            if (otherArchivePostAllocateExpr != null) {
                return false;
            }
        }

        if (ttlRefColList != null) {
            if (otherTtlRefColList == null) {
                return false;
            }
            if (!ttlRefColList.equals(otherTtlRefColList)) {
                return false;
            }
        } else {
            if (otherTtlRefColList != null) {
                return false;
            }
        }

        if (ttlHybrid != null) {
            if (otherTtlHybrid == null) {
                return false;
            }
            if (!ttlHybrid.equals(otherTtlHybrid)) {
                return false;
            }
        } else {
            if (otherTtlHybrid != null) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("");
        SQLASTVisitor visitor = new MySqlOutputVisitor(sb);
        this.accept(visitor);
        return sb.toString();
    }

    @Override
    public int hashCode() {

        int result = ttlEnableExpr != null ? ttlEnableExpr.hashCode() : 0;
        result = 31 * result + (ttlExpr != null ? ttlExpr.hashCode() : 0);
        result = 31 * result + (ttlJobExpr != null ? ttlJobExpr.hashCode() : 0);
        result = 31 * result + (ttlColEncoderExpr != null ? ttlColEncoderExpr.hashCode() : 0);
        result = 31 * result + (ttlColDecoderExpr != null ? ttlColDecoderExpr.hashCode() : 0);
        result = 31 * result + (ttlFilterExpr != null ? ttlFilterExpr.hashCode() : 0);
        result = 31 * result + (ttlCleanupExpr != null ? ttlCleanupExpr.hashCode() : 0);
        result = 31 * result + (ttlPartIntervalExpr != null ? ttlPartIntervalExpr.hashCode() : 0);
        result = 31 * result + (archiveTypeExpr != null ? archiveTypeExpr.hashCode() : 0);
        result = 31 * result + (archiveTableSchemaExpr != null ? archiveTableSchemaExpr.hashCode() : 0);
        result = 31 * result + (archiveTableNameExpr != null ? archiveTableNameExpr.hashCode() : 0);
        result = 31 * result + (archiveTablePreAllocateExpr != null ? archiveTablePreAllocateExpr.hashCode() : 0);
        result = 31 * result + (archiveTablePostAllocateExpr != null ? archiveTablePostAllocateExpr.hashCode() : 0);
        result = 31 * result + (ttlRefColList != null ? ttlRefColList.hashCode() : 0);
        result = 31 * result + (ttlHybrid != null ? ttlHybrid.hashCode() : 0);

        return result;
    }

    @Override
    public SQLExpr clone() {

        SQLTimeToLiveDefinitionExpr sqlTimeToLiveDefinitionExpr = new SQLTimeToLiveDefinitionExpr();

        if (ttlEnableExpr != null) {
            sqlTimeToLiveDefinitionExpr.setTtlEnableExpr(ttlEnableExpr.clone());
        }

        if (ttlExpr != null) {
            sqlTimeToLiveDefinitionExpr.setTtlExpr(ttlExpr.clone());
        }

        if (ttlJobExpr != null) {
            sqlTimeToLiveDefinitionExpr.setTtlJobExpr(ttlJobExpr.clone());
        }

        if (ttlColEncoderExpr != null) {
            sqlTimeToLiveDefinitionExpr.setTtlColEncoderExpr(ttlColEncoderExpr.clone());
        }

        if (ttlColDecoderExpr != null) {
            sqlTimeToLiveDefinitionExpr.setTtlColDecoderExpr(ttlColDecoderExpr.clone());
        }

        if (ttlFilterExpr != null) {
            sqlTimeToLiveDefinitionExpr.setTtlFilterExpr(ttlFilterExpr.clone());
        }

        if (ttlCleanupExpr != null) {
            sqlTimeToLiveDefinitionExpr.setTtlCleanupExpr(ttlCleanupExpr.clone());
        }

        if (ttlPartIntervalExpr != null) {
            sqlTimeToLiveDefinitionExpr.setTtlPartIntervalExpr(ttlPartIntervalExpr.clone());
        }

        if (archiveTypeExpr != null) {
            sqlTimeToLiveDefinitionExpr.setArchiveTypeExpr(archiveTypeExpr.clone());
        }

        if (archiveTableSchemaExpr != null) {
            sqlTimeToLiveDefinitionExpr.setArchiveTableSchemaExpr(archiveTableSchemaExpr.clone());
        }

        if (archiveTableNameExpr != null) {
            sqlTimeToLiveDefinitionExpr.setArchiveTableNameExpr(archiveTableNameExpr.clone());
        }

        if (archiveTablePreAllocateExpr != null) {
            sqlTimeToLiveDefinitionExpr.setArchiveTablePreAllocateExpr(archiveTablePreAllocateExpr.clone());
        }

        if (archiveTablePostAllocateExpr != null) {
            sqlTimeToLiveDefinitionExpr.setArchiveTablePostAllocateExpr(archiveTablePostAllocateExpr.clone());
        }

        if (ttlRefColList != null) {
            sqlTimeToLiveDefinitionExpr.setTtlRefColList(ttlRefColList.clone());
        }

        if (ttlHybrid != null) {
            sqlTimeToLiveDefinitionExpr.setTtlHybrid(ttlHybrid.clone());
        }

        return sqlTimeToLiveDefinitionExpr;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (ttlEnableExpr != null) {
                acceptChild(visitor, ttlEnableExpr);
            }

            if (ttlExpr != null) {
                acceptChild(visitor, ttlExpr);
            }

            if (ttlJobExpr != null) {
                acceptChild(visitor, ttlJobExpr);
            }

            if (ttlFilterExpr != null) {
                acceptChild(visitor, ttlFilterExpr);
            }

            if (archiveTypeExpr != null) {
                acceptChild(visitor, archiveTypeExpr);
            }

            if (archiveTableSchemaExpr != null) {
                acceptChild(visitor, archiveTableSchemaExpr);
            }

            if (archiveTableNameExpr != null) {
                acceptChild(visitor, archiveTableNameExpr);
            }

            if (archiveTablePreAllocateExpr != null) {
                acceptChild(visitor, archiveTablePreAllocateExpr);
            }

            if (archiveTablePostAllocateExpr != null) {
                acceptChild(visitor, archiveTablePostAllocateExpr);
            }

            if (ttlRefColList != null) {
                acceptChild(visitor, ttlRefColList);
            }

            if (ttlHybrid != null) {
                acceptChild(visitor, ttlHybrid);
            }
        }
        visitor.endVisit(this);
    }

    public SQLExpr getTtlEnableExpr() {
        return ttlEnableExpr;
    }

    public void setTtlEnableExpr(SQLExpr ttlEnableExpr) {
        this.ttlEnableExpr = ttlEnableExpr;
    }

    public SQLExpr getTtlExpr() {
        return ttlExpr;
    }

    public void setTtlExpr(SQLExpr ttlExpr) {
        this.ttlExpr = ttlExpr;
    }

    public SQLExpr getTtlJobExpr() {
        return ttlJobExpr;
    }

    public void setTtlJobExpr(SQLExpr ttlJobExpr) {
        this.ttlJobExpr = ttlJobExpr;
    }

    public SQLExpr getTtlFilterExpr() {
        return ttlFilterExpr;
    }

    public void setTtlFilterExpr(SQLExpr ttlFilterExpr) {
        this.ttlFilterExpr = ttlFilterExpr;
    }

    public SQLExpr getArchiveTypeExpr() {
        return archiveTypeExpr;
    }

    public void setArchiveTypeExpr(SQLExpr archiveTypeExpr) {
        this.archiveTypeExpr = archiveTypeExpr;
    }

    public SQLExpr getArchiveTableSchemaExpr() {
        return archiveTableSchemaExpr;
    }

    public void setArchiveTableSchemaExpr(SQLExpr archiveTableSchemaExpr) {
        this.archiveTableSchemaExpr = archiveTableSchemaExpr;
    }

    public SQLExpr getArchiveTableNameExpr() {
        return archiveTableNameExpr;
    }

    public void setArchiveTableNameExpr(SQLExpr archiveTableNameExpr) {
        this.archiveTableNameExpr = archiveTableNameExpr;
    }

    public SQLExpr getArchiveTablePreAllocateExpr() {
        return archiveTablePreAllocateExpr;
    }

    public void setArchiveTablePreAllocateExpr(SQLExpr archiveTablePreAllocateExpr) {
        this.archiveTablePreAllocateExpr = archiveTablePreAllocateExpr;
    }

    public SQLExpr getArchiveTablePostAllocateExpr() {
        return archiveTablePostAllocateExpr;
    }

    public void setArchiveTablePostAllocateExpr(SQLExpr archiveTablePostAllocateExpr) {
        this.archiveTablePostAllocateExpr = archiveTablePostAllocateExpr;
    }

    public SQLExpr getTtlCleanupExpr() {
        return ttlCleanupExpr;
    }

    public void setTtlCleanupExpr(SQLExpr ttlCleanupExpr) {
        this.ttlCleanupExpr = ttlCleanupExpr;
    }

    public SQLExpr getTtlPartIntervalExpr() {
        return ttlPartIntervalExpr;
    }

    public void setTtlPartIntervalExpr(SQLExpr ttlPartIntervalExpr) {
        this.ttlPartIntervalExpr = ttlPartIntervalExpr;
    }

    public SQLExpr getTtlColEncoderExpr() {
        return ttlColEncoderExpr;
    }

    public void setTtlColEncoderExpr(SQLExpr ttlColEncoderExpr) {
        this.ttlColEncoderExpr = ttlColEncoderExpr;
    }

    public SQLExpr getTtlColDecoderExpr() {
        return ttlColDecoderExpr;
    }

    public void setTtlColDecoderExpr(SQLExpr ttlColDecoderExpr) {
        this.ttlColDecoderExpr = ttlColDecoderExpr;
    }

    public SQLExpr getTtlRefColList() {
        return ttlRefColList;
    }

    public void setTtlRefColList(SQLExpr ttlRefColList) {
        this.ttlRefColList = ttlRefColList;
    }

    public SQLExpr getTtlHybrid() {
        return ttlHybrid;
    }

    public void setTtlHybrid(SQLExpr ttlHybrid) {
        this.ttlHybrid = ttlHybrid;
    }
}
