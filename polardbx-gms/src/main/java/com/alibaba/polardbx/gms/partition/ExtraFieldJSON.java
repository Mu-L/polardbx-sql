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

package com.alibaba.polardbx.gms.partition;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.List;

/**
 * Extensible extra fields for table_partitions
 */
public class ExtraFieldJSON {
    public String partitionPattern;
    public String locality = "";
    protected String timeZone;
    protected String charset;
    protected String collation;
    /**
     * The ttl state of a partition of ttl_tmp (only used by ttl_tmp table) ,
     * this state is used to label if a part has been finished oss archiving.
     * <pre>
     *  arc_state_archived(state=3): means current part has been finish oss archiving;
     *  arc_state_archiving(state=2): means current part is performing oss archiving;
     *  arc_state_ready(state=1):  means current part has be ready to do archiving, waiting do archiving;
     *  arc_state_not_ready(state=0):  means current part is performing oss archiving.
     *
     *  (The enum definition is TtlPartArcState)
     *
     *  Notice:
     *  When a part is on the state of ttl_state_ready, it means that
     *  all the expired data of this part has been inserted, and not more increment。
     *  So, a ttl_state_ready-state part will REJECT any insert/update/delete operation.
     *
     * </pre>
     */
    protected Integer arcState;

    protected String arcBound;

    protected List<String> ttlRefColList;

    protected List<String> ttlRefColValueList;

    protected Boolean ttlHybrid;

    public ExtraFieldJSON() {
    }

    public static ExtraFieldJSON fromJson(String json) {
        return JSON.parseObject(json, ExtraFieldJSON.class);
    }

    public static String toJson(ExtraFieldJSON obj) {
        if (obj == null) {
            return "";
        }
        return JSON.toJSONString(obj);
    }

    @Override
    public String toString() {
        return toJson(this);
    }

    public String getPartitionPattern() {
        return partitionPattern;
    }

    public void setPartitionPattern(String partitionPattern) {
        this.partitionPattern = partitionPattern;
    }

    public String getLocality() {
        return locality;
    }

    public void setLocality(String locality) {
        this.locality = locality;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public String getCollation() {
        return collation;
    }

    public void setCollation(String collation) {
        this.collation = collation;
    }

    public Integer getArcState() {
        return arcState;
    }

    public void setArcState(Integer arcState) {
        this.arcState = arcState;
    }

    public String getArcBound() {
        return arcBound;
    }

    public void setArcBound(String arcBound) {
        this.arcBound = arcBound;
    }

    public List<String> getTtlRefColList() {
        return ttlRefColList;
    }

    public void setTtlRefColList(List<String> ttlRefColList) {
        this.ttlRefColList = ttlRefColList;
    }

    public List<String> getTtlRefColValueList() {
        return ttlRefColValueList;
    }

    public void setTtlRefColValueList(List<String> ttlRefColValueList) {
        this.ttlRefColValueList = ttlRefColValueList;
    }

    public Boolean getTtlHybrid() {
        return ttlHybrid;
    }

    public void setTtlHybrid(Boolean ttlHybrid) {
        this.ttlHybrid = ttlHybrid;
    }

    public ExtraFieldJSON copy() {
        ExtraFieldJSON extraFieldJSON = new ExtraFieldJSON();
        extraFieldJSON.setPartitionPattern(this.partitionPattern);
        extraFieldJSON.setLocality(this.locality);
        extraFieldJSON.setTimeZone(this.timeZone);
        extraFieldJSON.setCharset(this.charset);
        extraFieldJSON.setCollation(this.collation);
        extraFieldJSON.setArcState(this.arcState);
        extraFieldJSON.setArcBound(this.arcBound);
        extraFieldJSON.setTtlRefColList(this.ttlRefColList == null ? null : new ArrayList<>(this.ttlRefColList));
        extraFieldJSON.setTtlRefColValueList(
            this.ttlRefColValueList == null ? null : new ArrayList<>(this.ttlRefColValueList));
        extraFieldJSON.setTtlHybrid(this.ttlHybrid);
        return extraFieldJSON;
    }
}
