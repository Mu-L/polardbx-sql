package com.alibaba.polardbx.qatest.ddl.auto.ghost;

import lombok.Data;

import java.util.List;

@Data
public class GhostTestBean {
    public String dbName;
    public String tableName;
    public String sqlMode;
    public List<String> prepareDb;
    public List<String> prepareTables;
    public List<String> prepareEvents;
    public String alterTableCommand;
    public String expectFailure;
    public List<String> expectTableStructure;
    public List<String> cleanupDDLs;
}
