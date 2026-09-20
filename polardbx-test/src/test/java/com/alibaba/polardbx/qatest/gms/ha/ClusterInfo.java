package com.alibaba.polardbx.qatest.gms.ha;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import lombok.Data;

@Data
public class ClusterInfo {
    public static final String INIT_URL_TEMPLATE =
        "jdbc:mysql://%s:%s/?allowMultiQueries=true&rewriteBatchedStatements=true&connectTimeout=10000&socketTimeout=15000&zeroDateTimeBehavior=convertToNull&serverTimezone=Asia/Shanghai&allowLoadLocalInfile=true";
    public static final String URL_TEMPLATE =
        "jdbc:mysql://%s:%s/?allowMultiQueries=true&rewriteBatchedStatements=true&connectTimeout=2000&socketTimeout=2000&zeroDateTimeBehavior=convertToNull&serverTimezone=Asia/Shanghai&allowLoadLocalInfile=true";
    public static final String HA_CHECK_URL_TEMPLATE =
        "jdbc:mysql://%s:%s/?allowMultiQueries=true&rewriteBatchedStatements=true&connectTimeout=2000&socketTimeout=1000&zeroDateTimeBehavior=convertToNull&serverTimezone=Asia/Shanghai&allowLoadLocalInfile=true";

    protected String url;
    protected String urlOfInitHaEnv;
    protected String urlOfCheckIfHaFinished;
    protected String user;
    protected String password;

    public ClusterInfo() {

    }

    public static ClusterInfo getPolarDBXClusterInfoFromEnv() {
        String user = ConnectionManager.getInstance().getPolardbxUser();
        String passwd = ConnectionManager.getInstance().getPolardbxPassword();
        String addr = ConnectionManager.getInstance().getPolardbxAddress();
        String port = ConnectionManager.getInstance().getPolardbxPort();
        ClusterInfo clusterInfo = new ClusterInfo();
        clusterInfo.setUser(user);
        clusterInfo.setPassword(passwd);
        clusterInfo.setUrl(String.format(URL_TEMPLATE, addr, port));
        clusterInfo.setUrlOfInitHaEnv(String.format(INIT_URL_TEMPLATE, addr, port));
        clusterInfo.setUrlOfCheckIfHaFinished(String.format(HA_CHECK_URL_TEMPLATE, addr, port));
        return clusterInfo;
    }
}
