package com.alibaba.polardbx.manager.response;

import com.alibaba.polardbx.server.response.ShowHelp;
import com.google.common.truth.Truth;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;

public class ShowHelpTest {

    List<ShowHelp.HelpData> getData() {
        try {
            // 获取 ShowHelp 类
            Class<?> showHelpClass = null;
            showHelpClass = Class.forName("com.alibaba.polardbx.server.response.ShowHelp");

            // 获取 datas 字段
            Field datasField = showHelpClass.getDeclaredField("datas");

            // 设置字段可访问（绕过 private 限制）
            datasField.setAccessible(true);

            // 获取 datas 集合的值
            return (List<ShowHelp.HelpData>) datasField.get(null); // 静态字段，所以传 null
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRoutingRule() {
        try {
            List<ShowHelp.HelpData> data = getData();
            Class<?> helpDataClass = Class.forName("com.alibaba.polardbx.server.response.ShowHelp$HelpData");
            // 获取 key 字段
            Field keyField = helpDataClass.getDeclaredField("key");
            keyField.setAccessible(true);
            Field descField = helpDataClass.getDeclaredField("desc");
            descField.setAccessible(true);
            Field exampleField = helpDataClass.getDeclaredField("example");
            exampleField.setAccessible(true);

            for (ShowHelp.HelpData d : data) {
                String key = (String) keyField.get(d);
                String desc = (String) descField.get(d);
                String example = (String) exampleField.get(d);
                if (key.equals("explain keyword SQL")) {
                    Truth.assertThat(desc).isEqualTo("Report keywords of sql");
                    Truth.assertThat(example).isEqualTo("explain keyword select count(*) from user");
                }
                if (key.equals("explain routing SQL")) {
                    Truth.assertThat(desc).isEqualTo("Report routing process of sql");
                    Truth.assertThat(example).isEqualTo("explain routing select count(*) from user");
                }
                if (key.equals("show routing_rules")) {
                    Truth.assertThat(desc).isEqualTo("Report the routing rule info");
                    Truth.assertThat(example).isEqualTo("show routing_rules limit 2");
                }
            }
        } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

}
