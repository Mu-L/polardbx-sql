package com.alibaba.polardbx.optimizer.partition.datatype.function.udf;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.druid.support.json.JSONUtils;
import org.junit.Test;

import java.util.Map;

public class DbleRouteTest {


    /**
     * Single test
     */
    @Test
    public void testDbleRouteInit() {

        try {


            String jsonText = "{\n"
                + "          \"algorithm\": \"mymurhash\",\n"
                + "          \"params\": {\n"
                + "              \"partitionCount\": \"4\",　\n"
                + "              \"defaultNode\": \"0\",\n"
                + "              \"mappings\": [\n"
                + "                \"0-200M=0\",\n"
                + "                \"200M1-400M=1\",\n"
                + "                \"400M1-600M=2\",\n"
                + "                \"600M1-800M=3\",\n"
                + "                \"800M1-1000M=4\"\n"
                + "              ]\n"
                + "          }\n"
                + "  }";
//            String jsonText = "{\n"
//                + "          \"algorithm\": \"mymurhash\",\n"
//                + "          \"params\": {\n"
//                + "              \"partitionCount\": \"4\",　\n"
//                + "              \"defaultNode\": \"0\"\n"
//                + "          }\n"
//                + "  }";
            Map<String,Object> jsonObj = (Map<String, Object>) JSONUtils.parse(jsonText);
            JSONObject topNJson = JSON.parseObject(jsonText);
            String jsonRs = jsonObj.toString();
            System.out.println(jsonRs);
            JSON.parseObject(null);



        } catch (Throwable ex) {
            ex.printStackTrace();
        }


    }
}
