package com.yp114.omc.grasp;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import com.yp114.omc.utils.HttpUtil;
import com.yp114.omc.utils.JDBCUtill;
import com.yp114.omc.utils.RopUtils;
//import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
//import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;


class o2o {
    //http://116.228.55.172:9088/bop/service       // 真实环境
    //http://116.228.55.190:50022/bop/service      // 测试环境
    public static final String SERVER_URL = "http://116.228.55.172:9088/bop/service";// 测试环境
    JDBCUtill jdbcUtill = new JDBCUtill();
    static String[] types = {"0", "1", "2", "3"};
    boolean useProxy = false;

    private ObjectMapper objectMapper = null;

    @Before
    public void setUp() throws Exception {
        objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    }

    // 查询统计订单--今天小时
    @Test
    public void testQueryStatisticsOrderByHour() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("appKey", "query_statistics_order"); // 查询统计订单
        map.put("method", "common.queryStatisticsOrder");
        map.put("v", "1.0");
        map.put("messageFormat", "json");
        map.put("lb", "TH");
        map.put("type", "1");
        map.put("hour", "16");
        map.put("prodCode", "query_statistics");// 查询统计订单
        String sign = RopUtils.sign(map, "D9811912A975507257412BE137B3076E"); // 查询统计订单

        map.put("sign", sign);

        String response = HttpUtil.postServer(SERVER_URL, map, useProxy);
        System.out.println(response);
        Map<String, Object> reponseMapper = objectMapper.readValue(response, HashMap.class);
        Object status = reponseMapper.get("status");
        // Object msg = reponseMapper.get("msg");
        // Object data = reponseMapper.get("data");
        // System.out.println(status.toString());
        // System.out.println(msg.toString());
        // System.out.println(data.toString());

        //data:时间，订单量，交易额


    }

    // 查询统计订单 -以前每天
    @Test
    public void testQueryStatisticsOrderByDay() throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("appKey", "query_statistics_order"); // 查询统计订单
        map.put("method", "common.queryStatisticsOrder");
        map.put("v", "1.0");
        map.put("messageFormat", "json");
        map.put("lb", "YD");
        map.put("type", "1");
        map.put("date", "2016-05-01");
        map.put("prodCode", "query_statistics");// 查询统计订单
        String sign = RopUtils.sign(map, "D9811912A975507257412BE137B3076E"); // 查询统计订单

        map.put("sign", sign);

        String response = HttpUtil.postServer(SERVER_URL, map, useProxy);
        System.out.println(response);
        Map<String, Object> reponseMapper = objectMapper.readValue(response, HashMap.class);
        Object status = reponseMapper.get("status");
        // Object msg = reponseMapper.get("msg");
        // Object data = reponseMapper.get("data");
        // System.out.println(status.toString());
        // System.out.println(msg.toString());
        // System.out.println(data.toString());

        //data:省份，订单量，交易额

    }

    public static void main(String[] args) throws Exception {
        //获取时间
        String time = args[0];
        o2o o2ogp = new o2o();
        //循环type
        for (int j = 0; j < 4; j++) {
            o2ogp.basegrasp(time, types[j]);
        }


    }

    public void basegrasp(String time, String type) throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("appKey", "query_statistics_order"); // 查询统计订单
        map.put("method", "common.queryStatisticsOrder");
        map.put("v", "1.0");
        map.put("messageFormat", "json");
        map.put("type", type);
        //时间参数判断
        Date date = null;
        String lb = null;
        int hour = 0;
        if (time.length() == 10) {
            map.put("date", time);
            lb = "YD";
        } else if (time.length() == 2) {
            map.put("hour", time);
            hour = Integer.parseInt(time);
            date = time=="23"?new Date(new java.util.Date().getTime()-24*60*60*1000):new Date(new java.util.Date().getTime());
            lb = "TH";
        }
        map.put("lb", lb);
        map.put("prodCode", "query_statistics");// 查询统计订单
        String sign = RopUtils.sign(map, "D9811912A975507257412BE137B3076E"); // 查询统计订单
        map.put("sign", sign);
        String response = HttpUtil.postServer(SERVER_URL, map, useProxy);
        //System.out.println(response);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

        Map<String, Object> reponseMapper = objectMapper.readValue(response, HashMap.class);
        Object status = reponseMapper.get("status");
        String data = reponseMapper.get("data").toString();
        //System.out.println(data);
        String[] splits = data.split("], \\[");
        //System.out.println(data.split("], \\[").length);

        //插入数据库
        PreparedStatement ppmt = null;
        ResultSet resultSet = null;
        Connection cnn = JDBCUtill.getConn();
        String o2osql = "INSERT into ta_o2o_order_hour (day_id,hour_id,region,type,order_cnt,order_amt) values (?,?,?,?,?,?)";
        ppmt = cnn.prepareStatement(o2osql);
        //解析字符串
        for (String s : splits) {
            String[] s2 = s.replace("]", "").replace("[", "").split(",");
            ppmt.setDate(1, date);
            ppmt.setInt(2, hour);
            //System.out.println(s2[0]);
            String s1 = new String(s2[0].getBytes(),"utf-8");
            ppmt.setString(3, s2[0]);
            ppmt.setInt(4,Integer.parseInt(type));
            Double d1= s2[1].contains("null")?0.0:Double.parseDouble(s2[1]);
            Double d2= s2[2].contains("null")?0.0:Double.parseDouble(s2[2]);
            ppmt.setDouble(5,d1);
            ppmt.setDouble(6,d2);

            ppmt.addBatch();
            ppmt.executeBatch();
        }
        ppmt.close();




    }

}
