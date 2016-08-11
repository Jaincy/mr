package com.yp114.omc.grasp;

import com.yp114.omc.utils.HttpUtil;
import com.yp114.omc.utils.JDBCUtill;
import com.yp114.omc.utils.RopUtils;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Jain on 2016/5/31.
 */
public class O2oUser {
    //http://116.228.55.172:9088/bop/service       // 真实环境
    //http://116.228.55.190:50022/bop/service      // 测试环境
    public static final String SERVER_URL = "http://116.228.55.172:9088/bop/service";// 测试环境
    JDBCUtill jdbcUtill = new JDBCUtill();
    boolean useProxy = false;
    static String[] types = {"0", "1", "2", "3"};
    private ObjectMapper objectMapper = new ObjectMapper();
    @Before
    public void setUp() throws Exception {
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    }
    public O2oUser(){
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    }


    // 查询统计订单 -以前每天
    @Test
    public void testQueryStatisticsOrderByDay() throws Exception {
        String time="";
        Map<String, String> map = new HashMap<String, String>();
        map.put("appKey", "query_active_users"); // 查询统计订单
        map.put("method", "common.queryActiveUsers");
        map.put("v", "1.0");
        map.put("messageFormat", "json");
        map.put("lb", "YD");
      //map.put("type", "1");
        map.put("startDate", time);
        map.put("endDate",time);
        map.put("date",time);
        map.put("month",time);
        map.put("prodCode", "queryActiveUsers");// 查询统计订单
        String sign = RopUtils.sign(map, "DAD47F07FB0517C26D2BE2B39AF685D0"); // 查询统计订单
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
        O2oUser o2oUser = new O2oUser();
        String type=args[0];
        String time=null;
        SimpleDateFormat dft = new SimpleDateFormat("yyyy-MM-dd");
        Date beginDate = new Date();
        Calendar date = Calendar.getInstance();
        date.setTime(beginDate);
        date.set(Calendar.DATE, date.get(Calendar.DATE) - 1);
        Date endDate = dft.parse(dft.format(date.getTime()));
        System.out.println(dft.format(endDate));

//        String time = "2016-05-01";
//        String type="storderDay";
        if (args.length == 2) {
            //判断请求类型，调用相应方法
            time = args[1];
        } else if (args.length == 1) {
            time=dft.format(endDate);
        }
        if (type.equals("cntUsers")){
            o2oUser.cntUsers(time);
        }else if (type.equals("activeUsers")){
            o2oUser.activeUsers();
        } else if (type.equals("reOrder")){
            o2oUser.reOrder(time);
        }else if (type.equals("appO2o")){
            o2oUser.appO2o(time);
        }else if (type.equals("storderDay")){
            for (int j = 0; j < 4; j++) {
                o2oUser.storderDay(time, types[j]);
            }
        }
    }
    @Test
    public void cntUsers(String time) throws Exception {
//        String time="2016-05-01";
        Map<String, String> map = new HashMap<String, String>();
        map.put("appKey", "query_users"); // 查询统计订单
        map.put("method", "common.queryUsers");
        map.put("v", "1.0");
        map.put("messageFormat", "json");
        map.put("startDate", time);
        map.put("endDate",time);
        map.put("prodCode", "query_users");// 查询统计订单
        String sign = RopUtils.sign(map, "82CD392AFEF6696264BC3059A41870B1"); // 查询统计订单
        map.put("sign", sign);
        String response = HttpUtil.postServer(SERVER_URL, map, useProxy);
       System.out.println(response);
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

        Map<String, Object> reponseMapper = objectMapper.readValue(response, HashMap.class);
        Object status = reponseMapper.get("status");
        String data = reponseMapper.get("data").toString();

        String[] splits = data.split("], \\[");
        //插入数据库
        Connection cnn = JDBCUtill.getConn();
        String o2osql = "INSERT into cntusers (day_id,province,today_cnts,total_cnts) values (?,?,?,?)";
        PreparedStatement ppmt  = cnn.prepareStatement(o2osql);
        //解析字符串
        for (String s : splits) {
            String[] s2 = s.replace("]", "").replace("[", "").split(",");
            ppmt.setString(1, time);
            ppmt.setString(2, s2[0]);
            Double d1 = s2[1].contains("null") ? 0.0 : Double.parseDouble(s2[1]);
            Double d2 = s2[2].contains("null") ? 0.0 : Double.parseDouble(s2[2]);
            ppmt.setDouble(3, d1);
            ppmt.setDouble(4, d2);
            ppmt.addBatch();
            ppmt.executeBatch();
        }
        ppmt.close();

    }

    @Test
    public void activeUsers() throws Exception {
        String time="2016-05";

//        添加参数
        Map<String, String> map = new HashMap<String, String>();
        map.put("appKey", "query_active_users"); // 查询统计订单
        map.put("method", "common.queryActiveUsers");
        map.put("v", "1.0");
        map.put("messageFormat", "json");
        map.put("month",time);
        map.put("prodCode", "queryActiveUsers");// 查询统计订单
        String sign = RopUtils.sign(map, "DAD47F07FB0517C26D2BE2B39AF685D0"); // 查询统计订单
        map.put("sign", sign);
//        发送请求，接收相应
        String response = HttpUtil.postServer(SERVER_URL, map, useProxy);
//        将结果代入map，从map中取值
        System.out.println(response);

        Map<String, Object> reponseMapper = objectMapper.readValue(response, HashMap.class);
        String data = reponseMapper.get("data").toString();
        //插入数据库,准备工作
        Connection cnn = JDBCUtill.getConn();
        String o2osql = "INSERT into activeusers (month_id,province,cnts) values (?,?,?)";
        PreparedStatement ppmt  = cnn.prepareStatement(o2osql);
        //解析字符串，给字段赋值
        String[] splits = data.trim().split("], \\[");
        for (String s : splits) {
            String[] s2 = s.replace("]", "").replace("[", "").split(",");
            ppmt.setString(1, time);
            ppmt.setString(2, s2[0]);
            int d1 = s2[1].contains("null")||s2[1].equals(" ") ? 0 : Integer.parseInt(s2[1].trim());
            ppmt.setInt(3, d1);
            ppmt.addBatch();
            ppmt.executeBatch();
        }
        ppmt.close();
    }

    @Test
    public void reOrder(String time) throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("appKey", "query_repeat_order"); // 查询统计订单
        map.put("method", "common.queryRepeatOrder");
        map.put("v", "1.0");
        map.put("messageFormat", "json");
        map.put("lb", "YD");
        map.put("month",time);
        map.put("prodCode", "queryRepeatOrder");// 查询统计订单
        String sign = RopUtils.sign(map, "3F9B309C3B4A7E3405FD7197C7A52F37"); // 查询统计订单
        map.put("sign", sign);
        String response = HttpUtil.postServer(SERVER_URL, map, useProxy);
        System.out.println(response);
        Map<String, Object> reponseMapper = objectMapper.readValue(response, HashMap.class);
        String data = reponseMapper.get("data").toString();
        String[] splits = data.trim().split("], \\[");

        //data:省份，交易额
        //插入数据库
        Connection cnn = JDBCUtill.getConn();
        String o2osql = "INSERT into reorder (month_id,province,re_rate_1,re_rate_2,re_rate_3,re_cnts_1,re_cnts_2,re_cnts_3,cnts_1,cnts_2,cnts_3)" +
                " values (?,?,?,?,?,?,?,?,?,?,?)";
        PreparedStatement ppmt  = cnn.prepareStatement(o2osql);

        //解析字符串
        for (String s : splits) {
            String[] s2 = s.replace("]", "").replace("[", "").split(",");
            ppmt.setString(1,time);
            ppmt.setString(2,s2[0]);
            Double d=0.0;
            for (int i=1;i<10;i++){
                try {
                    d= Double.parseDouble(s2[i]);
                }catch (NumberFormatException e){
                    d=0.0;
                }
                ppmt.setDouble(i+2, d);
            }
            ppmt.addBatch();
            ppmt.executeBatch();
        }

        ppmt.close();

    }
    @Test
    public void appO2o(String time) throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("appKey", "query_114bst_o2o"); // 查询统计订单
        map.put("method", "common.query114bstO2O");
        map.put("v", "1.0");
        map.put("messageFormat", "json");
        map.put("lb", "YD");
        map.put("startDate", time);
        map.put("endDate",time);
        map.put("prodCode", "query114bstO2O");// 查询统计订单
        String sign = RopUtils.sign(map, "117F5580853B04F1ABC1E1618852AE3A"); // 查询统计订单
        map.put("sign", sign);
        String response = HttpUtil.postServer(SERVER_URL, map, useProxy);
        System.out.println(response);
        Map<String, Object> reponseMapper = objectMapper.readValue(response, HashMap.class);
        Object status = reponseMapper.get("status");
        String data = reponseMapper.get("data").toString();
        String[] splits = data.trim().split("], \\[");
        //data:省份，交易额
        //插入数据库
        Connection cnn = JDBCUtill.getConn();
        String o2osql = "INSERT into appo2o (day_id,province,order_cnts,trade_cnts) values (?,?,?,?)";
        PreparedStatement ppmt = cnn.prepareStatement(o2osql);
        //解析字符串
        for (String s : splits) {
            String[] s2 = s.replace("]", "").replace("[", "").split(",");
            ppmt.setString(1, time);
            ppmt.setString(2, s2[0]);
            Double d1 = s2[1].contains("null") ? 0.0 : Double.parseDouble(s2[1]);
            Double d2 = s2[2].contains("null") ? 0.0 : Double.parseDouble(s2[2]);
            ppmt.setDouble(3, d1);
            ppmt.setDouble(4, d2);
            ppmt.addBatch();
            ppmt.executeBatch();
        }
        ppmt.close();


    }

    public void storderDay(String time, String type) throws Exception {
        Map<String, String> map = new HashMap<String, String>();
        map.put("appKey", "query_statistics_order"); // 查询统计订单
        map.put("method", "common.queryStatisticsOrder");
        map.put("v", "1.0");
        map.put("messageFormat", "json");
        map.put("type", type);
        map.put("lb", "YD");
        map.put("prodCode", "query_statistics");// 查询统计订单
        String sign = RopUtils.sign(map, "D9811912A975507257412BE137B3076E"); // 查询统计订单
        map.put("sign", sign);
        String response = HttpUtil.postServer(SERVER_URL, map, useProxy);
        System.out.println(response);
        Map<String, Object> reponseMapper = objectMapper.readValue(response, HashMap.class);
        Object status = reponseMapper.get("status");
        String data = reponseMapper.get("data").toString();
//        System.out.println(data);
        String[] splits = data.split("], \\[");
        //System.out.println(data.split("], \\[").length);

        //插入数据库
        PreparedStatement ppmt = null;
        ResultSet resultSet = null;
        Connection cnn = JDBCUtill.getConn();
        String o2osql = "INSERT into o2o_order_day (day_id,province,type,order_cnt,order_amt) values (?,?,?,?,?)";
        ppmt = cnn.prepareStatement(o2osql);
        //解析字符串
        for (String s : splits) {
            String[] s2 = s.replace("]", "").replace("[", "").split(",");
            ppmt.setString(1, time);
            String s1 = new String(s2[0].getBytes(),"utf-8");
            ppmt.setString(2, s2[0]);
            ppmt.setInt(3,Integer.parseInt(type));
            Double d1= s2[1].contains("null")?0.0:Double.parseDouble(s2[1]);
            Double d2= s2[2].contains("null")?0.0:Double.parseDouble(s2[2]);
            ppmt.setDouble(4,d1);
            ppmt.setDouble(5,d2);

            ppmt.addBatch();
            ppmt.executeBatch();
        }
        ppmt.close();




    }




}
